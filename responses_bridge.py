from __future__ import annotations

import json
import secrets
import time
from typing import Any, AsyncIterator, Optional

from .http_common import HttpProtocolError
from .pipeline import AFReadChunk, Handler, RequestInfo, ResponseInfo

# Мост Responses (frontend) -> Chat Completions (backend), как bridge у litellm
# (litellm/responses/litellm_completion_transformation/transformation.py):
# клиент шлёт POST */v1/responses, backend видит POST */v1/chat/completions
# с тем же префиксом, ответ конвертируется обратно. Внутри конвейера SSE-чанки
# ходят ЧИСТЫМИ (без data:, без [DONE]) — как и везде в проекте.

_RESPONSES_SUFFIX = "/v1/responses"
_CHAT_SUFFIX = "/v1/chat/completions"


def _chat_path(path: str) -> Optional[str]:
	"""'.../v1/responses[?query]' -> '.../v1/chat/completions[?query]', префикс сохраняем."""
	base, sep, query = path.partition("?")
	rest = base[:-1] if base.endswith("/") and len(base) > 1 else base
	if not rest.endswith(_RESPONSES_SUFFIX):
		return None
	new_base = rest[: -len(_RESPONSES_SUFFIX)] + _CHAT_SUFFIX
	return new_base + (sep + query if sep else "")


def _make_id(prefix: str) -> str:
	return prefix + secrets.token_hex(12)


def _error_body(message: str) -> bytes:
	return json.dumps(
		{"error": {"message": message, "type": "invalid_request_error", "code": "invalid_request_error"}}
	).encode()


def _text_of_output(output: Any) -> str:
	if isinstance(output, str):
		return output
	if isinstance(output, list):
		texts: list[str] = []
		for part in output:
			if isinstance(part, dict):
				t = part.get("text")
				if isinstance(t, str):
					texts.append(t)
			elif isinstance(part, str):
				texts.append(part)
		return "\n".join(texts)
	if output is None:
		return ""
	return str(output)


def _content_part_to_chat(part: Any) -> Optional[dict]:
	if not isinstance(part, dict):
		return None
	t = part.get("type")
	if t in ("input_text", "output_text", "text"):
		text = part.get("text", "")
		return {"type": "text", "text": text if isinstance(text, str) else str(text)}
	if t == "input_image":
		image_url = part.get("image_url")
		if isinstance(image_url, dict):
			url = image_url.get("url")
		else:
			url = image_url
		if not isinstance(url, str) or not url:
			return None
		return {"type": "image_url", "image_url": {"url": url}}
	if t == "refusal":
		refusal = part.get("refusal", "")
		return {"type": "text", "text": refusal if isinstance(refusal, str) else str(refusal)}
	return None


def _message_item_to_chat(item: dict) -> Optional[dict]:
	role = item.get("role", "user")
	if role == "developer":
		role = "system"
	if role not in ("system", "user", "assistant", "tool"):
		role = "user"
	content = item.get("content", "")
	if isinstance(content, str):
		return {"role": role, "content": content}
	if isinstance(content, list):
		parts = [p for p in (_content_part_to_chat(p) for p in content) if p is not None]
		if any(p.get("type") == "image_url" for p in parts):
			return {"role": role, "content": parts}
		return {"role": role, "content": "".join(p.get("text", "") for p in parts if p.get("type") == "text")}
	return {"role": role, "content": ""}


def _input_to_messages(payload_input: Any, instructions: Any) -> list[dict]:
	"""Responses input (+instructions) -> chat messages. function_call'ы рядом склеиваем в одно tool_calls."""
	messages: list[dict] = []
	if isinstance(instructions, str) and instructions:
		messages.append({"role": "system", "content": instructions})
	if payload_input is None:
		return messages
	items = [payload_input] if isinstance(payload_input, str) else payload_input
	if isinstance(items, str):
		messages.append({"role": "user", "content": items})
		return messages
	if not isinstance(items, list):
		raise ValueError("поле 'input' должно быть строкой или массивом")
	pending_calls: list[dict] = []
	def flush_calls() -> None:
		if pending_calls:
			messages.append({"role": "assistant", "content": None, "tool_calls": pending_calls.copy()})
			pending_calls.clear()
	for item in items:
		if isinstance(item, str):
			flush_calls()
			messages.append({"role": "user", "content": item})
			continue
		if not isinstance(item, dict):
			continue
		t = item.get("type", "message")
		if t == "message":
			flush_calls()
			msg = _message_item_to_chat(item)
			if msg is not None:
				messages.append(msg)
		elif t == "function_call":
			call_id = item.get("call_id") or item.get("id")
			name = item.get("name")
			if not call_id or not name:
				continue
			args = item.get("arguments", "{}")
			if not isinstance(args, str):
				args = json.dumps(args)
			pending_calls.append(
				{"id": str(call_id), "type": "function", "function": {"name": str(name), "arguments": args}}
			)
		elif t == "function_call_output":
			flush_calls()
			call_id = item.get("call_id") or item.get("id")
			if not call_id:
				continue
			messages.append(
				{"role": "tool", "tool_call_id": str(call_id), "content": _text_of_output(item.get("output"))}
			)
		elif t in ("reasoning", "web_search_call", "file_search_call", "computer_call",
			"mcp_call", "code_interpreter_call", "image_generation_call"):
			# Хостед-тулы и reasoning generic chat-backend'у не пересказать — дропаем.
			# (litellm в этом месте реплеит reasoning_content/thinking_blocks
			# под конкретный провайдер; здесь backend произвольный OpenAI-совместимый.)
			flush_calls()
			continue
		else:
			flush_calls()
			continue
	flush_calls()
	return messages


def _responses_tools_to_chat(tools: Any) -> list[dict]:
	if not isinstance(tools, list):
		return []
	out: list[dict] = []
	for t in tools:
		if not isinstance(t, dict):
			continue
		tt = t.get("type", "function")
		if tt == "function":
			name = t.get("name")
			if not name:
				continue
			fn: dict = {"name": str(name)}
			if t.get("description") is not None:
				fn["description"] = t["description"]
			if t.get("parameters") is not None:
				fn["parameters"] = t["parameters"]
			if t.get("strict") is not None:
				fn["strict"] = t["strict"]
			out.append({"type": "function", "function": fn})
		elif tt == "custom":
			# custom-тула едет как function (имена/аргументы те же, см. litellm custom_tools)
			name = t.get("name") or (t.get("custom", {}).get("name") if isinstance(t.get("custom"), dict) else None)
			if not name:
				continue
			fn = {"name": str(name)}
			custom = t.get("custom") if isinstance(t.get("custom"), dict) else None
			if isinstance(custom, dict):
				if custom.get("description") is not None:
					fn["description"] = custom["description"]
				if custom.get("parameters") is not None:
					fn["parameters"] = custom["parameters"]
			if t.get("description") is not None and "description" not in fn:
				fn["description"] = t["description"]
			out.append({"type": "function", "function": fn})
		# builtin-тулы (web_search, file_search, ...) chat-backend'у не пересказать — дропаем
	return out


def _transform_tool_choice(tool_choice: Any) -> Any:
	if tool_choice is None or isinstance(tool_choice, str):
		return tool_choice
	if isinstance(tool_choice, dict):
		fn = tool_choice.get("function")
		if isinstance(fn, dict) and fn.get("name"):
			return {"type": "function", "function": {"name": fn["name"]}}
		t = tool_choice.get("type")
		if t in ("auto", "none", "required"):
			return t
		if t in ("tool", "any"):
			return "required"
		if t == "function":
			name = tool_choice.get("name")
			if name:
				return {"type": "function", "function": {"name": name}}
			return "required"
		if t == "custom":
			custom = tool_choice.get("custom")
			name = tool_choice.get("name") or (custom.get("name") if isinstance(custom, dict) else None)
			if name:
				return {"type": "function", "function": {"name": name}}
			return "required"
	return tool_choice


def _transform_text_format(text: Any) -> Optional[dict]:
	if not isinstance(text, dict):
		return None
	fmt = text.get("format")
	if not isinstance(fmt, dict):
		return None
	ftype = fmt.get("type", "text")
	if ftype == "json_schema":
		return {
			"type": "json_schema",
			"json_schema": {
				"name": fmt.get("name", "response"),
				"schema": fmt.get("schema", {}),
				"strict": fmt.get("strict", False),
			},
		}
	if ftype == "json_object":
		return {"type": "json_object"}
	return None


def _chat_usage_to_responses(usage: Any) -> Optional[dict]:
	if not isinstance(usage, dict):
		return None
	in_tok = usage.get("prompt_tokens", usage.get("input_tokens", 0))
	out_tok = usage.get("completion_tokens", usage.get("output_tokens", 0))
	total = usage.get("total_tokens", (in_tok or 0) + (out_tok or 0))
	return {"input_tokens": in_tok or 0, "output_tokens": out_tok or 0, "total_tokens": total or 0}


def _build_output(message: dict, finish_reason: Any, msg_id: str) -> tuple[list[dict], str, Optional[dict]]:
	"""chat message + finish_reason -> (responses output, status, incomplete_details)."""
	output: list[dict] = []
	content = message.get("content")
	if isinstance(content, str) and content:
		output.append(
			{
				"type": "message",
				"id": msg_id,
				"status": "completed",
				"role": "assistant",
				"content": [{"type": "output_text", "text": content, "annotations": []}],
			}
		)
	for tc in message.get("tool_calls") or []:
		fn = (tc or {}).get("function", {}) if isinstance(tc, dict) else {}
		output.append(
			{
				"type": "function_call",
				"id": _make_id("fc_"),
				"call_id": tc.get("id", "") if isinstance(tc, dict) else "",
				"name": fn.get("name", "") if isinstance(fn, dict) else "",
				"arguments": fn.get("arguments", "{}") if isinstance(fn, dict) else "{}",
				"status": "completed",
			}
		)
	if not output:
		output.append(
			{
				"type": "message",
				"id": msg_id,
				"status": "completed",
				"role": "assistant",
				"content": [{"type": "output_text", "text": "", "annotations": []}],
			}
		)
	if finish_reason == "length":
		return output, "incomplete", {"reason": "max_output_tokens"}
	if finish_reason == "content_filter":
		return output, "incomplete", {"reason": "content_filter"}
	return output, "completed", None


class ResponsesBridge(Handler):
	"""
	Мост Responses -> Chat Completions (как responses->chat bridge у litellm).

	Фронтенд (клиенты типа Codex CLI с wire_api="responses") шлёт
	POST */v1/responses; backend'у уезжает POST */v1/chat/completions
	с тем же префиксом пути. Остальные запросы — транзитом без изменений.

	Конвертация запроса: instructions -> system, input items -> messages
	(message/function_call/function_call_output), function/custom tools -> tools,
	text.format -> response_format, max_output_tokens -> max_tokens,
	tool_choice нормализуется. reasoning/hosted-тулы дропаются: произвольный
	OpenAI-совместимый backend их не перескажет.

	previous_response_id обслуживается сессионным кэшем в памяти: история
	сообщений склеивается перед новыми. store=false — не запоминать.
	"""

	def __init__(self, backend: Handler, *, max_sessions: int = 1000) -> None:
		self._next = backend
		self._max_sessions = max_sessions
		self._sessions: dict[str, list[dict]] = {}

	def _store_session(self, resp_id: str, messages: list[dict]) -> None:
		while len(self._sessions) >= self._max_sessions:
			self._sessions.pop(next(iter(self._sessions)))
		self._sessions[resp_id] = messages

	def _bad_request(self, message: str) -> tuple[ResponseInfo, AFReadChunk]:
		body = _error_body(message)
		async def afread_error() -> AsyncIterator[bytes]:
			yield body
		return ResponseInfo(status=400, reason="Bad Request", headers=[("Content-Type", "application/json")]), afread_error

	async def ahandle(self, request: RequestInfo, body: bytes) -> tuple[ResponseInfo, AFReadChunk]:
		chat_path = _chat_path(request.path) if request.method == "POST" else None
		if chat_path is None:
			return await self._next.ahandle(request, body)
		try:
			payload = json.loads(body.decode("utf-8"))
		except Exception:
			return self._bad_request("тело запроса — не JSON")
		if not isinstance(payload, dict):
			return self._bad_request("тело запроса — не JSON-объект")
		model = payload.get("model")
		if not isinstance(model, str) or not model:
			return self._bad_request("поле 'model' обязательно")
		stream = payload.get("stream", False) is True

		history: Optional[list[dict]] = None
		previous_id = payload.get("previous_response_id")
		if previous_id is not None:
			if not isinstance(previous_id, str) or previous_id not in self._sessions:
				return self._bad_request(f"неизвестный previous_response_id: {previous_id!r}")
			history = self._sessions[previous_id]

		try:
			new_messages = _input_to_messages(payload.get("input"), payload.get("instructions"))
		except ValueError as exc:
			return self._bad_request(str(exc))
		if not new_messages:
			return self._bad_request("пустой 'input': нечего отправлять в backend")
		combined = (list(history) + new_messages) if history else new_messages

		tools = _responses_tools_to_chat(payload.get("tools"))
		tool_choice = _transform_tool_choice(payload.get("tool_choice")) if tools else None
		response_format = _transform_text_format(payload.get("text"))
		chat_req: dict = {
			"model": model,
			"messages": combined,
			"stream": stream,
		}
		if tools:
			chat_req["tools"] = tools
			if tool_choice is not None:
				chat_req["tool_choice"] = tool_choice
			if payload.get("parallel_tool_calls") is not None:
				chat_req["parallel_tool_calls"] = payload["parallel_tool_calls"]
		if payload.get("max_output_tokens") is not None:
			chat_req["max_tokens"] = payload["max_output_tokens"]
		for key in ("temperature", "top_p", "user"):
			if payload.get(key) is not None:
				chat_req[key] = payload[key]
		if response_format is not None:
			chat_req["response_format"] = response_format
		if stream:
			chat_req["stream_options"] = {"include_usage": True}

		fwd = RequestInfo(
			method="POST",
			path=chat_path,
			http_version=request.http_version,
			headers=list(request.headers),
			client_addr=request.client_addr,
		)
		print(f"ResponsesBridge: {request.method} {request.path} -> {chat_path} (stream={stream})")
		response, afread_backend = await self._next.ahandle(fwd, json.dumps(chat_req).encode())
		if response.status != 200 or not stream:
			if response.status != 200:
				return response, afread_backend
			return await self._acollect_chat_json(model, combined, payload, afread_backend)
		return self._wrap_stream_to_sse(model, combined, payload, response, afread_backend)

	async def _acollect_chat_json(
		self, model: str, combined: list[dict], payload: dict, afread_backend: AFReadChunk
	) -> tuple[ResponseInfo, AFReadChunk]:
		"""Non-stream: забираем chat JSON целиком, отдаём responses JSON одним чанком."""
		gen = afread_backend()
		try:
			parts: list[bytes] = []
			async for chunk in gen:
				parts.append(chunk)
			raw = b"".join(parts)
		finally:
			await gen.aclose()
		try:
			chat = json.loads(raw.decode("utf-8"))
		except Exception as exc:
			raise HttpProtocolError(f"backend прислал не JSON: {exc}") from exc
		if not isinstance(chat, dict):
			raise HttpProtocolError(f"backend прислал не JSON-объект: {raw[:200]!r}")
		choices = chat.get("choices") or []
		message = (choices[0].get("message") if choices else None) or {}
		if not isinstance(message, dict):
			raise HttpProtocolError("у backend'а нет choices[0].message")
		finish = choices[0].get("finish_reason") if choices else None
		resp_id = _make_id("resp_")
		created = int(time.time())
		output, status, incomplete = _build_output(message, finish, _make_id("msg_"))
		usage = _chat_usage_to_responses(chat.get("usage"))
		resp_model = chat.get("model") or model
		resp: dict = {
			"id": resp_id,
			"object": "response",
			"created_at": created,
			"model": resp_model,
			"status": status,
			"output": output,
		}
		if incomplete is not None:
			resp["incomplete_details"] = incomplete
		if usage is not None:
			resp["usage"] = usage
		if payload.get("store", True) is not False:
			assistant_msg: dict = {"role": "assistant", "content": message.get("content")}
			if message.get("tool_calls"):
				assistant_msg["tool_calls"] = message["tool_calls"]
			self._store_session(resp_id, combined + [assistant_msg])
		body = json.dumps(resp).encode()
		async def afread_json() -> AsyncIterator[bytes]:
			yield body
		return ResponseInfo(status=200, reason="OK", headers=[("Content-Type", "application/json")]), afread_json

	def _wrap_stream_to_sse(
		self, model: str, combined: list[dict], payload: dict, backend_head: ResponseInfo, afread_backend: AFReadChunk
	) -> tuple[ResponseInfo, AFReadChunk]:
		"""Stream: chat-chunks (чистые) -> responses-события (чистые, обвязку навесит HttpServer)."""
		bridge = self
		resp_id = _make_id("resp_")
		created = int(time.time())
		msg_id = _make_id("msg_")

		def ev(event: dict) -> bytes:
			return json.dumps(event).encode()

		async def afread_response() -> AsyncIterator[bytes]:
			full_text = ""
			tools_acc: dict[int, dict] = {}
			fc_ids: dict[int, str] = {}
			finish_reason: Any = None
			usage: Optional[dict] = None
			resp_model = model
			inner = afread_backend()
			try:
				yield ev({"type": "response.created",
					"response": {"id": resp_id, "object": "response", "created_at": created,
						"model": model, "status": "in_progress", "output": []}})
				yield ev({"type": "response.in_progress",
					"response": {"id": resp_id, "object": "response", "status": "in_progress", "output": []}})
				yield ev({"type": "response.output_item.added", "output_index": 0,
					"item": {"type": "message", "id": msg_id, "status": "in_progress",
						"role": "assistant", "content": []}})
				yield ev({"type": "response.content_part.added", "item_id": msg_id,
					"output_index": 0, "content_index": 0,
					"part": {"type": "output_text", "text": "", "annotations": []}})
				async for chunk in inner:
					try:
						evt = json.loads(chunk.decode("utf-8"))
					except Exception as exc:
						raise HttpProtocolError(f"backend прислал не JSON-чанк: {exc}") from exc
					if not isinstance(evt, dict):
						continue
					if evt.get("model"):
						resp_model = evt["model"]
					if evt.get("usage") and usage is None:
						usage = _chat_usage_to_responses(evt["usage"])
					choices = evt.get("choices") or []
					if not choices:
						continue
					first = choices[0] or {}
					delta = first.get("delta") or {}
					if first.get("finish_reason") is not None:
						finish_reason = first["finish_reason"]
					content = delta.get("content")
					if isinstance(content, str) and content:
						full_text += content
						yield ev({"type": "response.output_text.delta", "item_id": msg_id,
							"output_index": 0, "content_index": 0, "delta": content})
					for tc in delta.get("tool_calls") or []:
						if not isinstance(tc, dict):
							continue
						idx = tc.get("index", 0)
						acc = tools_acc.setdefault(idx, {"id": "", "name": "", "arguments": ""})
						if tc.get("id") and not acc["id"]:
							acc["id"] = str(tc["id"])
						fn = tc.get("function") or {}
						if isinstance(fn, dict):
							if fn.get("name") and not acc["name"]:
								acc["name"] = str(fn["name"])
							elif fn.get("name") and fn["name"] not in acc["name"]:
								acc["name"] += str(fn["name"])
							if isinstance(fn.get("arguments"), str):
								frag = fn["arguments"]
								acc["arguments"] += frag
								if idx not in fc_ids:
									fc_ids[idx] = _make_id("fc_")
									out_index = len(fc_ids)
									yield ev({"type": "response.output_item.added",
										"output_index": out_index,
										"item": {"type": "function_call", "id": fc_ids[idx],
											"call_id": acc["id"], "name": acc["name"],
											"arguments": "", "status": "in_progress"}})
								if frag:
									yield ev({"type": "response.function_call_arguments.delta",
										"item_id": fc_ids[idx], "output_index": len(fc_ids),
										"delta": frag})
						if idx not in fc_ids and (acc["id"] or acc["name"]):
							fc_ids[idx] = _make_id("fc_")
							yield ev({"type": "response.output_item.added",
								"output_index": len(fc_ids),
								"item": {"type": "function_call", "id": fc_ids[idx],
									"call_id": acc["id"], "name": acc["name"],
									"arguments": "", "status": "in_progress"}})
			except GeneratorExit:
				raise
			except Exception:
				raise
			else:
				tool_items: list[dict] = []
				for idx in sorted(tools_acc):
					acc = tools_acc[idx]
					if not acc["name"] and not acc["arguments"]:
						continue
					item = {"type": "function_call", "id": fc_ids.get(idx, _make_id("fc_")),
						"call_id": acc["id"], "name": acc["name"],
						"arguments": acc["arguments"], "status": "completed"}
					tool_items.append(item)
					yield ev({"type": "response.function_call_arguments.done",
						"item_id": item["id"], "output_index": 1 + len(tool_items) - 1,
						"arguments": acc["arguments"]})
					yield ev({"type": "response.output_item.done",
						"output_index": 1 + len(tool_items) - 1, "item": item})
				yield ev({"type": "response.output_text.done", "item_id": msg_id,
					"output_index": 0, "content_index": 0, "text": full_text})
				yield ev({"type": "response.content_part.done", "item_id": msg_id,
					"output_index": 0, "content_index": 0,
					"part": {"type": "output_text", "text": full_text, "annotations": []}})
				yield ev({"type": "response.output_item.done", "output_index": 0,
					"item": {"type": "message", "id": msg_id, "status": "completed",
						"role": "assistant",
						"content": [{"type": "output_text", "text": full_text, "annotations": []}]}})
				output = [{"type": "message", "id": msg_id, "status": "completed",
					"role": "assistant",
					"content": [{"type": "output_text", "text": full_text, "annotations": []}]}]
				output.extend(tool_items)
				if finish_reason == "length":
					status, incomplete = "incomplete", {"reason": "max_output_tokens"}
				elif finish_reason == "content_filter":
					status, incomplete = "incomplete", {"reason": "content_filter"}
				else:
					status, incomplete = "completed", None
				full: dict = {"id": resp_id, "object": "response", "created_at": created,
					"model": resp_model, "status": status, "output": output}
				if incomplete is not None:
					full["incomplete_details"] = incomplete
				if usage is not None:
					full["usage"] = usage
				yield ev({"type": "response.completed", "response": full})
				if payload.get("store", True) is not False:
					assistant_msg = {"role": "assistant", "content": full_text or None}
					if tools_acc:
						assistant_msg["tool_calls"] = [
							{"id": tools_acc[i]["id"], "type": "function",
								"function": {"name": tools_acc[i]["name"], "arguments": tools_acc[i]["arguments"]}}
							for i in sorted(tools_acc) if tools_acc[i]["name"]
						]
					bridge._store_session(resp_id, combined + [assistant_msg])
			finally:
				aclose = getattr(inner, "aclose", None)
				if aclose is not None:
					await aclose()

		head = [(n, v) for n, v in backend_head.headers if n.lower() not in ("content-length", "transfer-encoding")]
		head = [(n, v) for n, v in head if n.lower() != "content-type"]
		head.append(("Content-Type", "text/event-stream"))
		return ResponseInfo(status=200, reason="OK", headers=head), afread_response
