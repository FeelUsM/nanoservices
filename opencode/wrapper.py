import json
import secrets
import time
from typing import AsyncIterator
from ..http_common import is_event_stream
from ..pipeline import Handler, RequestInfo, ResponseInfo

def make_id(prefix):
	ts = int(time.time() * 1000)
	v = ((ts << 12) | 1) ^ 0xFFFFFFFFFFFFFF  # opencode stores ~(ts<<12|ctr) for desc sort
	v &= 0xFFFFFFFFFFFF
	hexp = "".join(f"{(v >> (40 - 8 * i)) & 0xFF:02x}" for i in range(6))
	b62 = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	return prefix + hexp + "".join(secrets.choice(b62) for _ in range(14))

class OpencodeWrapper(Handler):
	"""
	делает запрос таким как будто его отправил opencode
	"""
	
	def __init__(
		self,
		backend,
		newh = {
			"User-Agent": "opencode/1.18.31 ai-sdk/provider-utils/4.0.23 runtime/bun/1.3.14",
			"x-opencode-client": "cli", 
			"x-opencode-project": "global",
			"x-opencode-request": lambda : make_id("msg_"), 
			"x-opencode-session": lambda : make_id("ses_"),
		},
		delh = {},
		tools = ["bash", "glob", "grep", "read"],
		body = {
			"tool_choice": "auto",
			"stream": True, 
			"stream_options": {"include_usage": True},
		}
	):
		self.backend = backend
		self.newh = newh
		self.delh = delh
		self.tools = tools
		self.body = body

	async def ahandle(self, request: RequestInfo, body: bytes):
		# Заголовки: чужие форвардим как есть, свои (newh) — подменяем/дописываем.
		# Сравнение регистронезависимо, иначе Authorization/Content-Type терялись.
		newh_lower = {str(k).lower(): (k, v) for k, v in self.newh.items()}
		if isinstance(self.delh, dict):
			del_names = self.delh.keys()
		else:
			del_names = self.delh
		del_lower = {str(k).lower() for k in del_names}
		new_headers = []
		done = set()
		for k, v in request.headers:
			lk = str(k).lower()
			if lk in del_lower:
				continue
			if lk in newh_lower:
				canon, nv = newh_lower[lk]
				done.add(lk)
				if isinstance(nv, str):
					new_headers.append((canon, nv))
				else:
					new_headers.append((canon, nv()))
			else:
				new_headers.append((k, v))
		for lk, (canon, nv) in newh_lower.items():
			if lk in done:
				continue
			if isinstance(nv, str):
				new_headers.append((canon, nv))
			else:
				new_headers.append((canon, nv()))
		request.headers = new_headers

		try:
			parsed = json.loads(body.decode())
		except Exception:
			# не-JSON (GET, пустое тело и т.п.) — форвардим как есть
			return await self.backend.ahandle(request, body)
		if not isinstance(parsed, dict):
			return await self.backend.ahandle(request, body)
		body = parsed
		# Клиентский stream: только явный true — стрим навылет.
		# Всё остальное (false/отсутствует) — апстрим всё равно просим
		# стримом, а клиенту собираем один нестриминговый JSON (openai-completions).
		client_wants_stream = body.get("stream") is True
		for k, v in self.body.items():
			if k not in body:
				body[k] = v
		# апстрим всегда стримим (free tier), даже если клиент попросил без стрима
		body["stream"] = True
		if not isinstance(body.get("stream_options"), dict):
			body["stream_options"] = {"include_usage": True}

		tools = set()
		if not isinstance(body.get("tools"), list):
			body["tools"] = []
		for tool in body["tools"]:
			if isinstance(tool, dict) and tool.get("type") == "function":
				fn = tool.get("function", {})
				if isinstance(fn, dict):
					tools.add(fn.get("name"))
		for tool in self.tools:
			if tool not in tools:
				body["tools"].append({"type": "function", "function": {"name": tool, "description": "Don't use this tool."}})

		model = body.get("model") if isinstance(body.get("model"), str) else "big-pickle"
		response, afread_response = await self.backend.ahandle(request, json.dumps(body).encode())
		if client_wants_stream or not is_event_stream(response.headers):
			return response, afread_response
		# Клиент просил без стрима: вычитываем SSE-поток целиком и отдаём одним JSON-чанком.
		inner = afread_response()
		chunks: list[bytes] = []
		try:
			async for chunk in inner:
				chunks.append(chunk)
		finally:
			aclose = getattr(inner, "aclose", None)
			if aclose is not None:
				await aclose()
		payload = _aggregate_openai_chunks(chunks, fallback_model=model)
		headers = [
			(k, v) for k, v in response.headers
			if str(k).lower() not in ("content-type", "content-length", "transfer-encoding", "connection")
		]
		headers.append(("Content-Type", "application/json"))
		headers.append(("Content-Length", str(len(payload))))
		aggregated = ResponseInfo(status=response.status, reason=response.reason, headers=headers)

		async def afsingle() -> AsyncIterator[bytes]:
			yield payload

		return aggregated, afsingle


def _append_text(parts: list[str], value: object) -> None:
	# content в delta — обычно str/None, но терпим и к спискам контент-блоков
	if isinstance(value, str):
		parts.append(value)
	elif isinstance(value, list):
		for item in value:
			if isinstance(item, str):
				parts.append(item)
			elif isinstance(item, dict):
				text = item.get("text")
				if isinstance(text, str):
					parts.append(text)


def _aggregate_openai_chunks(chunks: list[bytes], *, fallback_model: str) -> bytes:
	"""Собирает chat.completion.chunk'и в один chat.completion (openai-completions)."""
	resp_id: object = None
	created: object = None
	model: object = None
	usage: object = None
	roles: dict[int, str] = {}
	contents: dict[int, list[str]] = {}
	reasonings: dict[int, list[str]] = {}
	tool_calls: dict[int, dict[int, dict]] = {}
	finishes: dict[int, object] = {}
	for raw in chunks:
		try:
			parsed = json.loads(raw.decode() if isinstance(raw, bytes) else raw)
		except Exception:
			continue
		if not isinstance(parsed, dict):
			continue
		if resp_id is None and parsed.get("id") is not None:
			resp_id = parsed.get("id")
		if created is None and parsed.get("created") is not None:
			created = parsed.get("created")
		if parsed.get("model") is not None:
			model = parsed.get("model")
		if parsed.get("usage") is not None:
			usage = parsed.get("usage")
		choices = parsed.get("choices")
		if not isinstance(choices, list):
			continue
		for ch in choices:
			if not isinstance(ch, dict):
				continue
			idx = ch.get("index", 0)
			if not isinstance(idx, int):
				try:
					idx = int(idx)
				except Exception:
					idx = 0
			delta = ch.get("delta")
			if not isinstance(delta, dict):
				msg = ch.get("message")
				delta = msg if isinstance(msg, dict) else {}
			role = delta.get("role")
			if isinstance(role, str) and idx not in roles:
				roles[idx] = role
			contents.setdefault(idx, [])
			_append_text(contents[idx], delta.get("content"))
			reasonings.setdefault(idx, [])
			_append_text(reasonings[idx], delta.get("reasoning_content"))
			tcs = delta.get("tool_calls")
			if isinstance(tcs, list):
				slot = tool_calls.setdefault(idx, {})
				for tc in tcs:
					if not isinstance(tc, dict):
						continue
					tc_idx = tc.get("index", 0)
					if not isinstance(tc_idx, int):
						try:
							tc_idx = int(tc_idx)
						except Exception:
							tc_idx = 0
					entry = slot.setdefault(tc_idx, {"id": None, "type": "function", "name": None, "args": []})
					if entry["id"] is None and isinstance(tc.get("id"), str):
						entry["id"] = tc.get("id")
					if isinstance(tc.get("type"), str):
						entry["type"] = tc.get("type")
					fn = tc.get("function")
					if isinstance(fn, dict):
						if entry["name"] is None and isinstance(fn.get("name"), str):
							entry["name"] = fn.get("name")
						_append_text(entry["args"], fn.get("arguments"))
			finish = ch.get("finish_reason")
			if finish is not None:
				finishes[idx] = finish
	if resp_id is None:
		resp_id = "chatcmpl-0"
	if created is None:
		created = int(time.time())
	if not isinstance(model, str):
		model = fallback_model
	out_choices = []
	for idx in sorted(set(list(contents.keys()) + list(finishes.keys()) + list(roles.keys())) or [0]):
		message: dict = {"role": roles.get(idx, "assistant"), "content": "".join(contents.get(idx, []))}
		reasoning = "".join(reasonings.get(idx, []))
		if reasoning:
			message["reasoning_content"] = reasoning
		slot = tool_calls.get(idx, {})
		if slot:
			calls = []
			for tc_idx in sorted(slot.keys()):
				entry = slot[tc_idx]
				calls.append({
					"id": entry["id"] or f"call_{tc_idx}",
					"type": entry["type"] or "function",
					"function": {
						"name": entry["name"] or "",
						"arguments": "".join(entry["args"]),
					},
				})
			message["tool_calls"] = calls
		out_choices.append({
			"index": idx,
			"message": message,
			"finish_reason": finishes.get(idx, "stop"),
		})
	result: dict = {
		"id": resp_id,
		"object": "chat.completion",
		"created": created,
		"model": model,
		"choices": out_choices,
	}
	if usage is not None:
		result["usage"] = usage
	return json.dumps(result, ensure_ascii=False).encode()