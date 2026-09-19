import asyncio
import json
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from nanoservices.pipeline import Handler, RequestInfo, ResponseInfo
from nanoservices.responses_bridge import ResponsesBridge


CHAT_OK = {
	"id": "chatcmpl-1",
	"object": "chat.completion",
	"created": 1700000000,
	"model": "test-model",
	"choices": [
		{"index": 0, "message": {"role": "assistant", "content": "hi there"}, "finish_reason": "stop"}
	],
	"usage": {"prompt_tokens": 5, "completion_tokens": 3, "total_tokens": 8},
}

CHAT_TOOLS = {
	"id": "chatcmpl-2",
	"object": "chat.completion",
	"created": 1700000000,
	"model": "test-model",
	"choices": [
		{
			"index": 0,
			"message": {
				"role": "assistant",
				"content": None,
				"tool_calls": [
					{"id": "call_1", "type": "function",
						"function": {"name": "get_weather", "arguments": '{"city":"Paris"}'}}
				],
			},
			"finish_reason": "tool_calls",
		}
	],
	"usage": {"prompt_tokens": 10, "completion_tokens": 6, "total_tokens": 16},
}


class StubBackend(Handler):
	def __init__(self, head=None, chunks=None):
		self.head = head or ResponseInfo(status=200, reason="OK",
			headers=[("Content-Type", "application/json")])
		self.chunks = chunks if chunks is not None else [json.dumps(CHAT_OK).encode()]
		self.last_request = None
		self.last_body = None

	async def ahandle(self, request, body):
		self.last_request = request
		self.last_body = body
		chunks = self.chunks
		async def afread():
			for c in chunks:
				yield c
		return self.head, afread


async def acollected(factory):
	gen = factory()
	try:
		return b"".join([chunk async for chunk in gen])
	finally:
		await gen.aclose()


def arequest(path, method="POST"):
	return RequestInfo(method=method, path=path, http_version="HTTP/1.1",
		headers=[("Content-Type", "application/json")], client_addr=("127.0.0.1", 1))


class BridgeCase(unittest.IsolatedAsyncioTestCase):
	async def test_passthrough_other_paths(self):
		stub = StubBackend()
		bridge = ResponsesBridge(stub)
		resp, factory = await bridge.ahandle(arequest("/v1/models", method="GET"), b"")
		self.assertEqual(resp.status, 200)
		self.assertEqual(stub.last_request.path, "/v1/models")
		body = await acollected(factory)
		self.assertEqual(json.loads(body)["id"], "chatcmpl-1")

	async def test_path_prefix_preserved(self):
		stub = StubBackend()
		bridge = ResponsesBridge(stub)
		payload = {"model": "m", "input": "hello"}
		await bridge.ahandle(arequest("/prefix/v1/responses"), json.dumps(payload).encode())
		self.assertEqual(stub.last_request.path, "/prefix/v1/chat/completions")

	async def test_query_preserved(self):
		stub = StubBackend()
		bridge = ResponsesBridge(stub)
		payload = {"model": "m", "input": "hello"}
		await bridge.ahandle(arequest("/v1/responses?x=1"), json.dumps(payload).encode())
		self.assertEqual(stub.last_request.path, "/v1/chat/completions?x=1")

	async def test_request_mapping(self):
		stub = StubBackend()
		bridge = ResponsesBridge(stub)
		payload = {
			"model": "m",
			"input": "hello",
			"instructions": "be nice",
			"max_output_tokens": 50,
			"temperature": 0.5,
			"tools": [{"type": "function", "name": "get_weather",
				"description": "d", "parameters": {"type": "object", "properties": {}}}],
			"tool_choice": "auto",
			"text": {"format": {"type": "json_schema", "name": "r",
				"schema": {"type": "object"}, "strict": True}},
		}
		await bridge.ahandle(arequest("/v1/responses"), json.dumps(payload).encode())
		chat = json.loads(stub.last_body.decode())
		self.assertEqual(chat["model"], "m")
		self.assertEqual(chat["messages"][0], {"role": "system", "content": "be nice"})
		self.assertEqual(chat["messages"][1], {"role": "user", "content": "hello"})
		self.assertEqual(chat["max_tokens"], 50)
		self.assertEqual(chat["temperature"], 0.5)
		self.assertEqual(chat["tools"][0]["function"]["name"], "get_weather")
		self.assertEqual(chat["tool_choice"], "auto")
		self.assertEqual(chat["response_format"]["type"], "json_schema")
		self.assertFalse(chat["stream"])

	async def test_input_items_mapping(self):
		stub = StubBackend()
		bridge = ResponsesBridge(stub)
		payload = {
			"model": "m",
			"input": [
				{"type": "message", "role": "user",
					"content": [{"type": "input_text", "text": "hi"}]},
				{"type": "function_call", "call_id": "call_1",
					"name": "get_weather", "arguments": "{}"},
				{"type": "function_call", "call_id": "call_2",
					"name": "get_time", "arguments": "{}"},
				{"type": "function_call_output", "call_id": "call_1", "output": "sunny"},
			],
		}
		await bridge.ahandle(arequest("/v1/responses"), json.dumps(payload).encode())
		msgs = json.loads(stub.last_body.decode())["messages"]
		self.assertEqual(msgs[0], {"role": "user", "content": "hi"})
		# соседние function_call склеены в один assistant
		self.assertEqual(msgs[1]["role"], "assistant")
		self.assertEqual(len(msgs[1]["tool_calls"]), 2)
		self.assertEqual(msgs[2], {"role": "tool", "tool_call_id": "call_1", "content": "sunny"})

	async def test_nonstream_text_response(self):
		stub = StubBackend()
		bridge = ResponsesBridge(stub)
		resp, factory = await bridge.ahandle(arequest("/v1/responses"),
			json.dumps({"model": "m", "input": "hi"}).encode())
		self.assertEqual(resp.status, 200)
		out = json.loads((await acollected(factory)).decode())
		self.assertTrue(out["id"].startswith("resp_"))
		self.assertEqual(out["object"], "response")
		self.assertEqual(out["status"], "completed")
		self.assertEqual(out["output"][0]["type"], "message")
		self.assertEqual(out["output"][0]["content"][0]["text"], "hi there")
		self.assertEqual(out["usage"], {"input_tokens": 5, "output_tokens": 3, "total_tokens": 8})
		# сессия запомнилась
		self.assertIn(out["id"], bridge._sessions)

	async def test_nonstream_tool_response(self):
		stub = StubBackend(chunks=[json.dumps(CHAT_TOOLS).encode()])
		bridge = ResponsesBridge(stub)
		resp, factory = await bridge.ahandle(arequest("/v1/responses"),
			json.dumps({"model": "m", "input": "weather?"}).encode())
		out = json.loads((await acollected(factory)).decode())
		fc = [i for i in out["output"] if i["type"] == "function_call"][0]
		self.assertEqual(fc["call_id"], "call_1")
		self.assertEqual(fc["name"], "get_weather")
		self.assertEqual(json.loads(fc["arguments"]), {"city": "Paris"})

	async def test_nonstream_incomplete_on_length(self):
		chat = dict(CHAT_OK)
		chat["choices"] = [{"index": 0, "message": {"role": "assistant", "content": "x"},
			"finish_reason": "length"}]
		stub = StubBackend(chunks=[json.dumps(chat).encode()])
		bridge = ResponsesBridge(stub)
		_, factory = await bridge.ahandle(arequest("/v1/responses"),
			json.dumps({"model": "m", "input": "hi"}).encode())
		out = json.loads((await acollected(factory)).decode())
		self.assertEqual(out["status"], "incomplete")
		self.assertEqual(out["incomplete_details"], {"reason": "max_output_tokens"})

	async def test_backend_error_passthrough(self):
		stub = StubBackend(
			head=ResponseInfo(status=500, reason="Err", headers=[("Content-Type", "application/json")]),
			chunks=[b'{"error":"boom"}'])
		bridge = ResponsesBridge(stub)
		resp, factory = await bridge.ahandle(arequest("/v1/responses"),
			json.dumps({"model": "m", "input": "hi"}).encode())
		self.assertEqual(resp.status, 500)
		self.assertEqual(await acollected(factory), b'{"error":"boom"}')

	async def test_bad_requests(self):
		stub = StubBackend()
		bridge = ResponsesBridge(stub)
		for body in (b"not json", json.dumps({"input": "hi"}).encode(),
			json.dumps({"model": "m"}).encode(),
			json.dumps({"model": "m", "previous_response_id": "resp_nope", "input": "hi"}).encode()):
			resp, factory = await bridge.ahandle(arequest("/v1/responses"), body)
			self.assertEqual(resp.status, 400, body)
			await acollected(factory)
		self.assertIsNone(stub.last_body)

	async def test_previous_response_id_continues_history(self):
		stub = StubBackend()
		bridge = ResponsesBridge(stub)
		_, f1 = await bridge.ahandle(arequest("/v1/responses"),
			json.dumps({"model": "m", "input": "first"}).encode())
		first_id = json.loads((await acollected(f1)).decode())["id"]
		n_before = len(json.loads(stub.last_body.decode())["messages"])
		_, f2 = await bridge.ahandle(arequest("/v1/responses"),
			json.dumps({"model": "m", "input": "second", "previous_response_id": first_id}).encode())
		await acollected(f2)
		msgs = json.loads(stub.last_body.decode())["messages"]
		self.assertGreater(len(msgs), n_before)
		self.assertEqual(msgs[0], {"role": "user", "content": "first"})
		self.assertIn({"role": "user", "content": "second"}, msgs)

	async def test_store_false_not_remembered(self):
		stub = StubBackend()
		bridge = ResponsesBridge(stub)
		_, factory = await bridge.ahandle(arequest("/v1/responses"),
			json.dumps({"model": "m", "input": "hi", "store": False}).encode())
		out = json.loads((await acollected(factory)).decode())
		self.assertNotIn(out["id"], bridge._sessions)

	async def test_tool_choice_dict_mapping(self):
		stub = StubBackend()
		bridge = ResponsesBridge(stub)
		payload = {"model": "m", "input": "hi",
			"tools": [{"type": "function", "name": "a"}],
			"tool_choice": {"type": "function", "name": "a"}}
		await bridge.ahandle(arequest("/v1/responses"), json.dumps(payload).encode())
		chat = json.loads(stub.last_body.decode())
		self.assertEqual(chat["tool_choice"], {"type": "function", "function": {"name": "a"}})

	async def test_streaming_events(self):
		chunks = [
			{"id": "c1", "object": "chat.completion.chunk", "created": 1, "model": "m",
				"choices": [{"index": 0, "delta": {"role": "assistant", "content": "hel"}, "finish_reason": None}]},
			{"id": "c1", "object": "chat.completion.chunk", "created": 1, "model": "m",
				"choices": [{"index": 0, "delta": {"content": "lo"}, "finish_reason": None}]},
			{"id": "c1", "object": "chat.completion.chunk", "created": 1, "model": "m",
				"choices": [{"index": 0, "delta": {"tool_calls": [{"index": 0, "id": "call_9",
					"function": {"name": "get_weather", "arguments": '{"city"'}}]}, "finish_reason": None}]},
			{"id": "c1", "object": "chat.completion.chunk", "created": 1, "model": "m",
				"choices": [{"index": 0, "delta": {"tool_calls": [{"index": 0,
					"function": {"arguments": ':"Paris"}'}}]}, "finish_reason": None}]},
			{"id": "c1", "object": "chat.completion.chunk", "created": 1, "model": "m",
				"choices": [{"index": 0, "delta": {}, "finish_reason": "tool_calls"}],
				"usage": {"prompt_tokens": 7, "completion_tokens": 5, "total_tokens": 12}},
		]
		stub = StubBackend(
			head=ResponseInfo(status=200, reason="OK", headers=[("Content-Type", "text/event-stream")]),
			chunks=[json.dumps(c).encode() for c in chunks])
		bridge = ResponsesBridge(stub)
		resp, factory = await bridge.ahandle(arequest("/v1/responses"),
			json.dumps({"model": "m", "input": "hi", "stream": True}).encode())
		self.assertEqual(resp.status, 200)
		# backend едет в chat-режиме со stream_options
		chat = json.loads(stub.last_body.decode())
		self.assertTrue(chat["stream"])
		self.assertEqual(chat.get("stream_options"), {"include_usage": True})
		events = [json.loads(c.decode()) for c in await self._astream_all(factory)]
		types = [e["type"] for e in events]
		self.assertEqual(types[0], "response.created")
		self.assertIn("response.output_text.delta", types)
		self.assertIn("response.function_call_arguments.delta", types)
		self.assertEqual(types[-1], "response.completed")
		done = events[-1]["response"]
		texts = [i["content"][0]["text"] for i in done["output"] if i["type"] == "message"]
		self.assertEqual(texts, ["hello"])
		fc = [i for i in done["output"] if i["type"] == "function_call"][0]
		self.assertEqual(fc["name"], "get_weather")
		self.assertEqual(json.loads(fc["arguments"]), {"city": "Paris"})
		self.assertEqual(done["usage"], {"input_tokens": 7, "output_tokens": 5, "total_tokens": 12})
		self.assertIn(done["id"], bridge._sessions)

	async def _astream_all(self, factory):
		gen = factory()
		out = []
		try:
			async for chunk in gen:
				out.append(chunk)
		finally:
			await gen.aclose()
		return out


if __name__ == "__main__":
	unittest.main()
