import asyncio
import os
import re
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from nanoservices.http_common import HttpProtocolError, NetworkError
from nanoservices.http_server import HttpServer
from nanoservices.pipeline import Handler, RequestInfo, ResponseInfo


class FakeHandler(Handler):
	def __init__(self, response=None, chunks=(), error=None):
		self.response = response or ResponseInfo(status=200, reason="OK", headers=[("Content-Length", "5")])
		self.chunks = list(chunks)
		self.error = error
		self.seen = []

	async def ahandle(self, request, body):
		self.seen.append((request, body))
		if self.error is not None:
			raise self.error

		async def afread_response():
			for chunk in self.chunks:
				yield chunk

		return self.response, afread_response


async def aread_head(reader):
	return await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), 10)


async def aread_exact(reader, n):
	return await asyncio.wait_for(reader.readexactly(n), 10)


async def aread_chunked(reader):
	body = b""
	while True:
		size_line = await asyncio.wait_for(reader.readuntil(b"\r\n"), 10)
		n = int(size_line.strip().split(b";")[0], 16)
		if n == 0:
			await aread_exact(reader, 2)
			break
		body += await aread_exact(reader, n)
		await aread_exact(reader, 2)
	return body


class ServerCase(unittest.IsolatedAsyncioTestCase):
	async def astart(self, handler):
		server = HttpServer(handler, host="127.0.0.1", port=0)
		await server.astart()
		port = server._server.sockets[0].getsockname()[1]

		async def _aclose():
			await asyncio.wait_for(server.aclose(), 10)

		self.addAsyncCleanup(_aclose)
		return port

	async def aconnect(self, port):
		reader, writer = await asyncio.open_connection("127.0.0.1", port)
		self.addCleanup(writer.close)
		return reader, writer

	async def test_content_length_passthrough(self):
		handler = FakeHandler(chunks=[b"hello"])
		port = await self.astart(handler)
		reader, writer = await self.aconnect(port)
		writer.write(b"GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
		await writer.drain()
		head = await aread_head(reader)
		self.assertIn("200", head.split(b"\r\n")[0].decode())
		self.assertRegex(head.decode(), r"(?i)Content-Length: 5")
		self.assertEqual(await aread_exact(reader, 5), b"hello")
		writer.close()

	async def test_chunked_when_no_length(self):
		resp = ResponseInfo(status=200, reason="OK", headers=[("Content-Type", "text/plain")])
		handler = FakeHandler(response=resp, chunks=[b"ab", b"c"])
		port = await self.astart(handler)
		reader, writer = await self.aconnect(port)
		writer.write(b"GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
		await writer.drain()
		head = await aread_head(reader)
		self.assertRegex(head.decode(), r"(?i)Transfer-Encoding: chunked")
		self.assertEqual(await aread_chunked(reader), b"abc")
		writer.close()

	async def test_empty_chunks_skipped(self):
		resp = ResponseInfo(status=200, reason="OK", headers=[("Content-Type", "text/plain")])
		handler = FakeHandler(response=resp, chunks=[b"", b"data", b""])
		port = await self.astart(handler)
		reader, writer = await self.aconnect(port)
		writer.write(b"GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
		await writer.drain()
		await aread_head(reader)
		self.assertEqual(await aread_chunked(reader), b"data")
		writer.close()

	async def test_network_error_maps_502(self):
		handler = FakeHandler(error=NetworkError("down"))
		port = await self.astart(handler)
		reader, writer = await self.aconnect(port)
		writer.write(b"GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
		await writer.drain()
		head = await aread_head(reader)
		self.assertIn("502", head.split(b"\r\n")[0].decode())
		writer.close()

	async def test_protocol_error_maps_502(self):
		handler = FakeHandler(error=HttpProtocolError("bad"))
		port = await self.astart(handler)
		reader, writer = await self.aconnect(port)
		writer.write(b"GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
		await writer.drain()
		head = await aread_head(reader)
		self.assertIn("502", head.split(b"\r\n")[0].decode())
		writer.close()

	async def test_generic_error_maps_500(self):
		handler = FakeHandler(error=ValueError("bug"))
		port = await self.astart(handler)
		reader, writer = await self.aconnect(port)
		writer.write(b"GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
		await writer.drain()
		head = await aread_head(reader)
		self.assertIn("500", head.split(b"\r\n")[0].decode())
		writer.close()

	async def test_malformed_start_line_400(self):
		handler = FakeHandler()
		port = await self.astart(handler)
		reader, writer = await self.aconnect(port)
		writer.write(b"GARBAGE\r\n\r\n")
		await writer.drain()
		head = await aread_head(reader)
		self.assertIn("400", head.split(b"\r\n")[0].decode())
		self.assertEqual(len(handler.seen), 0)
		writer.close()

	async def test_keep_alive_two_requests(self):
		handler = FakeHandler(chunks=[b"hello"])
		port = await self.astart(handler)
		reader, writer = await self.aconnect(port)
		for _ in range(2):
			writer.write(b"GET /a HTTP/1.1\r\nHost: x\r\nConnection: keep-alive\r\n\r\n")
			await writer.drain()
			head = await aread_head(reader)
			self.assertIn("200", head.split(b"\r\n")[0].decode())
			self.assertEqual(await aread_exact(reader, 5), b"hello")
		self.assertEqual(len(handler.seen), 2)
		writer.close()

	async def test_request_body_buffered(self):
		handler = FakeHandler(chunks=[b"hello"])
		port = await self.astart(handler)
		reader, writer = await self.aconnect(port)
		body = b"x" * 1000
		writer.write(
			b"POST /echo HTTP/1.1\r\nHost: x\r\nContent-Length: 1000\r\nConnection: close\r\n\r\n" + body
		)
		await writer.drain()
		await aread_head(reader)
		await aread_exact(reader, 5)
		self.assertEqual(handler.seen[0][1], body)
		self.assertEqual(handler.seen[0][0].method, "POST")
		self.assertEqual(handler.seen[0][0].path, "/echo")
		writer.close()

	async def test_chunked_request_body(self):
		handler = FakeHandler(chunks=[b"hello"])
		port = await self.astart(handler)
		reader, writer = await self.aconnect(port)
		writer.write(
			b"POST /c HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n"
			b"5\r\nhello\r\n0\r\n\r\n"
		)
		await writer.drain()
		await aread_head(reader)
		await aread_exact(reader, 5)
		self.assertEqual(handler.seen[0][1], b"hello")
		writer.close()

	async def test_chunked_request_with_trailers(self):
		handler = FakeHandler(chunks=[b"hello"])
		port = await self.astart(handler)
		reader, writer = await self.aconnect(port)
		writer.write(
			b"POST /c HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n"
			b"5\r\nhello\r\n0\r\nX-Trailer: v\r\n\r\n"
		)
		await writer.drain()
		await aread_head(reader)
		await aread_exact(reader, 5)
		self.assertEqual(handler.seen[0][1], b"hello")
		writer.close()

	async def test_sse_rewrap(self):
		resp = ResponseInfo(status=200, reason="OK", headers=[("Content-Type", "text/event-stream")])
		handler = FakeHandler(response=resp, chunks=[b'{"n": 1}'])
		port = await self.astart(handler)
		reader, writer = await self.aconnect(port)
		writer.write(b"GET /sse HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
		await writer.drain()
		head = await aread_head(reader)
		self.assertRegex(head.decode(), r"(?i)Transfer-Encoding: chunked")
		body = await aread_chunked(reader)
		self.assertIn(b'data: {"n": 1}\n\n', body)
		self.assertTrue(body.endswith(b"data: [DONE]\n\n"))
		writer.close()


if __name__ == "__main__":
	unittest.main()
