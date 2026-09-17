import asyncio
import json
import os
import re
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from nanoservices.http_backend import HttpBackend
from nanoservices.http_server import HttpServer
from nanoservices.pipeline import StreamLogger

JSON_BODY = json.dumps({"hello": "world", "n": list(range(30))}).encode()


async def astub(reader, writer):
	"""Тестовый backend: /json, /sse (пауза 1.2с), /stall (долгий стрим)."""
	try:
		while True:
			try:
				head = await reader.readuntil(b"\r\n\r\n")
			except (asyncio.IncompleteReadError, asyncio.LimitOverrunError):
				return
			parts = head.split(b"\r\n")[0].decode().split(" ", 2)
			path = parts[1] if len(parts) == 3 else "/"
			clen = 0
			for hline in head.split(b"\r\n"):
				if hline.lower().startswith(b"content-length:"):
					clen = int(hline.split(b":")[1].strip())
			if clen:
				await reader.readexactly(clen)
			if path == "/json":
				writer.write(
					b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: %d\r\n"
					b"Connection: keep-alive\r\n\r\n" % len(JSON_BODY) + JSON_BODY
				)
				await writer.drain()
			elif path == "/sse":
				evs = [b'data: {"n": 1}\n\n', b'data: {"n": 2}\n\n', b"data: [DONE]\n\n"]
				writer.write(
					b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: %d\r\n"
					b"Connection: keep-alive\r\n\r\n" % sum(map(len, evs))
				)
				await writer.drain()
				writer.write(evs[0])
				await writer.drain()
				await asyncio.sleep(1.2)
				writer.write(evs[1] + evs[2])
				await writer.drain()
			elif path == "/stall":
				writer.write(
					b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n"
					b"Content-Length: 100000\r\nConnection: keep-alive\r\n\r\n"
				)
				await writer.drain()
				try:
					for _ in range(30):
						writer.write(b"0123456789")
						await writer.drain()
						await asyncio.sleep(0.2)
				except (ConnectionError, OSError):
					return
				await asyncio.sleep(5)
			else:
				writer.write(b"HTTP/1.1 404 NF\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n")
				await writer.drain()
	except (ConnectionError, asyncio.IncompleteReadError):
		return
	finally:
		writer.close()


async def aread_head(reader):
	return await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), 10)


async def aread_chunked(reader):
	body = b""
	while True:
		size_line = await asyncio.wait_for(reader.readuntil(b"\r\n"), 10)
		n = int(size_line.strip().split(b";")[0], 16)
		if n == 0:
			await asyncio.wait_for(reader.readexactly(2), 10)
			break
		body += await asyncio.wait_for(reader.readexactly(n), 10)
		await asyncio.wait_for(reader.readexactly(2), 10)
	return body


class ProxyCase(unittest.IsolatedAsyncioTestCase):
	async def asyncSetUp(self):
		self.stub = await asyncio.start_server(astub, "127.0.0.1", 0)
		self.bport = self.stub.sockets[0].getsockname()[1]
		self.tmp = Path(tempfile.mkdtemp(prefix="nanoproxy-"))
		self.console = []
		self.backend = HttpBackend(f"http://127.0.0.1:{self.bport}")
		self.pipe = StreamLogger(self.backend, self.tmp, 100, log=self.console.append)
		self.server = HttpServer(self.pipe, host="127.0.0.1", port=0)
		await self.server.astart()
		self.pport = self.server._server.sockets[0].getsockname()[1]

	async def asyncTearDown(self):
		# порядок важен: сначала backend (будит висящие чтения),
		# потом сервер (wait_closed ждёт открытые соединения)
		await self.backend.aclose_all()
		await asyncio.wait_for(self.server.aclose(), 10)
		self.stub.close()
		await self.stub.wait_closed()

	async def aconnect_tracked(self):
		reader, writer = await asyncio.open_connection("127.0.0.1", self.pport)
		self.addCleanup(writer.close)
		return reader, writer

	async def arequest(self, path, accept_json=True):
		reader, writer = await self.aconnect_tracked()
		hdrs = f"GET {path} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n"
		if accept_json:
			hdrs += "Accept: application/json\r\n"
		writer.write((hdrs + "\r\n").encode())
		await writer.drain()
		head = await aread_head(reader)
		htext = head.decode()
		m = re.search(r"Content-Length: (\d+)", htext, re.I)
		if m:
			body = await asyncio.wait_for(reader.readexactly(int(m.group(1))), 10)
		elif "chunked" in htext.lower():
			body = await aread_chunked(reader)
		else:
			body = b""
		writer.close()
		return htext, body

	def log_files(self):
		return sorted(self.tmp.glob("*.txt"))

	async def test_json_passthrough_and_log(self):
		head, body = await self.arequest("/json")
		self.assertIn("200", head.split("\r\n")[0])
		self.assertEqual(body, JSON_BODY)
		await asyncio.sleep(0.2)
		files = self.log_files()
		self.assertEqual(len(files), 1)
		self.assertRegex(files[0].name, r"^\d{6}-\d{6}\.\d{3}-.+-\d+\.txt$")
		text = files[0].read_text(encoding="utf-8")
		self.assertIn("GET /json HTTP/1.1", text)
		self.assertIn("RESPOND", text)
		self.assertIn("END", text)
		self.assertTrue(any("-> GET /json" in m for m in self.console))
		self.assertTrue(any("<- 200" in m for m in self.console))

	async def test_sse_rewrap_clean_and_gap(self):
		head, body = await self.arequest("/sse", accept_json=False)
		self.assertIn(b'data: {"n": 1}\n\n', body)
		self.assertTrue(body.endswith(b"data: [DONE]\n\n"))
		await asyncio.sleep(0.2)
		text = self.log_files()[-1].read_text(encoding="utf-8")
		# внутри конвейера — чистые payload, без обвязки
		self.assertNotIn("data: {", text)
		self.assertIn('{"n": 1}', text)
		self.assertIn('{"n": 2}', text)
		# пауза 1.2с между событиями помечена
		self.assertIsNotNone(re.search(r"\+\d+\.\d+s / \+\d+\.\d+s", text))

	async def test_abort_then_server_alive(self):
		reader, writer = await self.aconnect_tracked()
		writer.write(b"GET /stall HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
		await writer.drain()
		await aread_head(reader)
		writer.close()
		await asyncio.sleep(2.5)
		head, body = await self.arequest("/json")
		self.assertIn("200", head.split("\r\n")[0])
		self.assertEqual(body, JSON_BODY)
		await asyncio.sleep(0.2)
		stall = [f for f in self.log_files() if "/stall" in f.read_text(encoding="utf-8")]
		self.assertEqual(len(stall), 1)
		self.assertIn("ABORT", stall[0].read_text(encoding="utf-8"))


class RotationCase(unittest.IsolatedAsyncioTestCase):
	async def test_rotation(self):
		stub = await asyncio.start_server(astub, "127.0.0.1", 0)
		bport = stub.sockets[0].getsockname()[1]
		tmp = Path(tempfile.mkdtemp(prefix="nanorot-"))
		backend = HttpBackend(f"http://127.0.0.1:{bport}")
		pipe = StreamLogger(backend, tmp, 2, log=lambda m: None)
		server = HttpServer(pipe, host="127.0.0.1", port=0)
		await server.astart()
		pport = server._server.sockets[0].getsockname()[1]
		try:
			for _ in range(3):
				reader, writer = await asyncio.open_connection("127.0.0.1", pport)
				try:
					writer.write(b"GET /json HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
					await writer.drain()
					await aread_head(reader)
				finally:
					writer.close()
				await asyncio.sleep(0.05)
			await asyncio.sleep(0.2)
			self.assertEqual(len(list(tmp.glob("*.txt"))), 2)
		finally:
			await backend.aclose_all()
			await asyncio.wait_for(server.aclose(), 10)
			stub.close()
			await stub.wait_closed()


if __name__ == "__main__":
	unittest.main()
