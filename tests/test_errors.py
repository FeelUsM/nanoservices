"""Методика обработки ошибок: заворот сырых исключений, маппинг в статусы, LogError.

Отступы — табуляция, методы — test_* (требование discovery).
"""

import asyncio
import os
import socket
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from nanoservices.http_backend import HttpBackend
from nanoservices.http_common import (
	HttpProtocolError,
	NetworkError,
	afdecode_sse,
	afread_body,
	afread_until_close,
	aread_headers_block,
)
from nanoservices.http_server import HttpServer
from nanoservices.pipeline import Handler, LogError, RequestInfo, ResponseInfo, StreamLogger
from nanoservices.pipeline import _format_logged_data


class FailingReader:
	"""Заглушка StreamReader: каждый метод кидает заданное исключение."""

	def __init__(self, error):
		self._error = error

	async def read(self, n=-1):
		raise self._error

	async def readuntil(self, sep):
		raise self._error

	async def readexactly(self, n):
		raise self._error


async def afail_source(error):
	yield b"never"
	raise error  # pragma: no cover


async def agen(chunks):
	for chunk in chunks:
		yield chunk


class CommonWrapCase(unittest.IsolatedAsyncioTestCase):
	async def test_headers_oserror_maps_network(self):
		with self.assertRaises(NetworkError):
			await aread_headers_block(FailingReader(ConnectionResetError()))

	async def test_headers_timeout_maps_network(self):
		with self.assertRaises(NetworkError):
			await aread_headers_block(FailingReader(TimeoutError("timed out")))

	async def test_headers_limit_overrun_maps_protocol(self):
		with self.assertRaises(HttpProtocolError):
			await aread_headers_block(FailingReader(asyncio.LimitOverrunError("over", "x")))

	async def test_content_length_oserror_maps_network(self):
		with self.assertRaises(NetworkError):
			async for _ in afread_body(
				FailingReader(ConnectionResetError()),
				[("Content-Length", "10")],
				allow_until_close=False,
			):
				pass

	async def test_bad_content_length_maps_protocol(self):
		with self.assertRaises(HttpProtocolError):
			async for _ in afread_body(
				FailingReader(ConnectionResetError()),
				[("Content-Length", "abc")],
				allow_until_close=False,
			):
				pass

	async def test_chunked_size_line_oserror_maps_network(self):
		with self.assertRaises(NetworkError):
			async for _ in afread_body(
				FailingReader(ConnectionResetError()),
				[("Transfer-Encoding", "chunked")],
				allow_until_close=False,
			):
				pass

	async def test_chunked_size_line_limit_overrun_maps_protocol(self):
		with self.assertRaises(HttpProtocolError):
			async for _ in afread_body(
				FailingReader(asyncio.LimitOverrunError("over", "x")),
				[("Transfer-Encoding", "chunked")],
				allow_until_close=False,
			):
				pass

	async def test_until_close_oserror_maps_network(self):
		with self.assertRaises(NetworkError):
			async for _ in afread_until_close(FailingReader(OSError("boom"))):
				pass

	async def test_sse_transport_oserror_maps_network(self):
		with self.assertRaises(NetworkError):
			async for _ in afdecode_sse(afail_source(ConnectionResetError())):
				pass

	async def test_sse_known_types_passthrough(self):
		# уже классифицированные ошибки SSE не переклассифицирует
		with self.assertRaises(NetworkError):
			async for _ in afdecode_sse(afail_source(NetworkError("down"))):
				pass
		with self.assertRaises(HttpProtocolError):
			async for _ in afdecode_sse(afail_source(HttpProtocolError("bad"))):
				pass

	async def test_sse_clean_stream_unaffected(self):
		out = [chunk async for chunk in afdecode_sse(agen([b"data: one\n\n", b"data: two\n\n"]))]
		self.assertEqual(out, [b"one", b"two"])


class FakeConn:
	def __init__(self):
		self.lock = asyncio.Lock()
		self.alive = True
		self.closed = False

	async def aclose(self):
		self.closed = True


class BackendWrapCase(unittest.IsolatedAsyncioTestCase):
	async def test_open_connection_reset_maps_network(self):
		backend = HttpBackend("http://127.0.0.1:1")
		with mock.patch("asyncio.open_connection", side_effect=ConnectionResetError()):
			with self.assertRaises(NetworkError):
				await backend._aopen_connection()

	async def test_open_connection_timeout_maps_network(self):
		backend = HttpBackend("http://127.0.0.1:1")
		with mock.patch("asyncio.open_connection", side_effect=asyncio.TimeoutError()):
			with self.assertRaises(NetworkError):
				await backend._aopen_connection()

	async def test_transparent_retry_on_stale_keepalive(self):
		backend = HttpBackend("http://127.0.0.1:1")
		stale, fresh = FakeConn(), FakeConn()
		calls = []

		async def fake_exchange(conn, request, body):
			calls.append(conn)
			if len(calls) == 1:
				raise NetworkError("протухшее keep-alive")
			return ResponseInfo(status=200, reason="OK", headers=[]), agen([b"ok"]), True

		backend._aexchange = fake_exchange
		backend._aopen_connection = _const_coro(fresh)
		addr = ("127.0.0.1", 9999)
		backend._connections[addr] = stale
		logged = []
		backend._log = logged.append
		req = RequestInfo(method="GET", path="/", http_version="HTTP/1.1", headers=[], client_addr=addr)
		resp, afread = await backend.ahandle(req, b"")
		self.assertEqual(resp.status, 200)
		self.assertEqual([c async for c in afread()], [b"ok"])
		self.assertEqual(calls, [stale, fresh])  # один прозрачный повтор
		self.assertTrue(stale.closed)
		self.assertTrue(any(m.startswith("HttpBackend:") for m in logged))

	async def test_raw_oserror_in_body_drops_connection(self):
		backend = HttpBackend("http://127.0.0.1:1")
		conn = FakeConn()
		addr = ("127.0.0.1", 9998)
		backend._connections[addr] = conn

		async def fake_exchange(c, request, body):
			return ResponseInfo(status=200, reason="OK", headers=[]), afail_source(OSError("rst")), True

		backend._aexchange = fake_exchange
		req = RequestInfo(method="GET", path="/", http_version="HTTP/1.1", headers=[], client_addr=addr)
		_, afread = await backend.ahandle(req, b"")
		with self.assertRaises(NetworkError):
			async for _ in afread():
				pass
		await asyncio.sleep(0)  # drop уходит в create_task — даём ему отработать
		await asyncio.sleep(0)
		self.assertNotIn(addr, backend._connections)  # битое соединение в пул не вернулось


def _const_coro(value):
	async def _aget():
		return value

	return _aget


class FakeHandler(Handler):
	def __init__(self, error=None):
		self.error = error

	async def ahandle(self, request, body):
		if self.error is not None:
			raise self.error

		async def afread_response():
			yield b""

		return ResponseInfo(status=200, reason="OK", headers=[("Content-Length", "0")]), afread_response


class ServerMappingCase(unittest.IsolatedAsyncioTestCase):
	async def astart(self, handler):
		server = HttpServer(handler, host="127.0.0.1", port=0)
		await server.astart()
		port = server._server.sockets[0].getsockname()[1]

		async def _aclose():
			await asyncio.wait_for(server.aclose(), 10)

		self.addAsyncCleanup(_aclose)
		return port

	async def astatus(self, port):
		reader, writer = await asyncio.open_connection("127.0.0.1", port)
		self.addCleanup(writer.close)
		writer.write(b"GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
		await writer.drain()
		head = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), 10)
		writer.close()
		return head.split(b"\r\n")[0].decode()

	async def test_raw_oserror_maps_502(self):
		port = await self.astart(FakeHandler(error=ConnectionResetError()))
		self.assertIn("502", await self.astatus(port))

	async def test_raw_timeout_maps_504(self):
		port = await self.astart(FakeHandler(error=TimeoutError("slow backend")))
		self.assertIn("504", await self.astatus(port))

	async def test_logerror_maps_500(self):
		port = await self.astart(FakeHandler(error=LogError("disk dead")))
		self.assertIn("500", await self.astatus(port))


class LoggerErrorsCase(unittest.IsolatedAsyncioTestCase):
	def setUp(self):
		self.tmp = Path(tempfile.mkdtemp(prefix="nanoerr-"))
		self.console = []
		self.req = RequestInfo(
			method="GET",
			path="/",
			http_version="HTTP/1.1",
			headers=[],
			client_addr=("127.0.0.1", 1111),
		)

	async def test_console_lines_start_with_class(self):
		class OkHandler(Handler):
			async def ahandle(self, request, body):
				async def afread_response():
					yield b"x"

				return ResponseInfo(status=200, reason="OK", headers=[]), afread_response

		pipe = StreamLogger(OkHandler(), self.tmp, 10, log=self.console.append)
		_, afread = await pipe.ahandle(self.req, b"")
		async for _ in afread():
			pass
		self.assertTrue(self.console)
		for line in self.console:
			self.assertTrue(line.startswith("StreamLogger:"), line)

	async def test_file_write_error_bright_and_logerror(self):
		pipe = StreamLogger(FakeHandler(), self.tmp, 10, log=self.console.append)
		with mock.patch("builtins.open", side_effect=OSError("disk full")):
			with self.assertRaises(LogError):
				await pipe.ahandle(self.req, b"")
		bright = [m for m in self.console if "!!!" in m]
		self.assertTrue(bright, self.console)
		self.assertTrue(bright[0].startswith("StreamLogger:"), bright[0])
		self.assertIn("\x1b[", bright[0])  # ANSI-подсветка

	async def test_mkdir_error_bright_and_logerror(self):
		blocker = self.tmp / "blocker"
		blocker.write_text("x")
		with self.assertRaises(LogError):
			StreamLogger(FakeHandler(), blocker / "sub", 10, log=self.console.append)
		self.assertTrue(any("!!!" in m and m.startswith("StreamLogger:") for m in self.console))

	async def test_disk_death_does_not_mask_original_error(self):
		class DownHandler(Handler):
			async def ahandle(self, request, body):
				raise NetworkError("backend down")

		pipe = StreamLogger(DownHandler(), self.tmp, 10, log=self.console.append)
		real_open = open
		calls = []

		def flaky_open(*args, **kwargs):
			calls.append(1)
			if len(calls) > 3:  # запрос уже записан — диск умирает на sep(ERROR)
				raise OSError("disk full")
			return real_open(*args, **kwargs)

		with mock.patch("builtins.open", side_effect=flaky_open):
			with self.assertRaises(NetworkError):  # исходная, а не LogError от sep
				await pipe.ahandle(self.req, b"")

	async def test_yaml_failure_never_drops_request(self):
		big = b'{"k": "' + b"v" * 600 + b'"}'
		with mock.patch("nanoservices.pipeline.yaml.dump", side_effect=ValueError("bad")):
			self.assertEqual(_format_logged_data(big, is_json=True), big.decode("utf-8"))


class BackendNoTimeoutCase(unittest.IsolatedAsyncioTestCase):
	async def test_default_is_no_timeout(self):
		# решает клиент разрывом — по умолчанию ждём бесконечно
		self.assertIsNone(HttpBackend("http://127.0.0.1:1")._connect_timeout)

	async def test_explicit_timeout_still_enforced(self):
		backend = HttpBackend("http://127.0.0.1:1", connect_timeout=0.05)

		async def slow(*args, **kwargs):
			await asyncio.sleep(30)
			return object(), object()

		with mock.patch("asyncio.open_connection", side_effect=slow):
			with self.assertRaises(NetworkError):
				await backend._aopen_connection()

	async def test_none_timeout_waits(self):
		backend = HttpBackend("http://127.0.0.1:1")

		async def slow_ok(*args, **kwargs):
			await asyncio.sleep(0.3)  # дольше любого бывшего лимита — не должно резать
			return object(), object()

		with mock.patch("asyncio.open_connection", side_effect=slow_ok):
			conn = await asyncio.wait_for(backend._aopen_connection(), 10)
		self.assertIsNotNone(conn)


class FirstHangsHandler(Handler):
	"""Первый запрос висит вечно, остальные — сразу 200."""

	def __init__(self):
		self.calls = 0
		self.entered = asyncio.Event()
		self.cancelled = asyncio.Event()

	async def ahandle(self, request, body):
		self.calls += 1
		if self.calls == 1:
			self.entered.set()
			try:
				await asyncio.sleep(3600)
			except asyncio.CancelledError:
				self.cancelled.set()
				raise

		async def afread_response():
			yield b"hi"

		return ResponseInfo(status=200, reason="OK", headers=[("Content-Length", "2")]), afread_response


class ClientAbortCase(unittest.IsolatedAsyncioTestCase):
	async def test_client_close_aborts_backend_wait(self):
		handler = FirstHangsHandler()
		server = HttpServer(handler, host="127.0.0.1", port=0)
		await server.astart()
		port = server._server.sockets[0].getsockname()[1]

		async def _aclose():
			await asyncio.wait_for(server.aclose(), 10)

		self.addAsyncCleanup(_aclose)

		reader, writer = await asyncio.open_connection("127.0.0.1", port)
		self.addCleanup(writer.close)
		writer.write(b"GET /hang HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
		await writer.drain()
		await asyncio.wait_for(handler.entered.wait(), 10)
		writer.close()  # клиент решил прервать — без всяких таймаутов
		await asyncio.wait_for(handler.cancelled.wait(), 10)

		# сервер жив: следующий запрос обслуживается
		reader2, writer2 = await asyncio.open_connection("127.0.0.1", port)
		self.addCleanup(writer2.close)
		writer2.write(b"GET /ok HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
		await writer2.drain()
		head = await asyncio.wait_for(reader2.readuntil(b"\r\n\r\n"), 10)
		self.assertIn("200", head.split(b"\r\n")[0].decode())
		body = await asyncio.wait_for(reader2.readexactly(2), 10)
		self.assertEqual(body, b"hi")
		writer2.close()


class ClientWatchCase(unittest.IsolatedAsyncioTestCase):
	async def test_watcher_returns_on_peer_close(self):
		s1, s2 = socket.socketpair()
		try:
			s1.setblocking(False)
			s2.close()
			await asyncio.wait_for(HttpServer._aclient_closed(s1), 10)
		finally:
			s1.close()

	async def test_watcher_survives_live_socket(self):
		s1, s2 = socket.socketpair()
		try:
			s1.setblocking(False)
			s2.setblocking(False)
			watch = asyncio.create_task(HttpServer._aclient_closed(s1))
			await asyncio.sleep(0.1)
			self.assertFalse(watch.done())  # жив — не срабатывает
			watch.cancel()
			await asyncio.gather(watch, return_exceptions=True)
		finally:
			s1.close()
			s2.close()

	async def test_transportsocket_has_no_recv_but_dup_peeks(self):
		# Ловушка, в которую уже наступали: extra-info сокет — это
		# asyncio.trsock.TransportSocket без recv; пикать надо через fromfd-dup.
		seen = {}

		async def handle(reader, writer):
			await reader.readuntil(b"\r\n\r\n")  # как прод: запрос вычитан до пика
			raw = writer.get_extra_info("socket")
			seen["has_recv"] = hasattr(raw, "recv")
			dup = socket.fromfd(raw.fileno(), raw.family, raw.type, raw.proto)
			dup.setblocking(False)
			try:
				seen["peek"] = dup.recv(1, socket.MSG_PEEK)
			except BlockingIOError:
				seen["peek"] = None
			finally:
				dup.close()
			writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
			await writer.drain()
			writer.close()

		srv = await asyncio.start_server(handle, "127.0.0.1", 0)
		port = srv.sockets[0].getsockname()[1]
		async with srv:
			reader, writer = await asyncio.open_connection("127.0.0.1", port)
			writer.write(b"GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
			await writer.drain()
			await asyncio.wait_for(reader.read(-1), 10)
			writer.close()
		self.assertFalse(seen["has_recv"])
		self.assertIsNone(seen["peek"])  # запрос вычитан — данных нет, соединение живо


if __name__ == "__main__":
	unittest.main()
