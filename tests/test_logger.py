import asyncio
import os
import re
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from nanoservices.pipeline import Handler, RequestInfo, ResponseInfo, StreamLogger

FILENAME_RE = re.compile(r"^\d{6}-\d{6}\.\d{3}-.+-\d+(-1)?\.txt$")
GAP_RE = re.compile(r"\+\d+\.\d+s / \+\d+\.\d+s")


async def agen(chunks, delay=0.0, error=None):
	for chunk in chunks:
		if delay:
			await asyncio.sleep(delay)
		yield chunk
	if error is not None:
		raise error


class FakeHandler(Handler):
	"""Подменяемый низлежащий handler: canned-заголовки, чанки, опциональная ошибка."""

	def __init__(self, response=None, chunks=(), delay=0.0, error=None, fail_body=False):
		self.response = response or ResponseInfo(status=200, reason="OK", headers=[("Content-Length", "0")])
		self.chunks = list(chunks)
		self.delay = delay
		self.error = error
		self.fail_body = fail_body
		self.seen = []

	async def ahandle(self, request, body):
		self.seen.append((request, body))
		if self.error is not None and not self.fail_body:
			raise self.error

		async def afread_response():
			async for chunk in agen(self.chunks, self.delay):
				yield chunk
			if self.error is not None and self.fail_body:
				raise self.error

		return self.response, afread_response


class StreamLoggerCase(unittest.IsolatedAsyncioTestCase):
	def setUp(self):
		self.tmp = Path(tempfile.mkdtemp(prefix="nanotest-"))
		self.console = []
		self.req = RequestInfo(
			method="POST",
			path="/api?x=1",
			http_version="HTTP/1.1",
			headers=[("Host", "h"), ("Content-Type", "application/json")],
			client_addr=("127.0.0.1", 54321),
		)

	def logger(self, handler, max_files=100):
		return StreamLogger(handler, self.tmp, max_files, log=self.console.append)

	def files(self):
		return sorted(self.tmp.glob("*.txt"))

	async def test_file_layout(self):
		pipe = self.logger(FakeHandler(chunks=[b"one", b"two"]))
		resp, afread = await pipe.ahandle(self.req, b'{"a": 1}')
		out = []
		async for chunk in afread():
			out.append(chunk)
		self.assertEqual(out, [b"one", b"two"])  # passthrough без изменений
		self.assertEqual(len(self.files()), 1)
		name = self.files()[0].name
		self.assertRegex(name, FILENAME_RE)
		self.assertIn("127.0.0.1-54321", name)
		text = self.files()[0].read_text(encoding="utf-8")
		self.assertIn("POST /api?x=1 HTTP/1.1", text)
		self.assertIn("Content-Type: application/json", text)
		self.assertIn('{"a": 1}', text)
		self.assertIn("RESPOND", text)
		self.assertIn("HTTP/1.1 200 OK", text)
		self.assertIn("one\ntwo", text)
		self.assertIn("END", text)
		self.assertTrue(any("-> POST /api?x=1" in m for m in self.console))
		self.assertTrue(any("<- 200 OK" in m and "2 частях" in m for m in self.console))

	async def test_response_status_without_reason(self):
		handler = FakeHandler(response=ResponseInfo(status=204, headers=[]), chunks=[])
		pipe = self.logger(handler)
		_, afread = await pipe.ahandle(self.req, b"")
		async for _ in afread():
			pass
		self.assertIn("HTTP/1.1 204\n", self.files()[0].read_text(encoding="utf-8"))

	async def test_no_gap_no_separator(self):
		pipe = self.logger(FakeHandler(chunks=[b"a", b"b"]))
		_, afread = await pipe.ahandle(self.req, b"")
		async for _ in afread():
			pass
		self.assertIsNone(GAP_RE.search(self.files()[0].read_text(encoding="utf-8")))

	async def test_gap_separator(self):
		pipe = self.logger(FakeHandler(chunks=[b"a", b"b"], delay=1.15))
		_, afread = await pipe.ahandle(self.req, b"")
		async for _ in afread():
			pass
		text = self.files()[0].read_text(encoding="utf-8")
		self.assertIsNotNone(GAP_RE.search(text))

	async def test_abort_on_aclose(self):
		pipe = self.logger(FakeHandler(chunks=[b"a", b"b", b"c"]))
		_, afread = await pipe.ahandle(self.req, b"")
		gen = afread()
		self.assertEqual(await gen.__anext__(), b"a")
		await gen.aclose()
		text = self.files()[0].read_text(encoding="utf-8")
		self.assertIn("ABORT", text)
		self.assertNotIn("END", text)
		self.assertTrue(any("<- 200" in m for m in self.console))

	async def test_inner_closed_deterministically(self):
		closed = []

		class ClosingHandler(Handler):
			async def ahandle(self, request, body):
				async def afread_response():
					try:
						yield b"a"
						yield b"b"
					finally:
						closed.append(True)

				return ResponseInfo(status=200, reason="OK", headers=[]), afread_response

		pipe = self.logger(ClosingHandler())
		_, afread = await pipe.ahandle(self.req, b"")
		gen = afread()
		self.assertEqual(await gen.__anext__(), b"a")
		await gen.aclose()
		# без участия сборщика мусора: внутренний генератор уже закрыт
		self.assertEqual(closed, [True])

	async def test_body_error_logged(self):
		pipe = self.logger(FakeHandler(chunks=[b"a"], error=RuntimeError("boom"), fail_body=True))
		_, afread = await pipe.ahandle(self.req, b"")
		with self.assertRaises(RuntimeError):
			async for _ in afread():
				pass
		text = self.files()[0].read_text(encoding="utf-8")
		self.assertIn("ERROR", text)
		self.assertIn("boom", text)
		self.assertTrue(any("ошибка в теле ответа" in m for m in self.console))

	async def test_handler_error_logged(self):
		pipe = self.logger(FakeHandler(error=ConnectionError("down")))
		with self.assertRaises(ConnectionError):
			await pipe.ahandle(self.req, b"")
		text = self.files()[0].read_text(encoding="utf-8")
		self.assertIn("ERROR", text)
		self.assertNotIn("RESPOND", text)
		self.assertTrue(any("ошибка конвейера" in m for m in self.console))

	async def test_rotation(self):
		pipe = self.logger(FakeHandler(chunks=[b"x"]), max_files=2)
		for _ in range(3):
			_, afread = await pipe.ahandle(self.req, b"")
			async for _ in afread():
				pass
		self.assertEqual(len(self.files()), 2)

	async def test_rotation_ignores_non_txt(self):
		(self.tmp / "keep.log").write_text("x")
		pipe = self.logger(FakeHandler(chunks=[b"x"]), max_files=1)
		for _ in range(2):
			_, afread = await pipe.ahandle(self.req, b"")
			async for _ in afread():
				pass
		self.assertEqual(len(list(self.tmp.glob("*.txt"))), 1)
		self.assertTrue((self.tmp / "keep.log").exists())

	async def test_name_collision_suffix(self):
		pipe = self.logger(FakeHandler())
		first = pipe._new_log_path("260101-000000.000", "h", "1")
		second = pipe._new_log_path("260101-000000.000", "h", "1")
		self.assertTrue(first.name.endswith(".txt"))
		self.assertTrue(second.name.endswith("-1.txt"))

	async def test_request_body_json_formatted(self):
		big = b'{"k": "' + b"v" * 600 + b'"}'
		pipe = self.logger(FakeHandler(chunks=[]))
		_, afread = await pipe.ahandle(self.req, big)
		async for _ in afread():
			pass
		text = self.files()[0].read_text(encoding="utf-8")
		self.assertIn("YAML", text)

	async def test_response_chunk_json_by_accept(self):
		req = RequestInfo(
			method="GET", path="/", http_version="HTTP/1.1",
			headers=[("Accept", "application/json")], client_addr=("h", 1),
		)
		handler = FakeHandler(
			response=ResponseInfo(status=200, reason="OK", headers=[("Content-Type", "text/plain")]),
			chunks=[b'{"k": "' + b"v" * 600 + b'"}'],
		)
		pipe = self.logger(handler)
		_, afread = await pipe.ahandle(req, b"")
		async for _ in afread():
			pass
		self.assertIn("YAML", self.files()[0].read_text(encoding="utf-8"))


if __name__ == "__main__":
	unittest.main()
