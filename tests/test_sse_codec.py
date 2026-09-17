import asyncio
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from nanoservices.http_common import (
	afdecode_sse,
	encode_sse_done,
	encode_sse_message,
)


async def collect(agen):
	out = []
	async for chunk in agen:
		out.append(chunk)
	return out


def run(coro):
	loop = asyncio.new_event_loop()
	try:
		return loop.run_until_complete(coro)
	finally:
		loop.close()


async def asource(chunks):
	for chunk in chunks:
		yield chunk


class TestDecodeSse(unittest.TestCase):
	def decode(self, chunks):
		return run(collect(afdecode_sse(asource(chunks))))

	def test_strips_data_prefix(self):
		self.assertEqual(self.decode([b"data: hello\n\n"]), [b"hello"])

	def test_multiline_data_joined(self):
		self.assertEqual(self.decode([b"data: one\ndata: two\n\n"]), [b"one\ntwo"])

	def test_comments_and_fields_skipped(self):
		raw = b": comment\nevent: x\nid: 1\nretry: 3\ndata: payload\n\n"
		self.assertEqual(self.decode([raw]), [b"payload"])

	def test_done_terminates(self):
		raw = b"data: a\n\ndata: [DONE]\n\ndata: b\n\n"
		self.assertEqual(self.decode([raw]), [b"a"])

	def test_split_across_chunks(self):
		self.assertEqual(self.decode([b"data: he", b"llo\n", b"\ndata: bye\n\n"]), [b"hello", b"bye"])

	def test_no_trailing_blank_line(self):
		self.assertEqual(self.decode([b"data: tail"]), [b"tail"])

	def test_space_after_colon_optional(self):
		self.assertEqual(self.decode([b"data:nospace\n\n"]), [b"nospace"])
		self.assertEqual(self.decode([b"data:  two-spaces\n\n"]), [b" two-spaces"])

	def test_event_without_data_skipped(self):
		self.assertEqual(self.decode([b"event: ping\n\n"]), [])

	def test_crlf_tolerated(self):
		self.assertEqual(self.decode([b"data: a\r\n\r\ndata: b\r\n\r\n"]), [b"a", b"b"])


class TestEncodeSse(unittest.TestCase):
	def test_message_framing(self):
		self.assertEqual(encode_sse_message(b"hi"), b"data: hi\n\n")

	def test_multiline_message(self):
		self.assertEqual(encode_sse_message(b"a\nb"), b"data: a\ndata: b\n\n")

	def test_done(self):
		self.assertEqual(encode_sse_done(), b"data: [DONE]\n\n")

	def test_roundtrip(self):
		payloads = [b"one", b"two\nlines"]
		framed = [encode_sse_message(p) for p in payloads]
		back = run(collect(afdecode_sse(asource(framed))))
		self.assertEqual(back, payloads)


if __name__ == "__main__":
	unittest.main()
