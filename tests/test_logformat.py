import json
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import yaml

from nanoservices.pipeline import (
	_format_logged_data,
	_is_json_content,
	_pack_json_lines,
	_split_client_addr,
)


def make_json_of_length(n):
	"""JSON-текст ровно длины n (паддинг внутри строкового значения)."""
	overhead = len('{"k": ""}')
	return '{"k": "%s"}' % ("x" * (n - overhead))


class TestFormatLoggedData(unittest.TestCase):
	def test_short_json_raw(self):
		self.assertEqual(_format_logged_data(b'{"a": 1}', is_json=True), '{"a": 1}')

	def test_not_json_flag_long(self):
		self.assertEqual(_format_logged_data(b"y" * 600, is_json=False), "y" * 600)

	def test_invalid_json_long_raw(self):
		body = b"{not json, but long enough to cross the raw limit " + b"z" * 200
		self.assertTrue(_format_logged_data(body, is_json=True).startswith("{not json"))

	def test_mid_json_pretty_valid_and_narrow(self):
		obj = {"k%d" % i: "v" * 20 for i in range(12)}
		text = json.dumps(obj, ensure_ascii=False)
		self.assertTrue(200 <= len(text) < 500)
		out = _format_logged_data(text.encode(), is_json=True)
		self.assertIn("\n", out)
		self.assertLessEqual(max(len(line) for line in out.split("\n")), 200)
		self.assertEqual(json.loads(out), obj)

	def test_exactly_200_goes_pretty(self):
		text = make_json_of_length(200)
		self.assertEqual(len(text), 200)
		out = _format_logged_data(text.encode(), is_json=True)
		self.assertEqual(json.loads(out), {"k": "x" * (200 - len('{"k": ""}'))})

	def test_exactly_500_goes_yaml(self):
		text = make_json_of_length(500)
		self.assertEqual(len(text), 500)
		out = _format_logged_data(text.encode(), is_json=True)
		self.assertTrue(out.startswith("YAML\n"))
		self.assertEqual(yaml.safe_load(out.split("\n", 1)[1]), json.loads(text))

	def test_big_yaml_blocks_and_roundtrip(self):
		obj = {"f": "asdf   \n    sdfg    \n" + "x" * 600, "g": "q" * 600}
		out = _format_logged_data(json.dumps(obj).encode(), is_json=True)
		self.assertTrue(out.startswith("YAML\n"))
		self.assertIn("f: |-", out)
		# хвостовые пробелы пережили блок
		self.assertIn("  asdf   ", out)
		self.assertEqual(yaml.safe_load(out.split("\n", 1)[1]), obj)

	def test_tz_example_shape(self):
		# пример из ТЗ: значение заканчивается переносом -> chomping '|' (clip)
		obj = {"f": "asdf   \n    sdfg    \n" + "x" * 600, "g": "asdf   \n    sdfg    \n" + "y" * 600}
		out = _format_logged_data(json.dumps(obj).encode(), is_json=True)
		self.assertIn("f: |", out)
		self.assertEqual(yaml.safe_load(out.split("\n", 1)[1]), obj)


class TestPackJsonLines(unittest.TestCase):
	def test_packs_short_lines(self):
		pretty = '{\n "a": 1,\n "b": 2\n}'
		self.assertEqual(_pack_json_lines(pretty, 200), '{ "a": 1, "b": 2 }')

	def test_breaks_on_width(self):
		pretty = '{\n "a": "%s",\n "b": 1\n}' % ("x" * 100)
		out = _pack_json_lines(pretty, 50)
		self.assertTrue(all(len(line) <= 50 or len(line.strip()) > 50 for line in out.split("\n")))
		self.assertEqual(json.loads(out), json.loads(pretty))

	def test_long_single_token_kept_valid(self):
		obj = {"k": "x" * 300}
		pretty = json.dumps(obj, indent=1)
		out = _pack_json_lines(pretty, 200)
		self.assertEqual(json.loads(out), obj)


class TestHelpers(unittest.TestCase):
	def test_split_tuple(self):
		self.assertEqual(_split_client_addr(("127.0.0.1", 54321)), ("127.0.0.1", "54321"))

	def test_split_ipv6_colons_sanitized(self):
		host, port = _split_client_addr(("::1", 80))
		self.assertNotIn(":", host)
		self.assertEqual(port, "80")

	def test_split_nontuple(self):
		self.assertEqual(_split_client_addr("unix-sock"), ("unix-sock", "?"))

	def test_is_json_content_type(self):
		headers = [("Content-Type", "application/json; charset=utf-8")]
		self.assertTrue(_is_json_content(headers, "Content-Type"))

	def test_is_json_case_insensitive(self):
		headers = [("ACCEPT", "Application/JSON")]
		self.assertTrue(_is_json_content(headers, "accept"))

	def test_is_not_json(self):
		self.assertFalse(_is_json_content([("Content-Type", "text/plain")], "Content-Type"))
		self.assertFalse(_is_json_content([], "Content-Type"))


if __name__ == "__main__":
	unittest.main()
