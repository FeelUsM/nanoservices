import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from nanoservices.http_backend import HttpBackend, parse_target_url


class TestParseTargetUrl(unittest.TestCase):
	def test_plain_http(self):
		host, port, use_ssl, base_path, base_query, default = parse_target_url("http://127.0.0.1:8080")
		self.assertEqual((host, port, use_ssl, base_path, base_query, default), ("127.0.0.1", 8080, False, "", "", 80))

	def test_default_http_port(self):
		_, port, use_ssl, _, _, _ = parse_target_url("http://example.com/")
		self.assertEqual(port, 80)
		self.assertFalse(use_ssl)

	def test_https_defaults(self):
		host, port, use_ssl, base_path, _, _ = parse_target_url("https://example.com")
		self.assertEqual((host, port, use_ssl, base_path), ("example.com", 443, True, ""))

	def test_base_path_and_query(self):
		_, _, _, base_path, base_query, _ = parse_target_url("https://example.com/base/api/?x=1")
		self.assertEqual(base_path, "/base/api")
		self.assertEqual(base_query, "x=1")

	def test_bad_scheme(self):
		with self.assertRaises(ValueError):
			parse_target_url("ftp://example.com")

	def test_no_scheme(self):
		with self.assertRaises(ValueError):
			parse_target_url("example.com:8080")

	def test_no_host(self):
		with self.assertRaises(ValueError):
			parse_target_url("http:///path")


class TestTargetPath(unittest.TestCase):
	def setUp(self):
		self.backend = HttpBackend("http://example.com:8080/base?k=v")

	def test_join_query_both(self):
		self.assertEqual(self.backend._target_path("/a/b?z=2"), "/base/a/b?k=v&z=2")

	def test_base_query_only(self):
		self.assertEqual(self.backend._target_path("/a"), "/base/a?k=v")

	def test_request_query_only(self):
		backend = HttpBackend("http://example.com:8080/base")
		self.assertEqual(backend._target_path("/a?z=2"), "/base/a?z=2")

	def test_no_queries_no_base(self):
		backend = HttpBackend("http://example.com")
		self.assertEqual(backend._target_path("/a/b"), "/a/b")

	def test_host_header_default_port(self):
		self.assertEqual(HttpBackend("http://example.com/x")._host_header, "example.com")

	def test_host_header_custom_port(self):
		self.assertEqual(self.backend._host_header, "example.com:8080")

	def test_https_auto_ssl(self):
		backend = HttpBackend("https://example.com/")
		self.assertEqual(backend._host_header, "example.com")
		self.assertIsNotNone(backend._ssl_ctx)

	def test_http_no_ssl(self):
		backend = HttpBackend("http://example.com/")
		self.assertIsNone(backend._ssl_ctx)

	def test_explicit_ssl_ctx_ignored_for_http(self):
		import ssl

		ctx = ssl.create_default_context()
		backend = HttpBackend("http://example.com/", ssl_ctx=ctx)
		# для http явный контекст не применяется (соединение открываем без ssl)
		self.assertIsNone(backend._ssl_ctx)


if __name__ == "__main__":
	unittest.main()
