from __future__ import annotations

import asyncio
import ssl
import time
from typing import Any, AsyncIterator, Optional

from .http_common import (
	HttpProtocolError,
	NetworkError,
	afread_body,
	afread_sse_events,
	aread_headers_block,
	body_has_definite_length,
	get_header,
	parse_headers_block,
)
from .pipeline import AFReadChunk, AResponseInfo, AResponseStart, ARequestInfo, Handler


class _ABackendConnection:
	"""Одно keep-alive TCP/TLS-соединение до backend'а + лок на его использование."""

	def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
		self.reader = reader
		self.writer = writer
		self.lock = asyncio.Lock()
		self.alive = True
		self.last_used = time.monotonic()

	async def aclose(self) -> None:
		self.alive = False
		self.writer.close()
		try:
			await self.writer.wait_closed()
		except Exception:
			pass


class HttpBackend(Handler):
	"""
	Handler, который форвардит запрос в реальный HTTP backend.

	Создание экземпляра НЕ открывает соединений — это происходит лениво, при первом запросе
	от конкретного client_addr. На каждого клиента фронтенда держится одно keep-alive
	соединение до backend'а; если backend его незаметно закрыл (обычный keep-alive timeout),
	HttpBackend один раз прозрачно переподключается и повторяет запрос.

	Стриминг поддержан только на стороне ответа: тело запроса вычитывается целиком перед
	отправкой в backend, а тело ответа стримится клиенту по мере поступления (chunked
	transfer-encoding или Server-Sent Events по заголовку Content-Type: text/event-stream).
	"""

	def __init__(
		self,
		target_host: str,
		target_port: int,
		*,
		ssl_ctx: Optional[ssl.SSLContext] = None,
		connect_timeout: float = 10.0,
	) -> None:
		self._target_host = target_host
		self._target_port = target_port
		self._ssl_ctx = ssl_ctx
		self._connect_timeout = connect_timeout
		self._connections: dict[Any, _ABackendConnection] = {}
		self._connections_guard = asyncio.Lock()

	# ------------------------------------------------------------------ соединения

	async def _aopen_connection(self) -> _ABackendConnection:
		reader, writer = await asyncio.wait_for(
			asyncio.open_connection(self._target_host, self._target_port, ssl=self._ssl_ctx),
			timeout=self._connect_timeout,
		)
		return _ABackendConnection(reader, writer)

	async def _aget_connection(self, client_addr: Any) -> _ABackendConnection:
		async with self._connections_guard:
			conn = self._connections.get(client_addr)
			if conn is not None and conn.alive:
				return conn
			conn = await self._aopen_connection()
			self._connections[client_addr] = conn
			return conn

	async def _adrop_connection(self, client_addr: Any, conn: _ABackendConnection) -> None:
		await conn.aclose()
		async with self._connections_guard:
			if self._connections.get(client_addr) is conn:
				del self._connections[client_addr]

	async def aclose_all(self) -> None:
		"""Закрыть все keep-alive соединения (например, при остановке сервера)."""
		async with self._connections_guard:
			conns = list(self._connections.values())
			self._connections.clear()
		for conn in conns:
			await conn.aclose()

	# ------------------------------------------------------------------ HTTP-обмен с backend'ом

	async def _awrite_request(self, conn: _ABackendConnection, request: ARequestInfo, body: bytes) -> None:
		skip = {"content-length", "connection", "host"}
		lines = [f"{request.method} {request.path} HTTP/1.1"]
		for name, value in request.headers:
			if name.lower() in skip:
				continue
			lines.append(f"{name}: {value}")
		lines.append(f"Host: {self._target_host}")
		lines.append(f"Content-Length: {len(body)}")
		lines.append("Connection: keep-alive")
		head = ("\r\n".join(lines) + "\r\n\r\n").encode("iso-8859-1")
		try:
			conn.writer.write(head + body)
			await conn.writer.drain()
		except (ConnectionError, OSError) as exc:
			raise NetworkError(f"не удалось отправить запрос в backend: {exc}") from exc

	async def _asend_and_receive_headers(
		self, conn: _ABackendConnection, request: ARequestInfo, body: bytes
	) -> tuple[AResponseInfo, AsyncIterator[bytes], bool]:
		await self._awrite_request(conn, request, body)

		header_block = await aread_headers_block(conn.reader)
		start_line, headers = parse_headers_block(header_block)
		parts = start_line.split(" ", 2)
		if len(parts) < 2:
			raise HttpProtocolError(f"некорректная статус-строка backend'а: {start_line!r}")
		status = int(parts[1])
		reason = parts[2] if len(parts) > 2 else ""

		definite_length = body_has_definite_length(headers)
		byte_source = afread_body(conn.reader, headers, allow_until_close=not definite_length)

		content_type = get_header(headers, "Content-Type", "")
		if "text/event-stream" in content_type.lower():
			byte_source = afread_sse_events(byte_source)

		conn.last_used = time.monotonic()
		response_info = AResponseInfo(status=status, reason=reason, headers=headers)
		return response_info, byte_source, definite_length

	@staticmethod
	def _should_keep_alive(response_info: AResponseInfo) -> bool:
		connection_header = get_header(response_info.headers, "Connection", "")
		return "close" not in connection_header.lower()

	# ------------------------------------------------------------------ Handler

	async def ahandle(
		self,
		request: ARequestInfo,
		client_addr: Any,
		afread_request: AFReadChunk,
		aresponse_start: AResponseStart,
	) -> None:
		# стриминг поддержан только в ответе — тело запроса вычитываем целиком
		body = b"".join([chunk async for chunk in afread_request()])

		conn = await self._aget_connection(client_addr)
		await conn.lock.acquire()
		lock_released = False
		try:
			try:
				response_info, body_iter, definite_length = await self._asend_and_receive_headers(
					conn, request, body
				)
			except NetworkError:
				# backend прозрачно закрыл протухшее keep-alive соединение — переподключаемся один раз
				await self._adrop_connection(client_addr, conn)
				conn.lock.release()
				lock_released = True
				conn = await self._aget_connection(client_addr)
				await conn.lock.acquire()
				lock_released = False
				response_info, body_iter, definite_length = await self._asend_and_receive_headers(
					conn, request, body
				)

			keep_alive_ok = [definite_length and self._should_keep_alive(response_info)]

			async def afread_response() -> AsyncIterator[bytes]:
				try:
					async for chunk in body_iter:
						yield chunk
				except (NetworkError, HttpProtocolError):
					keep_alive_ok[0] = False
					raise

			await aresponse_start(response_info, afread_response)

			if not keep_alive_ok[0]:
				await self._adrop_connection(client_addr, conn)
		finally:
			if not lock_released:
				conn.lock.release()
