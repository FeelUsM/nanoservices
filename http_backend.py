from __future__ import annotations

import asyncio
import ssl
import time
from typing import Any, AsyncIterator, Optional
from urllib.parse import urlsplit

from .http_common import (
	HttpProtocolError,
	NetworkError,
	afdecode_sse,
	afread_body,
	aread_headers_block,
	body_has_definite_length,
	get_header,
	is_event_stream,
	parse_headers_block,
)
from .pipeline import AFReadChunk, Handler, RequestInfo, ResponseInfo


class _BackendConnection:
	"""Одно keep-alive TCP/TLS-соединение до backend'а плюс лок на его использование."""

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


def parse_target_url(url: str) -> tuple[str, int, bool, str, str, int]:
	"""URL backend'а -> (host, port, use_ssl, base_path, base_query, default_port)."""
	parts = urlsplit(url)
	if parts.scheme not in ("http", "https"):
		raise ValueError(f"поддерживаются только схемы http/https: {url!r}")
	if not parts.hostname:
		raise ValueError(f"в URL нет хоста: {url!r}")
	use_ssl = parts.scheme == "https"
	default_port = 443 if use_ssl else 80
	return (
		parts.hostname,
		parts.port or default_port,
		use_ssl,
		parts.path.rstrip("/") if parts.path else "",
		parts.query,
		default_port,
	)


class HttpBackend(Handler):
	"""
	Handler, форвардящий запрос в реальный HTTP backend.

 	Создание экземпляра соединений не открывает — они поднимаются лениво, при первом запросе
 	от конкретного client_addr. На каждого клиента фронтенда держится одно keep-alive
 	соединение до backend'а; если backend незаметно закрыл протухшее соединение, HttpBackend
 	один раз прозрачно переподключается и повторяет запрос.
	Адрес backend'а задаётся URL: схема http/https, хост, порт (по умолчанию 80/443)
	и необязательный префикс пути, который приклеивается перед путём запроса.
 
 	Стриминг поддержан на стороне ответа: заголовки возвращаются сразу, тело отдаётся
 	генератором по мере поступления. Если backend ответил Content-Type: text/event-stream,
 	тело разбирается как SSE и наружу идут ЧИСТЫЕ полезные нагрузки сообщений (без 'data:',
 	без терминатора '[DONE]') — обвязку обратно навесит тот, кто отдаёт ответ клиенту.
 
 	Возвращённый генератор тела обязан быть проитерирован до конца или закрыт: пока он живёт,
	за ним держатся лок и соединение с backend'ом.
	"""

	def __init__(
		self,
		url: str,
		*,
		ssl_ctx: Optional[ssl.SSLContext] = None,
		connect_timeout: float = 10.0,
	) -> None:
		host, port, use_ssl, base_path, base_query, default_port = parse_target_url(url)
		if ssl_ctx is None and use_ssl:
			ssl_ctx = ssl.create_default_context()
		self._target_host = host
		self._target_port = port
		self._ssl_ctx: Optional[ssl.SSLContext] = ssl_ctx if use_ssl else None
		self._base_path = base_path
		self._base_query = base_query
		self._host_header = host if port == default_port else f"{host}:{port}"
		self._connect_timeout = connect_timeout
		self._connections: dict[Any, _BackendConnection] = {}
		self._connections_guard = asyncio.Lock()

	def _target_path(self, request_path: str) -> str:
		req_path, _, req_query = request_path.partition("?")
		full = f"{self._base_path}{req_path}"
		query = req_query
		if self._base_query:
			query = f"{self._base_query}&{req_query}" if req_query else self._base_query
		return f"{full}?{query}" if query else full

	# ------------------------------------------------------------------ соединения

	async def _aopen_connection(self) -> _BackendConnection:
		reader, writer = await asyncio.wait_for(
			asyncio.open_connection(self._target_host, self._target_port, ssl=self._ssl_ctx),
			timeout=self._connect_timeout,
		)
		return _BackendConnection(reader, writer)

	async def _aget_connection(self, client_addr: Any) -> _BackendConnection:
		async with self._connections_guard:
			conn = self._connections.get(client_addr)
			if conn is not None and conn.alive:
				return conn
			conn = await self._aopen_connection()
			self._connections[client_addr] = conn
			return conn

	async def _adrop_connection(self, client_addr: Any, conn: _BackendConnection) -> None:
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

	# ------------------------------------------------------------------ обмен с backend'ом

	async def _awrite_request(self, conn: _BackendConnection, request: RequestInfo, body: bytes) -> None:
		skip = {"content-length", "connection", "host", "transfer-encoding"}
		lines = [f"{request.method} {self._target_path(request.path)} HTTP/1.1"]
		for name, value in request.headers:
			if name.lower() not in skip:
				lines.append(f"{name}: {value}")
		lines.append(f"Host: {self._host_header}")
		lines.append(f"Content-Length: {len(body)}")
		lines.append("Connection: keep-alive")
		head = ("\r\n".join(lines) + "\r\n\r\n").encode("iso-8859-1")
		try:
			conn.writer.write(head + body)
			await conn.writer.drain()
		except (ConnectionError, OSError) as exc:
			raise NetworkError(f"не удалось отправить запрос в backend: {exc}") from exc

	async def _aexchange(
		self, conn: _BackendConnection, request: RequestInfo, body: bytes
	) -> tuple[ResponseInfo, AsyncIterator[bytes], bool]:
		"""Отправляет запрос и читает заголовки ответа. Возвращает (заголовки, источник тела, keep-alive?)."""
		await self._awrite_request(conn, request, body)

		start_line, headers = parse_headers_block(await aread_headers_block(conn.reader))
		parts = start_line.split(" ", 2)
		if len(parts) < 2:
			raise HttpProtocolError(f"некорректная статус-строка backend'а: {start_line!r}")
		try:
			status = int(parts[1])
		except ValueError as exc:
			raise HttpProtocolError(f"некорректный код ответа backend'а: {start_line!r}") from exc
		reason = parts[2] if len(parts) > 2 else ""

		definite_length = body_has_definite_length(headers)
		byte_source = afread_body(conn.reader, headers, allow_until_close=not definite_length)
		if is_event_stream(headers):
			byte_source = afdecode_sse(byte_source)

		conn.last_used = time.monotonic()
		connection_header = (get_header(headers, "Connection", "") or "").lower()
		reusable = definite_length and "close" not in connection_header
		return ResponseInfo(status=status, reason=reason, headers=headers), byte_source, reusable

	# ------------------------------------------------------------------ Handler

	async def ahandle(self, request: RequestInfo, body: bytes) -> tuple[ResponseInfo, AFReadChunk]:
		client_addr = request.client_addr
		conn = await self._aget_connection(client_addr)
		await conn.lock.acquire()
		try:
			try:
				response, byte_source, reusable = await self._aexchange(conn, request, body)
			except NetworkError:
				# backend тихо закрыл протухшее keep-alive соединение — переподключаемся один раз
				await self._adrop_connection(client_addr, conn)
				conn.lock.release()
				conn = await self._aget_connection(client_addr)
				await conn.lock.acquire()
				response, byte_source, reusable = await self._aexchange(conn, request, body)
		except BaseException:
			conn.lock.release()
			raise

		held_conn = conn
		state = {"reusable": reusable, "released": False}

		def release(*, drop: bool) -> None:
			# вызывается в том числе из finally на пути GeneratorExit, поэтому строго синхронно:
			# await внутри GeneratorExit запрещён ("async generator ignored GeneratorExit"),
			# а фактическое закрытие сокета уводим в отдельную задачу
			if state["released"]:
				return
			state["released"] = True
			if drop:
				asyncio.create_task(self._adrop_connection(client_addr, held_conn))
			held_conn.lock.release()

		async def afread_response() -> AsyncIterator[bytes]:
			try:
				async for chunk in byte_source:
					yield chunk
			except (NetworkError, HttpProtocolError):
				state["reusable"] = False
				raise
			except GeneratorExit:
				# потребитель оборвал чтение на середине — соединение уже не в консистентном состоянии
				state["reusable"] = False
				raise
			finally:
				aclose = getattr(byte_source, "aclose", None)
				if aclose is not None:
					await aclose()
				release(drop=not state["reusable"])

		return response, afread_response
