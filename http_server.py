from __future__ import annotations

import asyncio
import logging
from typing import AsyncIterator

from .http_common import (
	HttpProtocolError,
	NetworkError,
	afread_body,
	aread_headers_block,
	get_header,
	parse_headers_block,
)
from .pipeline import AFReadChunk, AResponseInfo, ARequestInfo, Handler

_LOG = logging.getLogger("async_proxy.http_server")

_REASON_PHRASES = {
	200: "OK",
	204: "No Content",
	400: "Bad Request",
	404: "Not Found",
	502: "Bad Gateway",
	504: "Gateway Timeout",
}


def _areason_phrase(status: int) -> str:
	return _REASON_PHRASES.get(status, "")


class HttpServer:
	"""
	Простой HTTP/1.1 фронтенд-сервер на asyncio.

	Поднятие экземпляра ничего не открывает — сокет слушается только после astart()/aserve_forever().
	К серверу одновременно может подключаться сколько угодно клиентов, каждое соединение
	обслуживается своей задачей и поддерживает keep-alive (несколько запросов подряд).
	Каждый принятый запрос отдаётся заданному Handler'у (конвейеру).
	"""

	def __init__(self, host: str, port: int, handler: Handler) -> None:
		self._host = host
		self._port = port
		self._handler = handler
		self._server: asyncio.base_events.Server | None = None

	async def astart(self) -> None:
		self._server = await asyncio.start_server(self._ahandle_connection, self._host, self._port)

	async def aserve_forever(self) -> None:
		if self._server is None:
			await self.astart()
		assert self._server is not None
		async with self._server:
			await self._server.serve_forever()

	async def aclose(self) -> None:
		if self._server is not None:
			self._server.close()
			await self._server.wait_closed()

	# ------------------------------------------------------------------ одно TCP-соединение клиента

	async def _ahandle_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
		client_addr = writer.get_extra_info("peername")
		try:
			while True:
				keep_going = await self._ahandle_one_request(reader, writer, client_addr)
				if not keep_going:
					return
		except NetworkError:
			# клиент оборвал соединение — это штатная ситуация, не ошибка
			pass
		except Exception:
			_LOG.exception("[%s] необработанная ошибка на соединении", client_addr)
		finally:
			writer.close()
			try:
				await writer.wait_closed()
			except Exception:
				pass

	async def _ahandle_one_request(
		self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, client_addr
	) -> bool:
		"""Возвращает True, если соединение можно использовать для следующего запроса (keep-alive)."""
		header_block = await aread_headers_block(reader)
		start_line, headers = parse_headers_block(header_block)
		parts = start_line.split(" ", 2)
		if len(parts) != 3:
			await self._awrite_error(writer, 400, keep_alive=False)
			return False

		method, path, http_version = parts
		request = ARequestInfo(method=method, path=path, http_version=http_version, headers=headers)

		connection_header = get_header(headers, "Connection", "").lower()
		if http_version == "HTTP/1.0":
			client_wants_keep_alive = connection_header == "keep-alive"
		else:
			client_wants_keep_alive = connection_header != "close"

		body_consumed = False

		async def afread_request() -> AsyncIterator[bytes]:
			nonlocal body_consumed
			async for chunk in afread_body(reader, headers, allow_until_close=False):
				yield chunk
			body_consumed = True

		response_started = False
		keep_alive_result = [client_wants_keep_alive]

		async def aresponse_start(response: AResponseInfo, afread_response: AFReadChunk) -> None:
			nonlocal response_started
			response_started = True
			await self._awrite_response(writer, response, afread_response, keep_alive_result[0])

		try:
			await self._handler.ahandle(request, client_addr, afread_request, aresponse_start)
		except NetworkError:
			if not response_started:
				await self._awrite_error(writer, 502, keep_alive=False)
			return False
		except HttpProtocolError:
			if not response_started:
				await self._awrite_error(writer, 502, keep_alive=False)
			return False

		# если handler не дочитал тело запроса (например, ответил ошибкой сразу) —
		# на keep-alive соединении это испортит следующий запрос, поэтому дочитываем сами
		if not body_consumed:
			async for _ in afread_request():
				pass

		return keep_alive_result[0] and response_started

	# ------------------------------------------------------------------ запись ответа клиенту

	async def _awrite_response(
		self,
		writer: asyncio.StreamWriter,
		response: AResponseInfo,
		afread_response: AFReadChunk,
		keep_alive: bool,
	) -> None:
		headers = [
			(name, value)
			for name, value in response.headers
			if name.lower() not in ("transfer-encoding", "connection")
		]
		content_length_header = get_header(response.headers, "Content-Length")
		use_chunked = content_length_header is None
		if use_chunked:
			headers.append(("Transfer-Encoding", "chunked"))
		headers.append(("Connection", "keep-alive" if keep_alive else "close"))

		reason = response.reason or _areason_phrase(response.status)
		status_line = f"HTTP/1.1 {response.status} {reason}".rstrip()
		header_text = "\r\n".join(f"{name}: {value}" for name, value in headers)
		try:
			writer.write(f"{status_line}\r\n{header_text}\r\n\r\n".encode("iso-8859-1"))
			await writer.drain()

			async for chunk in afread_response():
				if not chunk:
					continue
				if use_chunked:
					writer.write(f"{len(chunk):x}\r\n".encode("ascii") + chunk + b"\r\n")
				else:
					writer.write(chunk)
				await writer.drain()

			if use_chunked:
				writer.write(b"0\r\n\r\n")
				await writer.drain()
		except (ConnectionError, OSError) as exc:
			# клиент отвалился посреди стриминга ответа — генератор источника узнает об этом
			# через исключение из потребителя (async for бросит внутрь него при следующем шаге)
			raise NetworkError(f"клиент разорвал соединение во время ответа: {exc}") from exc

	async def _awrite_error(self, writer: asyncio.StreamWriter, status: int, *, keep_alive: bool) -> None:
		body = _areason_phrase(status).encode() or b""
		reason = _areason_phrase(status)
		headers = (
			f"HTTP/1.1 {status} {reason}\r\n"
			f"Content-Length: {len(body)}\r\n"
			f"Connection: {'keep-alive' if keep_alive else 'close'}\r\n\r\n"
		)
		try:
			writer.write(headers.encode("iso-8859-1") + body)
			await writer.drain()
		except (ConnectionError, OSError):
			pass
