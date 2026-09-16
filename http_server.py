from __future__ import annotations

import asyncio
import logging
from typing import Any, Optional

from .http_common import (
	HttpProtocolError,
	NetworkError,
	afread_body,
	aread_headers_block,
	encode_sse_done,
	encode_sse_message,
	get_header,
	is_event_stream,
	parse_headers_block,
)
from .pipeline import AFReadChunk, Handler, RequestInfo, ResponseInfo

_LOG = logging.getLogger("async_proxy.http_server")

_REASON_PHRASES = {
	200: "OK",
	204: "No Content",
	400: "Bad Request",
	404: "Not Found",
	500: "Internal Server Error",
	502: "Bad Gateway",
	504: "Gateway Timeout",
}


def reason_phrase(status: int) -> str:
	return _REASON_PHRASES.get(status, "")


class HttpServer:
	"""
	HTTP/1.1 фронтенд-сервер на asyncio со своим парсером.

	Создание экземпляра сокет не открывает — слушать начинаем в astart()/aserve_forever().
	Клиентов может быть сколько угодно одновременно: каждое соединение обслуживается своей
	задачей и поддерживает keep-alive (несколько запросов подряд).

	Каждый запрос вычитывается целиком и отдаётся Handler'у по простому контракту:

		resp_head, afresp_gen = await handler.ahandle(req_head, req_body)

	Тело ответа стримится клиенту по мере поступления. Если в заголовках ответа стоит
	Content-Type: text/event-stream, сервер считает, что от конвейера приходят ЧИСТЫЕ
	полезные нагрузки сообщений, и сам навешивает SSE-обвязку: каждый чанк оборачивается
	в 'data: ...\\n\\n', а в конце дописывается 'data: [DONE]\\n\\n'.
	"""

	def __init__(self, host: str, port: int, handler: Handler) -> None:
		self._host = host
		self._port = port
		self._handler = handler
		self._server: Optional[asyncio.AbstractServer] = None

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

	# ------------------------------------------------------------------ соединение клиента

	async def _ahandle_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
		client_addr = writer.get_extra_info("peername")
		try:
			while await self._ahandle_one_request(reader, writer, client_addr):
				pass
		except NetworkError:
			pass  # клиент оборвал соединение — штатная ситуация
		except Exception:
			_LOG.exception("[%s] необработанная ошибка на соединении", client_addr)
		finally:
			writer.close()
			try:
				await writer.wait_closed()
			except Exception:
				pass

	async def _ahandle_one_request(
		self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, client_addr: Any
	) -> bool:
		"""Возвращает True, если соединение можно переиспользовать под следующий запрос."""
		start_line, headers = parse_headers_block(await aread_headers_block(reader))
		parts = start_line.split(" ", 2)
		if len(parts) != 3:
			await self._awrite_error(writer, 400, keep_alive=False)
			return False

		method, path, http_version = parts
		request = RequestInfo(
			method=method,
			path=path,
			http_version=http_version,
			headers=headers,
			client_addr=client_addr,
		)

		connection_header = (get_header(headers, "Connection", "") or "").lower()
		if http_version == "HTTP/1.0":
			keep_alive = connection_header == "keep-alive"
		else:
			keep_alive = connection_header != "close"

		# стриминг запроса пока не поддержан — вычитываем тело целиком
		body = b"".join([chunk async for chunk in afread_body(reader, headers, allow_until_close=False)])

		try:
			response, afread_response = await self._handler.ahandle(request, body)
		except (NetworkError, HttpProtocolError) as exc:
			_LOG.warning("[%s] backend недоступен: %s", client_addr, exc)
			await self._awrite_error(writer, 502, keep_alive=False)
			return False
		except Exception:
			_LOG.exception("[%s] ошибка в конвейере", client_addr)
			await self._awrite_error(writer, 500, keep_alive=False)
			return False

		try:
			await self._awrite_response(writer, response, afread_response, keep_alive)
		except NetworkError:
			raise  # клиент отвалился — соединение закрывает вызывающий
		except (HttpProtocolError, OSError) as exc:
			_LOG.warning("[%s] ошибка при передаче тела ответа: %s", client_addr, exc)
			return False

		return keep_alive

	# ------------------------------------------------------------------ запись ответа

	async def _awrite_response(
		self,
		writer: asyncio.StreamWriter,
		response: ResponseInfo,
		afread_response: AFReadChunk,
		keep_alive: bool,
	) -> None:
		sse = is_event_stream(response.headers)
		content_length = get_header(response.headers, "Content-Length")
		use_chunked = sse or content_length is None

		headers = [
			(name, value)
			for name, value in response.headers
			if name.lower() not in ("transfer-encoding", "connection", "content-length")
		]
		if use_chunked:
			headers.append(("Transfer-Encoding", "chunked"))
			if sse:
				headers.append(("Cache-Control", "no-cache"))
		else:
			headers.append(("Content-Length", content_length))
		headers.append(("Connection", "keep-alive" if keep_alive else "close"))

		reason = response.reason or reason_phrase(response.status)
		status_line = f"HTTP/1.1 {response.status} {reason}".rstrip()
		header_text = "\r\n".join(f"{name}: {value}" for name, value in headers)

		body_gen = afread_response()
		try:
			await self._awrite_raw(writer, f"{status_line}\r\n{header_text}\r\n\r\n".encode("iso-8859-1"))
			async for chunk in body_gen:
				if not chunk:
					continue
				payload = encode_sse_message(chunk) if sse else chunk
				if use_chunked:
					payload = f"{len(payload):x}\r\n".encode("ascii") + payload + b"\r\n"
				await self._awrite_raw(writer, payload)
			if sse:
				done = encode_sse_done()
				await self._awrite_raw(writer, f"{len(done):x}\r\n".encode("ascii") + done + b"\r\n")
			if use_chunked:
				await self._awrite_raw(writer, b"0\r\n\r\n")
		finally:
			# если мы вывалились на середине (клиент отвалился), источник должен узнать об этом
			# и подчистить за собой: aclose() бросит внутрь генератора GeneratorExit
			await body_gen.aclose()

	async def _awrite_raw(self, writer: asyncio.StreamWriter, data: bytes) -> None:
		try:
			writer.write(data)
			await writer.drain()
		except (ConnectionError, OSError) as exc:
			raise NetworkError(f"клиент разорвал соединение: {exc}") from exc

	async def _awrite_error(self, writer: asyncio.StreamWriter, status: int, *, keep_alive: bool) -> None:
		reason = reason_phrase(status)
		body = reason.encode()
		head = (
			f"HTTP/1.1 {status} {reason}\r\n"
			f"Content-Type: text/plain\r\n"
			f"Content-Length: {len(body)}\r\n"
			f"Connection: {'keep-alive' if keep_alive else 'close'}\r\n\r\n"
		)
		try:
			writer.write(head.encode("iso-8859-1") + body)
			await writer.drain()
		except (ConnectionError, OSError):
			pass
