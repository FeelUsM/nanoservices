from __future__ import annotations

import asyncio
from typing import AsyncIterator, Optional


class HttpProtocolError(Exception):
	"""Ошибка на уровне HTTP-протокола (собеседник прислал некорректные данные)."""


class NetworkError(Exception):
	"""Ошибка сети (разрыв соединения, таймаут) — обычно означает, что надо переподключиться."""


MAX_HEADERS_SIZE = 64 * 1024
SSE_DONE = b"[DONE]"


async def aread_headers_block(reader: asyncio.StreamReader) -> bytes:
	"""Читает байты до пустой строки \\r\\n\\r\\n: стартовая строка + заголовки."""
	try:
		data = await reader.readuntil(b"\r\n\r\n")
	except asyncio.IncompleteReadError as exc:
		if not exc.partial:
			raise NetworkError("соединение закрыто до получения заголовков") from exc
		raise NetworkError("соединение оборвалось посреди заголовков") from exc
	except asyncio.LimitOverrunError as exc:
		raise HttpProtocolError("заголовки превышают допустимый размер буфера") from exc
	except (ConnectionError, OSError) as exc:
		# обрыв/таймаут/сброс на уровне TCP/TLS — это сеть, а не протокол
		raise NetworkError(f"соединение оборвалось при чтении заголовков: {exc}") from exc
	if len(data) > MAX_HEADERS_SIZE:
		raise HttpProtocolError("заголовки превышают допустимый размер")
	return data


def parse_headers_block(block: bytes) -> tuple[str, list[tuple[str, str]]]:
	"""'СТАРТ-СТРОКА\\r\\nHeader: value\\r\\n...' -> (старт-строка, [(имя, значение), ...])."""
	lines = block.split(b"\r\n")
	start_line = lines[0].decode("iso-8859-1")
	headers: list[tuple[str, str]] = []
	for line in lines[1:]:
		if not line:
			continue
		name, sep, value = line.partition(b":")
		if not sep:
			raise HttpProtocolError(f"некорректная строка заголовка: {line!r}")
		headers.append((name.decode("iso-8859-1").strip(), value.decode("iso-8859-1").strip()))
	return start_line, headers


def get_header(headers: list[tuple[str, str]], name: str, default: Optional[str] = None) -> Optional[str]:
	name_lower = name.lower()
	for key, value in headers:
		if key.lower() == name_lower:
			return value
	return default


def has_chunked_encoding(headers: list[tuple[str, str]]) -> bool:
	return "chunked" in (get_header(headers, "Transfer-Encoding", "") or "").lower()


def is_event_stream(headers: list[tuple[str, str]]) -> bool:
	"""Content-Type: text/event-stream — тело является потоком Server-Sent Events."""
	return "text/event-stream" in (get_header(headers, "Content-Type", "") or "").lower()


def body_has_definite_length(headers: list[tuple[str, str]]) -> bool:
	"""Можно ли понять конец тела, не дожидаясь закрытия соединения."""
	if has_chunked_encoding(headers):
		return True
	return get_header(headers, "Content-Length") is not None


# ---------------------------------------------------------------- чтение тела

async def afread_content_length_body(reader: asyncio.StreamReader, length: int) -> AsyncIterator[bytes]:
	remaining = length
	while remaining > 0:
		try:
			chunk = await reader.read(min(65536, remaining))
		except (ConnectionError, OSError) as exc:
			raise NetworkError(f"соединение оборвалось посреди тела: {exc}") from exc
		if not chunk:
			raise NetworkError("соединение закрылось раньше, чем пришло тело ожидаемой длины")
		remaining -= len(chunk)
		yield chunk


async def afread_chunked_body(reader: asyncio.StreamReader) -> AsyncIterator[bytes]:
	"""Парсит тело с Transfer-Encoding: chunked."""
	while True:
		try:
			size_line = await reader.readuntil(b"\r\n")
		except asyncio.IncompleteReadError as exc:
			raise NetworkError("соединение закрылось посреди chunked-тела") from exc
		except asyncio.LimitOverrunError as exc:
			raise HttpProtocolError(f"строка размера chunk превышает буфер: {exc}") from exc
		except (ConnectionError, OSError) as exc:
			raise NetworkError(f"соединение оборвалось посреди chunked-тела: {exc}") from exc
		size_str = size_line.strip().split(b";")[0]
		try:
			size = int(size_str, 16)
		except ValueError as exc:
			raise HttpProtocolError(f"некорректный размер chunk: {size_line!r}") from exc
		if size == 0:
			# last-chunk: дочитываем trailer-section до пустой строки
			try:
				while True:
					line = await reader.readuntil(b"\r\n")
					if line == b"\r\n":
						break
			except asyncio.IncompleteReadError as exc:
				raise NetworkError("соединение закрылось посреди chunked-тела") from exc
			except asyncio.LimitOverrunError as exc:
				raise HttpProtocolError(f"трейлер chunked-тела превышает буфер: {exc}") from exc
			except (ConnectionError, OSError) as exc:
				raise NetworkError(f"соединение оборвалось посреди chunked-тела: {exc}") from exc
			return
		try:
			data = await reader.readexactly(size)
			await reader.readexactly(2)  # завершающий CRLF после данных chunk'а
		except asyncio.IncompleteReadError as exc:
			raise NetworkError("соединение закрылось посреди chunked-тела") from exc
		except (ConnectionError, OSError) as exc:
			raise NetworkError(f"соединение оборвалось посреди chunked-тела: {exc}") from exc
		yield data


async def afread_until_close(reader: asyncio.StreamReader) -> AsyncIterator[bytes]:
	while True:
		try:
			chunk = await reader.read(65536)
		except (ConnectionError, OSError) as exc:
			raise NetworkError(f"соединение оборвалось при чтении до закрытия: {exc}") from exc
		if not chunk:
			return
		yield chunk


async def afread_body(
	reader: asyncio.StreamReader,
	headers: list[tuple[str, str]],
	*,
	allow_until_close: bool,
) -> AsyncIterator[bytes]:
	"""Выбирает стратегию чтения тела по заголовкам собеседника."""
	if has_chunked_encoding(headers):
		async for chunk in afread_chunked_body(reader):
			yield chunk
		return
	content_length = get_header(headers, "Content-Length")
	if content_length is not None:
		try:
			length = int(content_length)
		except ValueError as exc:
			raise HttpProtocolError(f"некорректный Content-Length: {content_length!r}") from exc
		if length > 0:
			async for chunk in afread_content_length_body(reader, length):
				yield chunk
		return
	if allow_until_close:
		async for chunk in afread_until_close(reader):
			yield chunk
	return


# ---------------------------------------------------------------- SSE: разбор и сборка

async def afdecode_sse(byte_chunks: AsyncIterator[bytes]) -> AsyncIterator[bytes]:
	"""
	Разбирает поток Server-Sent Events и отдаёт ЧИСТЫЕ полезные нагрузки сообщений:
	префикс 'data:' снимается, многострочные data склеиваются через \\n,
	терминатор '[DONE]' поглощается и наружу НЕ выдаётся (поток на нём просто заканчивается).
	Концы строк CRLF/CR нормализуются в LF, т.ч. события детектятся по ходу стрима.

	Внутри конвейера сообщения ходят без SSE-обвязки — обратно её навешивает тот,
	кто отдаёт ответ клиенту (см. encode_sse_message / SSE_DONE).

	Классификация ошибок (методика): обрыв транспорта — NetworkError,
	нарушение формата SSE — HttpProtocolError.
	"""
	buffer = b""
	source = byte_chunks.__aiter__()
	try:
		while True:
			try:
				chunk = await source.__anext__()
			except StopAsyncIteration:
				break
			except (NetworkError, HttpProtocolError):
				raise
			except (ConnectionError, OSError) as exc:
				raise NetworkError(f"SSE-стрим оборвался: {exc}") from exc
			except Exception as exc:
				raise HttpProtocolError(f"некорректный SSE-стрим: {exc}") from exc
			# нормализуем концы строк до \n: backend'ы шлют и LF, и CRLF
			buffer += chunk.replace(b"\r\n", b"\n").replace(b"\r", b"\n")
			while b"\n\n" in buffer:
				raw_event, buffer = buffer.split(b"\n\n", 1)
				payload = _extract_sse_payload(raw_event)
				if payload is None:
					continue
				if payload.strip() == SSE_DONE:
					return
				yield payload
	finally:
		aclose = getattr(source, "aclose", None)
		if aclose is not None:
			await aclose()
	tail = _extract_sse_payload(buffer)
	if tail is not None and tail.strip() != SSE_DONE:
		yield tail


def _extract_sse_payload(raw_event: bytes) -> Optional[bytes]:
	"""Из блока SSE-события достаёт склеенные data-строки. None — если data в блоке нет."""
	data_lines: list[bytes] = []
	for line in raw_event.replace(b"\r\n", b"\n").split(b"\n"):
		line = line.strip()
		if not line or line.startswith(b":"):
			continue  # пустая строка или комментарий
		name, sep, value = line.partition(b":")
		if not sep:
			continue  # поле без значения (event, id и т.п. без двоеточия) — пропускаем
		if name.strip().lower() != b"data":
			continue  # event:, id:, retry: — для нашей задачи не нужны
		data_lines.append(value[1:] if value.startswith(b" ") else value)
	if not data_lines:
		return None
	return b"\n".join(data_lines)


def encode_sse_message(payload: bytes) -> bytes:
	"""Оборачивает чистую полезную нагрузку обратно в SSE-событие: 'data: ...\\n\\n'."""
	lines = payload.replace(b"\r\n", b"\n").split(b"\n")
	return b"".join(b"data: " + line + b"\n" for line in lines) + b"\n"


def encode_sse_done() -> bytes:
	"""Терминатор потока SSE."""
	return b"data: " + SSE_DONE + b"\n\n"
