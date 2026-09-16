from __future__ import annotations

import asyncio
from typing import AsyncIterator, Optional


class HttpProtocolError(Exception):
	"""Ошибка на уровне HTTP-протокола (собеседник прислал некорректные данные)."""


class NetworkError(Exception):
	"""Ошибка сети (разрыв соединения, таймаут и т.п.) — обычно означает, что нужно переподключиться."""


MAX_HEADERS_SIZE = 64 * 1024


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
	value = get_header(headers, "Transfer-Encoding", "")
	return "chunked" in value.lower()


async def afread_content_length_body(reader: asyncio.StreamReader, length: int) -> AsyncIterator[bytes]:
	remaining = length
	while remaining > 0:
		chunk = await reader.read(min(65536, remaining))
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
		size_str = size_line.strip().split(b";")[0]
		try:
			size = int(size_str, 16)
		except ValueError as exc:
			raise HttpProtocolError(f"некорректный размер chunk: {size_line!r}") from exc
		if size == 0:
			try:
				await reader.readuntil(b"\r\n\r\n")
			except asyncio.IncompleteReadError:
				pass
			return
		try:
			data = await reader.readexactly(size)
			await reader.readexactly(2)  # завершающий CRLF после данных chunk'а
		except asyncio.IncompleteReadError as exc:
			raise NetworkError("соединение закрылось посреди chunked-тела") from exc
		yield data


async def afread_until_close(reader: asyncio.StreamReader) -> AsyncIterator[bytes]:
	while True:
		chunk = await reader.read(65536)
		if not chunk:
			return
		yield chunk


def body_has_definite_length(headers: list[tuple[str, str]]) -> bool:
	"""Есть ли способ понять конец тела без закрытия соединения (chunked или Content-Length)."""
	if has_chunked_encoding(headers):
		return True
	return get_header(headers, "Content-Length") is not None


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
	return  # тела нет


async def afread_sse_events(byte_chunks: AsyncIterator[bytes]) -> AsyncIterator[bytes]:
	"""
	Парсит Server-Sent Events поверх произвольного источника байт-чанков.
	Событие отделяется пустой строкой. Поток завершается на 'data: [DONE]'.
	"""
	buffer = b""
	async for chunk in byte_chunks:
		buffer += chunk
		while b"\n\n" in buffer:
			event, buffer = buffer.split(b"\n\n", 1)
			event = event.strip(b"\r\n")
			if not event:
				continue
			yield event + b"\n\n"
			if event.strip() in (b"data: [DONE]", b"data:[DONE]"):
				return
	if buffer.strip():
		yield buffer
