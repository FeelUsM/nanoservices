from __future__ import annotations

from dataclasses import dataclass
from typing import Any, AsyncIterator, Awaitable, Callable

# Соглашение об именовании в проекте:
#   async-методы/функции   -> начинаются с "a"  (например: ahandle, aresponse_start)
#   async-генераторы       -> начинаются с "af" (например: afread_request)
# Это сделано специально, чтобы не забывать про await/async for.

AFReadChunk = Callable[[], AsyncIterator[bytes]]


@dataclass
class ARequestInfo:
	method: str
	path: str
	http_version: str
	headers: list[tuple[str, str]]


@dataclass
class AResponseInfo:
	status: int
	reason: str
	headers: list[tuple[str, str]]


AResponseStart = Callable[[AResponseInfo, AFReadChunk], Awaitable[None]]


class Handler:
	"""
	Элемент конвейера "вопрос-ответ".

	Архитектура симметрична: источник данных — генератор, потребитель делает async for.
	Ошибка источника -> генератор кидает исключение, async for его ловит.
	Ошибка потребителя -> async for кидает исключение внутрь генератора (GeneratorExit / что угодно
	через agen.athrow), генератор может это поймать и подчистить ресурсы.

	Если стриминг ответа должен идти параллельно со стримингом запроса — вызывающая сторона
	оборачивает aresponse_start(...) в отдельную asyncio.Task и дожидается её после чтения запроса:

		pump_task = asyncio.create_task(aresponse_start(response, self.afread_respond))
		async for chunk in afread_request():
			...
		await pump_task

	Но в большинстве случаев (нет стриминга в запросе) это не нужно — можно сначала дочитать
	запрос целиком, а затем один раз вызвать aresponse_start.
	"""

	async def ahandle(
		self,
		request: ARequestInfo,
		client_addr: Any,
		afread_request: AFReadChunk,
		aresponse_start: AResponseStart,
	) -> None:
		raise NotImplementedError


class StreamLogger(Handler):
	"""
	Пример элемента конвейера: прозрачно логирует запрос/ответ и передаёт управление дальше.
	Показывает, как оборачивать afread_request/aresponse_start, не трогая сами данные.
	"""

	def __init__(self, next_handler: Handler, *, log: Callable[[str], None] = print) -> None:
		self._next = next_handler
		self._log = log

	async def ahandle(
		self,
		request: ARequestInfo,
		client_addr: Any,
		afread_request: AFReadChunk,
		aresponse_start: AResponseStart,
	) -> None:
		self._log(f"[{client_addr}] -> {request.method} {request.path}")

		async def afread_request_logged() -> AsyncIterator[bytes]:
			total = 0
			async for chunk in afread_request():
				total += len(chunk)
				yield chunk
			if total:
				self._log(f"[{client_addr}] тело запроса: {total} байт")

		async def aresponse_start_logged(response: AResponseInfo, afread_response: AFReadChunk) -> None:
			self._log(f"[{client_addr}] <- {response.status} {response.reason}")

			async def afread_response_logged() -> AsyncIterator[bytes]:
				total = 0
				async for chunk in afread_response():
					total += len(chunk)
					yield chunk
				self._log(f"[{client_addr}] тело ответа: {total} байт")

			await aresponse_start(response, afread_response_logged)

		await self._next.ahandle(request, client_addr, afread_request_logged, aresponse_start_logged)
