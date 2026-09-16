from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Awaitable, Callable

# Соглашение об именовании в проекте:
#   async-функции/методы -> начинаются с "a"  (ahandle, aclose, aserve_forever)
#   async-генераторы     -> начинаются с "af" (afread_response, afread_body)
# Обычные типы, dataclass'ы и синхронные функции этих префиксов НЕ носят.

AFReadChunk = Callable[[], AsyncIterator[bytes]]


@dataclass
class RequestInfo:
	method: str
	path: str
	http_version: str
	headers: list[tuple[str, str]]
	client_addr: Any = None


@dataclass
class ResponseInfo:
	status: int
	reason: str = ""
	headers: list[tuple[str, str]] = field(default_factory=list)


class Handler:
	"""
	Элемент конвейера "вопрос-ответ".

	Основной (простой) контракт — тело запроса приходит целиком, ответ отдаётся стримом:

		resp_head, afresp_gen = await handler.ahandle(req_head, req_body)
		# отсылаем resp_head
		async for chunk in afresp_gen():
			# отсылаем chunk

	Возвращённый генератор обязан быть проитерирован до конца (или закрыт через aclose()):
	за ним могут стоять захваченные ресурсы — соединение с backend'ом, лок, файл.

	Архитектура симметрична: источник данных — async-генератор, потребитель делает async for.
	Ошибка в источнике  -> генератор кидает исключение, async for его ловит.
	Ошибка в приёмнике  -> async for обрывается, генератор получает GeneratorExit и подчищает за собой.

	Для случая, когда стримить нужно и запрос тоже (клиент ещё шлёт тело, а сервер уже начал
	отвечать), предусмотрен отдельный дуплексный контракт — ahandle_duplex, см. ниже.
	"""

	async def ahandle(self, request: RequestInfo, body: bytes) -> tuple[ResponseInfo, AFReadChunk]:
		raise NotImplementedError

	async def ahandle_duplex(
		self,
		request: RequestInfo,
		afread_request: AFReadChunk,
		aresponse_start: "AResponseStart",
	) -> None:
		"""
		Дуплексный контракт — на будущее, когда понадобится стриминг запроса.
		Ответ здесь начинается отдельной задачей, параллельно с чтением тела запроса:

			pump_task = asyncio.create_task(aresponse_start(resp_head, self.afread_respond))
			async for chunk in afread_request():
				...
			await pump_task  # дожидаемся, что все части ответа отправлены

		Пока не используется — реализация по умолчанию отсутствует.
		"""
		raise NotImplementedError("дуплексный режим пока не реализован")


AResponseStart = Callable[[ResponseInfo, AFReadChunk], Awaitable[None]]


class StreamLogger(Handler):
	"""
	Пример элемента конвейера: прозрачно логирует запрос и ответ, данных не меняет.
	Показывает, как оборачивать возвращаемый генератор, не ломая стриминг.
	"""

	def __init__(self, next_handler: Handler, *, log: Callable[[str], None] = print) -> None:
		self._next = next_handler
		self._log = log

	async def ahandle(self, request: RequestInfo, body: bytes) -> tuple[ResponseInfo, AFReadChunk]:
		self._log(f"[{request.client_addr}] -> {request.method} {request.path} ({len(body)} байт тела)")

		response, afread_response = await self._next.ahandle(request, body)
		self._log(f"[{request.client_addr}] <- {response.status} {response.reason}")

		async def afread_response_logged() -> AsyncIterator[bytes]:
			total = 0
			count = 0
			try:
				async for chunk in afread_response():
					total += len(chunk)
					count += 1
					yield chunk
			except Exception as exc:
				self._log(f"[{request.client_addr}] !! ошибка в теле ответа после {total} байт: {exc!r}")
				raise
			finally:
				self._log(f"[{request.client_addr}] тело ответа: {total} байт в {count} частях")

		return response, afread_response_logged
