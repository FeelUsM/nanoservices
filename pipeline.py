from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Awaitable, Callable

import json
import os
import time
from datetime import datetime
from pathlib import Path

import yaml
from yaml.emitter import Emitter as _YamlEmitter
from yaml.emitter import ScalarAnalysis as _ScalarAnalysis
from yaml.representer import SafeRepresenter as _YamlSafeRepresenter
from yaml.resolver import Resolver as _YamlResolver
from yaml.serializer import Serializer as _YamlSerializer

from .http_common import get_header

# Соглашение об именовании в проекте:
#   async-функции/методы -> начинаются с "a"  (ahandle, aclose, aserve_forever)
#   async-генераторы     -> начинаются с "af" (afread_response, afread_body)
# Обычные типы, dataclass'ы и синхронные функции этих префиксов НЕ носят.

AFReadChunk = Callable[[], AsyncIterator[bytes]]


class LogError(Exception):
	"""Ошибка самого файлового логгера (диск/права/путь).

	Внутренняя ошибка конвейера: сервер маппит её в 500, а не в 502 —
	вины backend'а здесь нет. В консоль такие ошибки подсвечиваются ярко.
	"""


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
	Элемент конвейера: пишет запрос и ответ в файл, данных не меняет.

	На каждый запрос в dir создаётся файл time-host-port.txt. Секции в файле
	отделяются строками '==== случайная_строка_10_байт сообщение ====':
	пустое сообщение — запрос (стартовая строка, заголовки, пустая строка, тело),
	'RESPOND' — ответ (статус-строка, заголовки, пустая строка, тело по чанкам).
	Пауза между чанками >= 1с помечается разделителем с обеими дельтами.
	Если файлов .txt в dir больше max_files — самые старые удаляются.
	"""

	_STREAM_GAP_SEC = 1.0
	_JSON_RAW_LIMIT = 200
	_JSON_PRETTY_LIMIT = 500
	_LOG_WIDTH = 200

	def __init__(
		self,
		backend: Handler,
		dir: str | Path,
		max_files: int,
		*,
		log: Callable[[str], None] = print,
	) -> None:
		self._next = backend
		self._dir = Path(dir)
		self._max_files = max_files
		self._log = log
		try:
			self._dir.mkdir(parents=True, exist_ok=True)
		except OSError as exc:
			self._log(f"StreamLogger: \x1b[31;1m!!! НЕ МОГУ СОЗДАТЬ ПАПКУ ЛОГОВ {self._dir}: {exc!r}\x1b[0m")
			raise LogError(f"не удалось создать папку логов {self._dir}: {exc}") from exc

	def _alog_bright(self, message: str) -> None:
		# Яркая подсветка в консоли: имя класса — в самом начале строки
		# (до ANSI-кодов), чтобы работал и startswith, и визуальный поиск.
		self._log(f"StreamLogger: \x1b[31;1m{message}\x1b[0m")

	def _new_log_path(self, date_part: str, host: str, port: str) -> Path:
		try:
			base = f"{date_part}-{host}-{port}"
			path = self._dir / f"{base}.txt"
			n = 0
			while path.exists():
				n += 1
				path = self._dir / f"{base}-{n}.txt"
			path.touch()
		except OSError as exc:
			self._alog_bright(f"!!! НЕ МОГУ СОЗДАТЬ ФАЙЛ ЛОГА в {self._dir}: {exc!r}")
			raise LogError(f"не удалось создать файл лога в {self._dir}: {exc}") from exc
		self._rotate(keep=path)
		return path

	def _rotate(self, keep: Path) -> None:
		# только что созданный файл не удаляем: при равном mtime сортировка
		# по имени снесла бы его (суффикс -N sorts before '.txt'), а следующая
		# запись его бы воскресила
		try:
			files = [p for p in self._dir.glob("*.txt") if p.is_file() and p != keep]
			excess = len(files) + 1 - self._max_files
			if excess <= 0:
				return
			aged = sorted((p.stat().st_mtime_ns, p.name, p) for p in files)
			for _, _, old in aged[:excess]:
				try:
					old.unlink()
				except OSError:
					pass
		except OSError:
			pass

	async def ahandle(self, request: RequestInfo, body: bytes) -> tuple[ResponseInfo, AFReadChunk]:
		stamp = datetime.fromtimestamp(time.time())
		msec = f"{stamp.microsecond // 1000:03d}"
		host, port = _split_client_addr(request.client_addr)
		tag = f"{stamp.strftime('%H%M%S')}.{msec}-{host}-{port}"
		path = self._new_log_path(f"{stamp.strftime('%y%m%d-%H%M%S')}.{msec}", host, port)

		def emit(text: str) -> None:
			# файл не держим открытым: генератор могут так и не проитерировать,
			# а синхронная запись безопасна и на пути GeneratorExit.
			# Ошибки записи ярко подсвечиваем в консоли и заворачиваем в LogError,
			# чтобы сервер вернул 500 (вина диска, а не backend'а).
			try:
				with open(path, "a", encoding="utf-8") as fh:
					fh.write(text)
			except OSError as exc:
				self._alog_bright(f"!!! ОШИБКА ЗАПИСИ ЛОГА {path}: {exc!r}")
				raise LogError(f"не удалось записать лог {path}: {exc}") from exc

		def sep(message: str) -> None:
			emit(f"==== {os.urandom(10).hex()} {message} ====\n")

		def safe_sep(message: str) -> None:
			# sep в обработчике чужой ошибки: если умер и сам лог, исходная
			# ошибка важнее — ярко уже подсветили внутри emit, не маскируем.
			try:
				sep(message)
			except LogError:
				pass

		req_json = _is_json_content(request.headers, "Content-Type")
		sep("")
		emit(f"{request.method} {request.path} {request.http_version}\n")
		for name, value in request.headers:
			emit(f"{name}: {value}\n")
		emit(f"\n{_format_logged_data(body, is_json=req_json)}\n")
		self._log(f"StreamLogger: [{tag}] -> {request.method} {request.path} ({len(body)} байт тела)")

		try:
			response, afread_response = await self._next.ahandle(request, body)
		except Exception as exc:
			safe_sep(f"ERROR {exc!r}")
			self._log(f"StreamLogger: [{tag}] !! ошибка конвейера: {exc!r}")
			raise

		resp_json = _is_json_content(response.headers, "Content-Type") or _is_json_content(
			request.headers, "Accept"
		)
		sep("RESPOND")
		reason = f" {response.reason}" if response.reason else ""
		emit(f"HTTP/1.1 {response.status}{reason}\n")
		for name, value in response.headers:
			emit(f"{name}: {value}\n")
		emit("\n")

		async def afread_response_logged() -> AsyncIterator[bytes]:
			total = 0
			count = 0
			first_ts: float | None = None
			prev_ts = 0.0
			inner = afread_response()
			try:
				async for chunk in inner:
					arrived = time.monotonic()
					if first_ts is None:
						first_ts = arrived
					elif arrived - prev_ts >= StreamLogger._STREAM_GAP_SEC:
						sep(f"+{arrived - prev_ts:.3f}s / +{arrived - first_ts:.3f}s")
					prev_ts = arrived
					total += len(chunk)
					count += 1
					emit(f"{_format_logged_data(chunk, is_json=resp_json)}\n")
					yield chunk
			except GeneratorExit:
				safe_sep("ABORT")
				raise
			except Exception as exc:
				safe_sep(f"ERROR {exc!r}")
				self._log(f"StreamLogger: [{tag}] !! ошибка в теле ответа после {total} байт: {exc!r}")
				raise
			else:
				safe_sep("END")
			finally:
				# детерминированно закрываем внутренний генератор: await здесь легален
				# (запрещён только yield на пути GeneratorExit), брошенный внутрь
				# GeneratorExit заставит источник освободить свои ресурсы сразу,
				# а не через финализатор цикла
				await inner.aclose()
				self._log(f"StreamLogger: [{tag}] <- {response.status} {response.reason} ({total} байт в {count} частях)")

		return response, afread_response_logged


def _split_client_addr(client_addr: Any) -> tuple[str, str]:
	"""peername -> (host, port) strings, пригодные для имени файла."""
	if isinstance(client_addr, (tuple, list)) and len(client_addr) >= 2:
		host, port = str(client_addr[0]), str(client_addr[1])
	else:
		host, port = str(client_addr), "?"
	for bad in ("/", "\\", ":", "\x00"):
		host = host.replace(bad, "_")
	return host, port


def _is_json_content(headers: list[tuple[str, str]], name: str) -> bool:
	return "application/json" in (get_header(headers, name, "") or "").lower()


class _LiteralEmitter(_YamlEmitter):
	"""Как Emitter, но разрешает '|' и для строк с пробелами перед переносом.

	PyYAML консервативно запрещает блочный стиль при space_break/trailing_space,
	хотя literal-блок такие пробелы сохраняет точно (проверяется round-trip).
	Без этого пример из ТЗ ('asdf   \\n    sdfg') печатался бы в кавычках.
	"""

	def analyze_scalar(self, scalar: str) -> _ScalarAnalysis:
		analysis = super().analyze_scalar(scalar)
		if analysis.multiline:
			analysis.allow_block = True
		return analysis


class _LiteralDumper(_LiteralEmitter, _YamlSerializer, _YamlSafeRepresenter, _YamlResolver):
	"""SafeDumper, печатающий многострочные строки блоком '|' (как в ТЗ)."""

	def __init__(self, stream: Any, default_style: Any = None, default_flow_style: bool = False,
		canonical: Any = None, indent: Any = None, width: Any = None,
		allow_unicode: Any = None, line_break: Any = None, encoding: Any = None,
		explicit_start: Any = None, explicit_end: Any = None, version: Any = None,
		tags: Any = None, sort_keys: bool = True) -> None:
		_LiteralEmitter.__init__(self, stream, canonical=canonical, indent=indent,
			width=width, allow_unicode=allow_unicode, line_break=line_break)
		_YamlSerializer.__init__(self, encoding=encoding, explicit_start=explicit_start,
			explicit_end=explicit_end, version=version, tags=tags)
		_YamlSafeRepresenter.__init__(self, default_style=default_style,
			default_flow_style=default_flow_style, sort_keys=sort_keys)
		_YamlResolver.__init__(self)


def _represent_literal_str(dumper: _YamlSafeRepresenter, value: str) -> yaml.ScalarNode:
	style = "|" if "\n" in value else None
	return dumper.represent_scalar("tag:yaml.org,2002:str", value, style=style)


_LiteralDumper.add_representer(str, _represent_literal_str)


def _pack_json_lines(pretty: str, width: int = 200) -> str:
	"""Склеивает строки pretty-JSON, пока влезают в width (разрывы только между строк)."""
	out: list[str] = []
	buf = ""
	for raw in pretty.split("\n"):
		piece = raw.strip()
		if not piece:
			continue
		if not buf:
			buf = piece
		elif len(buf) + 1 + len(piece) <= width:
			buf += " " + piece
		else:
			out.append(buf)
			buf = piece
	if buf:
		out.append(buf)
	return "\n".join(out)


def _format_logged_data(data: bytes, *, is_json: bool) -> str:
	"""Спецформат тела: <200 символов — как есть, <500 — компактный JSON, иначе YAML."""
	text = data.decode("utf-8", errors="replace")
	if not is_json or len(text) < StreamLogger._JSON_RAW_LIMIT:
		return text
	try:
		parsed = json.loads(text)
	except ValueError:
		return text
	if len(text) < StreamLogger._JSON_PRETTY_LIMIT:
		return _pack_json_lines(
			json.dumps(parsed, ensure_ascii=False, indent=1), StreamLogger._LOG_WIDTH
		)
	try:
		rendered = yaml.dump(
			parsed,
			Dumper=_LiteralDumper,
			allow_unicode=True,
			default_flow_style=False,
			sort_keys=False,
			width=StreamLogger._LOG_WIDTH,
		).rstrip("\n")
	except Exception:
		# логгер никогда не роняет запрос из-за форматирования — отдаём как есть
		return text
	return "YAML\n" + rendered
