# AGENTS.md

## Структура
- Плоский пакет: `pipeline.py`, `http_common.py`, `http_backend.py`, `http_server.py`, `example_proxy.py`, `__init__.py`, `pyproject.toml`, `tests/`. Единственная зависимость вне stdlib — `pyyaml` (нужен для YAML-спецформата логгера). Линта/CI нет.
- Импорты относительные (`from .http_backend import ...`), поэтому запуск только как пакет из родителя: `python3 -m nanoservices.example_proxy --url http://127.0.0.1:8080 --dir log --host 0.0.0.0 --port 8000`.
- `prompts.md` — история проектирования, а не описание текущего API. Текущие сигнатуры смотреть в коде.

## Установка (`pip install -e .`)
- В `pyproject.toml` обязательны `[build-system]` + явный маппинг `[tool.setuptools] packages = ["nanoservices"]` с `[tool.setuptools.package-dir] nanoservices = "."`: корень репозитория сам является пакетом, без этого автообнаружение setuptools пакет не видит (в установку уезжает только мусор вроде `back/`, а `import nanoservices` падает — было, чинилось). После правок упаковки проверять из нейтральной папки: `python -c "import nanoservices.pipeline, nanoservices.http_backend, nanoservices.http_server"` и `python -m nanoservices.example_proxy --help`.

## Тесты
- Раннер — stdlib `unittest`, других зависимостей нет. Из корня репозитория: `python3 -m unittest discover -s tests -v` (в файлах есть bootstrap `sys.path`, снаружи пакета тоже работает).
- Методы тестов называются `test_*` — требование discovery, здесь исключение из правила префиксов `a`/`af`.
- Live-тесты (`test_proxy.py`, `test_server.py`) — только localhost, порты `0` (автовыбор). В teardown: сначала `await backend.aclose_all()`, потом `server.aclose()` (его `wait_closed` ждёт открытые соединения); клиентские writer'ы — через `addCleanup(writer.close)`, `aclose` — под `asyncio.wait_for`. Иначе висящий cleanup маскирует настоящую ошибку.
- Детекция висящих генераторов: предупреждение `Task was destroyed but it is pending (async_generator_athrow)` в выводе — признак брошенного подвешенного генератора, чинить закрытием, а не игнором.

## Стиль
- Отступы — табуляция.
- Префиксы: `a` — async-функции/методы (`ahandle`, `aclose`, `aserve_forever`), `af` — async-генераторы (`afread_response`, `afread_body`). Синхронные типы и функции префиксов не носят (`Handler`, `RequestInfo`, `parse_headers_block`). Неправильный префикс — ошибка нейминга, см. `pipeline.py:6-9` и `prompts.md:67`.

## Конструкторы
- `HttpBackend(url)`: схема http/https, порт по умолчанию 80/443, префикс пути приклеивается перед путём запроса (`_target_path`), query склеиваются. `https` без явного `ssl_ctx` → `ssl.create_default_context()`. Парсер — `parse_target_url`.
- `StreamLogger(backend, dir, max_files)`: файловый логгер (см. ниже). Четвёртый keyword-only `log=print` — только для подмены консольного вывода.
- `HttpServer(pipeline, host="0.0.0.0", port=8000)`: первый аргумент — конвейер.
- Композиция: `HttpServer(StreamLogger(HttpBackend(url), dir, max_files))`, см. `example_proxy.py`.

## `StreamLogger` — файловый формат
- На запрос — файл `{YYMMDD}-{hhmmss}.{msec}-{host}-{port}.txt` (локальное время; `:` в host → `_`; коллизия имён → суффикс `-N`). В консоль — `[{hhmmss}.{msec}-{host}-{port}]` при старте и в конце (успех/ошибка).
- Секции через `==== {os.urandom(10).hex()} {сообщение} ====`: пустое — запрос (`METHOD path ver`, заголовки, пустая строка, тело), `RESPOND` — ответ (статус-строка `HTTP/1.1 status reason`, не метод/путь), `END`/`ABORT`/`ERROR ...` — финал.
- Пауза между чанками ≥1с (по `monotonic`) → разделитель `+{dlast:.3f}s / +{dfirst:.3f}s`, иначе чанк просто дописывается.
- Ротация после создания файла: пока `*.txt` больше `max_files` — удалять самые старые по `st_mtime_ns`. Считаются только `.txt`. Только что созданный файл из кандидатов исключён (`keep`): иначе при равном `mtime` по имени сносится он сам (`-N` < `.txt`), а следующая запись его воскрешает.
- Запись — открытием файла на каждую порцию (`emit`): дескриптор не держим, т.к. генератор могут не проитерировать, а синхронная запись легальна и на пути `GeneratorExit`.
- JSON-триггер: тело запроса — при `Content-Type: application/json` запроса; чанки ответа — при `Content-Type` ответа или `Accept` запроса. Пороги по символам UTF-8: <200 — как есть, <500 — `_pack_json_lines` (склейка строк `indent=1` до ширины 200, валидность сохраняется), иначе `YAML\n` + `_LiteralDumper`. Невалидный JSON — как есть. Каждый чанк форматируется независимо.
- `_LiteralDumper` — не упрощать до `yaml.safe_dump`: стоковый эмиттер запрещает `|` при пробелах перед переносом (`space_break`), а ТЗ требует блоки именно для таких строк. Кастомный `_LiteralEmitter` разрешает, round-trip через `safe_load` обязателен после правок.

## Владение генератором ответа — критично
- Вернувшийся `afread_response()` держит лок и keep-alive соединение `HttpBackend`. Его обязаны доитерировать до конца либо закрыть через `await body_gen.aclose()` (`http_server.py:191`). Иначе соединение/лок утекут.
- На пути `GeneratorExit` `await` запрещён: освобождение строго синхронное, закрытие сокета уводится в `asyncio.create_task` (`http_backend.py:208-216`). Не добавлять `await` в этот путь.
- Но закрыть ВЛОЖЕННЫЙ генератор через `await inner.aclose()` в `finally` — можно и нужно (запрещён только `yield` на пути `GeneratorExit`). Так делают `StreamLogger`, `HttpBackend` и `afdecode_sse` — иначе закрытие отдаётся сборщику мусора (см. предупреждение выше). `aclose` брать через `getattr(source, "aclose", None)`.
- Обрыв потребителя (`async for` прерван, клиент отвалился) делает соединение непригодным — помечать `reusable=False`/`drop=True`, как уже сделано.

## SSE
- Внутри конвейера сообщения ходят ЧИСТЫМИ (без `data:`, без `[DONE]`). `HttpBackend` снимает обвязку через `afdecode_sse`, `HttpServer` навешивает обратно через `encode_sse_message`/`encode_sse_done`, только если у ответа `Content-Type: text/event-stream`. Не оборачивать дважды.

## `HttpBackend` (`http_backend.py`)
- Ленивые keep-alive соединения: одно на `request.client_addr`, под `asyncio.Lock` на соединение. Создание экземпляра сокет не открывает.
- При `NetworkError` на протухшем соединении — одна прозрачная переподключалка с повтором запроса, дальше ошибка наружу.
- Переписывает `Host`/`Content-Length`/`Connection`, остальные заголовки форвардит как есть.
- При остановке вызывать `await backend.aclose_all()` (`example_proxy.py:37`).

## `HttpServer` (`http_server.py`)
- Свой парсер HTTP/1.1 на `asyncio streams`, каждое соединение — своя задача с keep-alive циклом. Создание экземпляра сокет не открывает; слушает в `astart()`/`aserve_forever()`.
- Тело запроса всегда вычитывается целиком; `ahandle_duplex` объявлен, но не реализован.
- Ошибки конвейера маппит в статусы: `NetworkError`/`HttpProtocolError` → 502, прочее → 500.
- Заголовки ответа пересобирает: режет `Transfer-Encoding`/`Connection`/`Content-Length` от backend; `Content-Length` отсутствует или SSE → `chunked` (SSE плюс `Cache-Control: no-cache` и финальный `data: [DONE]`).

## `http_common.py`
- Различать `HttpProtocolError` (битый протокол) и `NetworkError` (обрыв — можно переподключиться).
- Порядок чтения тела: `chunked` → `Content-Length` → `until-close` только при `allow_until_close`. Кодировка заголовков `iso-8859-1`.
- Конец chunked-тела — строки до первой пустой (трейлеры поддерживаются). `readuntil(b"\r\n\r\n")` после `0\r\n` делать нельзя: без трейлеров там всего 2 байта, чтение виснет навсегда.
- `afdecode_sse` нормализует CRLF/CR в LF до разбивки на события, иначе CRLF-стримы отдаются одним куском в конце.

## Ошибки и их обработка (методика)
- Три класса ошибок: сеть → `NetworkError`, протокол → `HttpProtocolError`, всё остальное (баги, диск, `LogError`, `ValueError`, `NotImplementedError`) → внутренняя ошибка.
- Заворот сырых исключений — на границе чтения/коннекта, как в правильных прокси: `(ConnectionError, OSError)` (включая `ConnectionResetError` при TLS-handshake, `ssl.SSLError`, `gaierror`, таймауты `wait_for`) → `NetworkError`; `LimitOverrunError`/oversize → `HttpProtocolError`. Сырой `OSError`/`TimeoutError` от чужого handler'а сервер тоже маппит в `502`/`504`, а не в `500`.
- SSE-разбор — это формат: обрыв транспорта внутри `afdecode_sse` → `NetworkError`, нарушение формата → `HttpProtocolError`.
- `HttpServer`: `NetworkError`/`HttpProtocolError`/сырой `OSError` → `502`, сырой `TimeoutError` → `504`, прочее → `500`. Ошибки чтения запроса от клиента — вне маппинга в статус: `NetworkError` → silent, остальное → лог без ответа.
- `HttpBackend`: одна прозрачная переподключалка при `NetworkError` на протухшем keep-alive; ошибки `_aopen_connection` сразу в `NetworkError` без ретрая вслепую. Любой обрыв тела ответа (включая сырой `OSError`) → `reusable=False`/`drop`, битое соединение в пул не возвращается.
- `StreamLogger` тип ошибки не меняет (транзит для `502`/`500`), файловые ошибки (`mkdir`/`touch`/`emit`) ярко подсвечивает в консоли красным (`!!! ...`, ANSI) и заворачивает в `LogError` → `500`. `sep` в обработчике чужой ошибки — через `safe_sep`, чтобы смерть диска не маскировала исходную ошибку. Сбой форматирования (`yaml.dump`) никогда не роняет запрос — отдаём тело как есть.
- `KeyboardInterrupt`/`CancelledError` (`BaseException`) не ловим и не заворачиваем нигде — им дают всплыть. `except Exception` их не глотает.
- Консоль: каждая строка, которую компонент выводит в консоль, начинается с имени его класса (`StreamLogger: ...`, `HttpServer: ...`, `HttpBackend: ...`). У `StreamLogger` имя стоит до ANSI-кодов подсветки, чтобы работал `startswith`.
- Импорты внутри пакета — только относительные (`from .http_common import ...`): абсолютные создают второй модуль-двойник, и `NetworkError` из разных модулей перестаёт ловиться `except` (было: `502` превращались в `500`).
