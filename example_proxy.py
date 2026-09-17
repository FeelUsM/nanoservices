"""
Пример запуска обратного прокси:

	python3 -m nanoservices.example_proxy --url http://127.0.0.1:8080 --dir log --host 0.0.0.0 --port 8000
"""

from __future__ import annotations

import argparse
import asyncio
import logging

from .http_backend import HttpBackend
from .http_server import HttpServer
from .pipeline import StreamLogger


def parse_args() -> argparse.Namespace:
	parser = argparse.ArgumentParser(description="Обратный прокси с логированием в файлы")
	parser.add_argument("--url", default="http://127.0.0.1:8080", help="URL backend'а")
	parser.add_argument("--dir", default="log", help="Папка файлов лога")
	parser.add_argument("--max-files", type=int, default=100, help="Максимум .txt файлов в папке лога")
	parser.add_argument("--host", default="0.0.0.0", help="Адрес прослушивания")
	parser.add_argument("--port", type=int, default=8000, help="Порт прослушивания")
	return parser.parse_args()


async def amain() -> None:
	logging.basicConfig(level=logging.INFO)
	args = parse_args()

	backend = HttpBackend(args.url)
	pipeline = StreamLogger(backend, args.dir, args.max_files, log=logging.getLogger("proxy").info)
	server = HttpServer(pipeline, host=args.host, port=args.port)

	try:
		await server.aserve_forever()
	finally:
		await backend.aclose_all()


if __name__ == "__main__":
	try:
		asyncio.run(amain())
	except KeyboardInterrupt:
		print('exit by Ctrl+C')
