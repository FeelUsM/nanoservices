"""
Пример запуска обратного прокси:

	python -m async_proxy.example_proxy

Слушает 0.0.0.0:8000, форвардит запросы на 127.0.0.1:8080, логируя их через StreamLogger.
"""

from __future__ import annotations

import asyncio
import logging

from .http_backend import HttpBackend
from .http_server import HttpServer
from .pipeline import StreamLogger


async def amain() -> None:
	logging.basicConfig(level=logging.INFO)

	backend = HttpBackend(target_host="127.0.0.1", target_port=8080)
	pipeline = StreamLogger(backend, log=logging.getLogger("proxy").info)
	server = HttpServer(host="0.0.0.0", port=8000, handler=pipeline)

	try:
		await server.aserve_forever()
	finally:
		await backend.aclose_all()


if __name__ == "__main__":
	asyncio.run(amain())
