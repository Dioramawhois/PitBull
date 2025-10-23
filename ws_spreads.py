# pitbull_v2/ws_spreads.py (ФИНАЛЬНАЯ ВЕРСИЯ)

import asyncio
import json
import logging
import websockets

# URL и ключ для подключения к серверу сигналов
BASE_URL = "ws://193.124.114.27:3000/ws?private_key=FKaskdkaskkah9sd9f9sdfgjsdj9gs21"

logger = logging.getLogger(__name__)


async def start_ws(orders_queue):
    while True:
        try:
            # Устанавливаем таймаут для подключения, чтобы не ждать вечно
            async with websockets.connect(BASE_URL, open_timeout=10) as ws:
                logger.info("WebSocket для получения сигналов подключен.")
                while True:
                    message = await ws.recv()
                    # Проверяем, что сообщение похоже на валидный JSON
                    if (
                        "base_coin_name" in message
                        and "{" in message
                        and "}" in message
                    ):
                        try:
                            data = json.loads(message)
                            await orders_queue.put(data)
                            logger.info(f"Получен новый сигнал: {data}")
                        except json.JSONDecodeError:
                            logger.warning(
                                f"Не удалось декодировать JSON из сообщения: {message}"
                            )

        # Обработка конкретных ошибок веб-сокета
        except (
            websockets.exceptions.ConnectionClosedError,
            websockets.exceptions.ConnectionClosedOK,
        ) as e:
            logger.warning(
                f"WebSocket соединение закрыто: {e}. Переподключение через 5 секунд..."
            )
        except asyncio.TimeoutError:
            logger.error(
                "Тайм-аут при подключении к WebSocket. Переподключение через 5 секунд..."
            )
        except Exception as e:
            logger.error(
                f"Неожиданная ошибка WebSocket: {e}. Переподключение через 5 секунд..."
            )

        await asyncio.sleep(2)  # Пауза перед попыткой переподключения
