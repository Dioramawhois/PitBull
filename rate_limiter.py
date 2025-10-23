# rate_limiter.py
import asyncio
import time
from collections import deque


class APIRateLimiter:
    """
    Асинхронный rate limiter для контроля частоты запросов к API.
    Работает по принципу скользящего окна.
    """

    def __init__(self, requests_per_second: int):
        if requests_per_second <= 0:
            raise ValueError("Количество запросов в секунду должно быть больше нуля")

        self.rate_limit = requests_per_second
        self.interval = 1.0 / requests_per_second
        self.timestamps = deque()
        self._lock = asyncio.Lock()
        print(
            f"Rate Limiter инициализирован: не более {requests_per_second} запросов/сек."
        )

    async def acquire(self):
        """
        Запрашивает "слот" для выполнения API-запроса.
        Если лимит исчерпан, асинхронно ждет необходимое время.
        """
        async with self._lock:
            while True:
                now = time.monotonic()

                # Удаляем старые временные метки, которые уже вышли за пределы окна в 1 секунду
                while self.timestamps and self.timestamps[0] <= now - 1.0:
                    self.timestamps.popleft()

                # Если количество запросов за последнюю секунду меньше лимита
                if len(self.timestamps) < self.rate_limit:
                    self.timestamps.append(now)
                    break  # Разрешаем выполнение запроса

                # Если лимит исчерпан, вычисляем, сколько нужно подождать
                # до момента, когда самый старый запрос "освободит" место.
                time_to_wait = self.timestamps[0] - (now - 1.0)
                if time_to_wait > 0:
                    await asyncio.sleep(time_to_wait)


# --- ГЛОБАЛЬНЫЙ ЭКЗЕМПЛЯР ---
limiter = APIRateLimiter(requests_per_second=8)
