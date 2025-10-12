# redis_repo.py
from typing import Optional, Dict, Any, Iterable
from redis.asyncio import Redis


def _b(x: Optional[str]) -> Optional[bytes]:
    return None if x is None else x.encode()


def _to_str(v: Optional[bytes]) -> str:
    return "" if v is None else v.decode()


def _to_bool(s: str) -> bool:
    return s == "1"


def _to_opt_float(s: str) -> Optional[float]:
    return None if s == "" else float(s)


def _to_opt_int(s: str) -> Optional[int]:
    return None if s == "" else int(s)


TOKEN_SET_KEY = "tokens:all"


class TokensRepo:
    def __init__(self, redis: Redis):
        self.r = redis

    async def list_symbols(self) -> list[str]:
        return [s.decode() for s in await self.r.smembers(TOKEN_SET_KEY)]

    async def get(self, symbol: str) -> Optional[Dict[str, Any]]:
        key = f"token:{symbol}"
        data = await self.r.hgetall(key)
        if not data:
            return None
        d = {k.decode(): _to_str(v) for k, v in data.items()}
        return {
            "mexc_symbol": d.get("mexc_symbol", symbol),
            "base_coin_name": d.get("base_coin_name", symbol.split("_", 1)[0]),
            "custom_percent": _to_opt_float(d.get("custom_percent", "")),
            "is_ignored": _to_bool(d.get("is_ignored", "1")),
            "is_normik": _to_bool(d.get("is_normik", "0")),
            "max_margin": _to_opt_int(d.get("max_margin", "")),
        }

    # --- НАЧАЛО ДОБАВЛЕННОГО КОДА ---

    async def upsert(self, token: Dict[str, Any]) -> None:
        """Сохраняет один токен в Redis."""
        symbol = token.get("mexc_symbol")
        if not symbol:
            return

        key = f"token:{symbol}"
        # Преобразуем все значения в строки для Redis, обрабатывая None и bool
        payload = {
            k: "" if v is None else ("1" if v is True else "0" if v is False else str(v))
            for k, v in token.items()
        }

        async with self.r.pipeline(transaction=True) as pipe:
            pipe.hset(key, mapping=payload)
            pipe.sadd(TOKEN_SET_KEY, symbol)
            await pipe.execute()

    async def upsert_many(self, tokens: Iterable[Dict[str, Any]]) -> None:
        """Эффективно сохраняет множество токенов за один раз."""
        async with self.r.pipeline(transaction=True) as pipe:
            for token in tokens:
                symbol = token.get("mexc_symbol")
                if not symbol:
                    continue

                key = f"token:{symbol}"
                payload = {
                    k: "" if v is None else ("1" if v is True else "0" if v is False else str(v))
                    for k, v in token.items()
                }
                pipe.hset(key, mapping=payload)
                pipe.sadd(TOKEN_SET_KEY, symbol)
            await pipe.execute()
