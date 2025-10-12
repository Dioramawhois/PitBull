import asyncio
import json
import os
from typing import Any, Dict, Iterable, Optional

import aiohttp
from loguru import logger

DATA_PROVIDER_URL = os.getenv("DATA_PROVIDER_URL", "http://127.0.0.1:8001/state")

SPREAD_THRESHOLD_PERCENT = float(os.getenv("SPREAD_THRESHOLD_PERCENT", "1.2"))
POLL_PERIOD_SEC = float(os.getenv("POLL_PERIOD_SEC", "13.0"))


def _norm_pair_key(k: str) -> str:
    ku = (k or "").upper()
    return ku if "_" in ku else f"{ku}_USDT"


def _base_from_pair(pair_key: str) -> str:
    return pair_key.split("_", 1)[0]


def _spot_url(pair_key: str) -> str:
    return f"https://www.mexc.com/ru-RU/exchange/{pair_key}"


def _calc_spread_pct(mexc_mid: Optional[float], dex: Optional[float]) -> Optional[float]:
    # Функция расчета спреда остается, но теперь принимает mexc_mid
    if mexc_mid is None or dex is None:
        return None
    if float(mexc_mid) == 0.0:
        return None
    # Считаем спред MEXC -> DEX
    return (float(dex) - float(mexc_mid)) / float(mexc_mid) * 100.0


def _iter_snapshot(snapshot: Any) -> Iterable[tuple[str, dict]]:
    """
    Унифицируем формат ответа от /state:
    - dict: {"ACH": {...}, "BNB": {...}}
    """
    if isinstance(snapshot, dict):
        for symbol, data in snapshot.items():
            if isinstance(data, dict):
                yield symbol, data


async def _read_snapshot(session: aiohttp.ClientSession, url: str) -> Any:
    async with session.get(url, headers={"Accept": "application/json"}) as resp:
        status = resp.status
        ctype = resp.headers.get("Content-Type", "")
        if status != 200:
            text = await resp.text()
            logger.warning("Снапшот %s -> HTTP %s (%s). Тело (первые 500): %r",
                           url, status, ctype, text[:500])
            return None
        try:
            return await resp.json()
        except Exception:
            text = await resp.text()
            s = text.lstrip("\ufeff").strip()
            start = min([i for i in [s.find("{"), s.find("[")] if i != -1] or [-1])
            if start > 0:
                s = s[start:]
            try:
                return json.loads(s)
            except Exception as e2:
                logger.error("Не удалось распарсить снапшот как JSON. Content-Type=%s. Ошибка: %s. "
                             "Первые 500 симв.: %r", ctype, e2, text[:500])
                return None


async def start_polling(
    orders_queue: "asyncio.Queue[Dict[str, Any]]",
    tokens_info: Dict[str, Dict[str, Any]],
) -> None:
    logger.info(f"Локальный генератор сигналов запущен. Цель: {DATA_PROVIDER_URL}")

    timeout = aiohttp.ClientTimeout(total=15)
    backoff = 1.0
    max_backoff = 10.0

    async with aiohttp.ClientSession(timeout=timeout) as session:
        while True:
            try:
                min_spread = float(os.getenv("SPREAD_MIN_PERCENT", "1.2"))
                max_spread = float(os.getenv("SPREAD_MAX_PERCENT", "90.0"))
                snapshot = await _read_snapshot(session, DATA_PROVIDER_URL)
                if not snapshot:
                    await asyncio.sleep(backoff)
                    backoff = min(max_backoff, backoff * 2)
                    continue
                backoff = 1.0

                for symbol, data in _iter_snapshot(snapshot):
                    logger.debug(f"Анализирую {symbol}: {data}")
                    best_bid = data.get("mexc_best_bid")
                    best_ask = data.get("mexc_best_ask")
                    dex_price = data.get("dex")

                    if best_bid is None or best_ask is None or dex_price is None:
                        continue
                    mexc_mid_price = (best_bid + best_ask) / 2.0
                    spread = _calc_spread_pct(mexc_mid_price, dex_price)
                    if spread is None or not (min_spread <= abs(spread) <= max_spread):
                        if spread is not None:
                            logger.info(f"Спред для {symbol} ({spread:.2f}%) вне диапазона ({min_spread}% - {max_spread}%). Пропускаем.")
                        continue
                    pair_key = _norm_pair_key(symbol)
                    base = _base_from_pair(pair_key)
                    token_details = tokens_info.get(pair_key, {})
                    order = {
                        "mexc_symbol": pair_key,
                        "base_coin_name": base,
                        "limit_price": mexc_mid_price,
                        "gmgn_price": dex_price,
                        "percent": spread,
                        "contract_size": token_details.get("contractSize", 1),
                        "price_scale": token_details.get("priceScale", 8),
                        "max_volume": token_details.get("maxVol", 10_000),
                        "mexc_url": _spot_url(pair_key),
                        "gmgn_url": "https://www.dextools.io/",
                    }
                    await orders_queue.put(order)
                    logger.info(f"Сигнал {pair_key}: спред {spread} (MEXC_mid={mexc_mid_price}, DEX={dex_price})")
                await asyncio.sleep(POLL_PERIOD_SEC)
            except aiohttp.ClientConnectorError:
                logger.error(f"Нет соединения с {DATA_PROVIDER_URL}. Проверь порт/хост.")
                await asyncio.sleep(backoff)
                backoff = min(max_backoff, backoff * 2)
            except Exception as e:
                logger.exception(f"Ошибка в локальном генераторе сигналов: {e}", exc_info=True)
                await asyncio.sleep(backoff)
                backoff = min(max_backoff, backoff * 2)