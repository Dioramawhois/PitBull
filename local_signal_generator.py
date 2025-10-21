import asyncio
import json
import os
from typing import Any, Dict, Optional

import aiohttp
from aiohttp import WSMsgType
from loguru import logger

DATA_PROVIDER_WS_URL = os.getenv("DATA_PROVIDER_WS_URL", "ws://127.0.0.1:8001/ws/tokens")


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


async def _process_symbol_update(
    payload: Dict[str, Any],
    tokens_info: Dict[str, Dict[str, Any]],
    orders_queue: "asyncio.Queue[Dict[str, Any]]",
    min_spread: float,
    max_spread: float,
) -> None:
    symbol = (payload or {}).get("symbol")
    if not symbol:
        logger.debug("Пропускаю сообщение без символа: %s", payload)
        return

    best_bid = payload.get("mexc_best_bid")
    best_ask = payload.get("mexc_best_ask")
    dex_price = payload.get("dex")

    if best_bid is None or best_ask is None or dex_price is None:
        logger.debug("Пропускаю %s: нет bid/ask/dex.", symbol)
        return

    mexc_mid_price = (best_bid + best_ask) / 2.0
    spread = _calc_spread_pct(mexc_mid_price, dex_price)
    if spread is None or not (min_spread <= abs(spread) <= max_spread):
        if spread is not None:
            logger.info(
                "Спред для %s (%.2f%%) вне диапазона (%.2f%% - %.2f%%). Пропускаем.",
                symbol,
                abs(spread),
                min_spread,
                max_spread,
            )
        return

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
        "dextools_pairs": payload.get("dextools_pairs", []),
    }
    await orders_queue.put(order)
    logger.info(f"Сигнал {pair_key}: спред {spread} (MEXC_mid={mexc_mid_price}, DEX={dex_price})")


async def _handle_event(
    event: Dict[str, Any],
    tokens_info: Dict[str, Dict[str, Any]],
    orders_queue: "asyncio.Queue[Dict[str, Any]]",
    min_spread: float,
    max_spread: float,
) -> None:
    if not isinstance(event, dict):
        logger.debug("Получено сообщение неожиданного формата: %r", event)
        return

    event_name = event.get("event")
    payload = event.get("payload")

    if event_name == "symbol_update":
        await _process_symbol_update(payload, tokens_info, orders_queue, min_spread, max_spread)
    else:
        logger.debug("Необработанное событие %s", event_name)




async def start_polling(
    orders_queue: "asyncio.Queue[Dict[str, Any]]",
    tokens_info: Dict[str, Dict[str, Any]],
) -> None:
    logger.info(f"Локальный генератор сигналов запущен. Цель: {DATA_PROVIDER_WS_URL}")

    timeout = aiohttp.ClientTimeout(total=15)
    backoff = 0.5
    max_backoff = 10.0

    while True:
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.ws_connect(
                    DATA_PROVIDER_WS_URL,
                    heartbeat=30.0,
                    receive_timeout=60.0,
                ) as ws:
                    logger.info("Подключились к WebSocket %s", DATA_PROVIDER_WS_URL)
                    backoff = 0.5

                    async for msg in ws:
                        if msg.type == WSMsgType.TEXT:
                            try:
                                data = json.loads(msg.data)
                            except json.JSONDecodeError:
                                logger.warning("Не удалось распарсить сообщение: %r", msg.data)
                                continue
                        elif msg.type == WSMsgType.BINARY:
                            try:
                                data = json.loads(msg.data.decode("utf-8"))
                            except (UnicodeDecodeError, json.JSONDecodeError):
                                logger.warning("Не удалось распарсить бинарное сообщение.")
                                continue
                        elif msg.type in (WSMsgType.CLOSED, WSMsgType.CLOSE):
                            logger.warning("WebSocket закрыт (%s).", ws.close_code)
                            break
                        elif msg.type == WSMsgType.ERROR:
                            logger.error("Ошибка WebSocket: %s", ws.exception())
                            break
                        else:
                            continue

                        min_spread = float(os.getenv("SPREAD_MIN_PERCENT", "1.2"))
                        max_spread = float(os.getenv("SPREAD_MAX_PERCENT", "90.0"))
                        await _handle_event(data, tokens_info, orders_queue, min_spread, max_spread)

        except aiohttp.ClientConnectorError:
            logger.error(f"Нет соединения с {DATA_PROVIDER_WS_URL}. Проверь порт/хост.")
        except Exception as e:
            logger.exception(f"Ошибка в локальном генераторе сигналов: {e}", exc_info=True)

        await asyncio.sleep(backoff)
        backoff = min(max_backoff, backoff * 2)
