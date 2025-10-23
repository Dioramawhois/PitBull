import aiohttp
import hashlib
import secrets
import base64
import random
import string
import json
import time
import logging

# 1. Импортируем наш глобальный "регулировщик"
from rate_limiter import limiter

logger = logging.getLogger(__name__)


async def mexc_call(url_mode, data, auth_token):
    await limiter.acquire()
    timestamp = int(time.time() * 1000)
    if url_mode == "create_order":
        url = "https://futures.mexc.com/api/v1/private/order/create"
        data["ts"] = timestamp - 5
        p0_raw = "".join(
            random.choices(string.ascii_letters + string.digits + "+/", k=384)
        )
        k0_raw = "".join(
            random.choices(string.ascii_letters + string.digits + "+/", k=384)
        )
        data["p0"] = base64.b64encode(p0_raw.encode()).decode()
        data["k0"] = base64.b64encode(k0_raw.encode()).decode()
        data["chash"] = secrets.token_hex(32)
        data["mtoken"] = secrets.token_hex(16)
        data["mhash"] = secrets.token_hex(16)
    elif url_mode == "close_position_market":
        url = "https://futures.mexc.com/api/v1/private/order/create"
    elif url_mode == "close_all_positions":
        data = {}
        url = "https://futures.mexc.com/api/v1/private/position/close_all"
    elif url_mode == "close_all_orders":
        data = {}
        url = "https://futures.mexc.com/api/v1/private/order/cancel_all"
    else:
        logger.error("Encryption error. Please cross-check your params")
        return
    data_str = json.dumps(data, separators=(",", ":"))
    first_md5 = hashlib.md5(f"{auth_token}{timestamp}".encode()).hexdigest()[7:]
    signature = hashlib.md5(f"{timestamp}{data_str}{first_md5}".encode()).hexdigest()
    headers = {
        "authorization": auth_token,
        "accept-language": "en-US",
        "content-type": "application/json",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "x-mxc-nonce": str(timestamp),
        "x-mxc-sign": signature,
    }
    async with aiohttp.ClientSession() as session:
        if url_mode in ["get_open_positions"]:
            async with session.get(url, headers=headers) as response:
                return await response.json()
        else:
            async with session.post(url, headers=headers, data=data_str) as response:
                return await response.json()


async def create_plan_order(payload, auth_token):
    await limiter.acquire()
    timestamp = int(time.time() * 1000)
    url = "https://futures.mexc.com/api/v1/private/planorder/place"
    data_str = json.dumps(payload, separators=(",", ":"))
    first_md5 = hashlib.md5(f"{auth_token}{timestamp}".encode()).hexdigest()[7:]
    signature = hashlib.md5(f"{timestamp}{data_str}{first_md5}".encode()).hexdigest()
    headers = {
        "authorization": auth_token,
        "accept-language": "en-US",
        "content-type": "application/json",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "x-mxc-nonce": str(timestamp),
        "x-mxc-sign": signature,
    }
    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=headers, data=data_str) as response:
            return await response.json()


async def get_open_positions(auth_tokens: set):
    opened_positions = {}
    await limiter.acquire()
    timestamp = int(time.time() * 1000)
    url = "https://futures.mexc.com/api/v1/private/position/open_positions"
    data_str = "{}"
    for auth_token in auth_tokens:
        first_md5 = hashlib.md5(f"{auth_token}{timestamp}".encode()).hexdigest()[7:]
        signature = hashlib.md5(
            f"{timestamp}{data_str}{first_md5}".encode()
        ).hexdigest()
        headers = {
            "authorization": auth_token,
            "accept-language": "en-US",
            "content-type": "application/json",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "x-mxc-nonce": str(timestamp),
            "x-mxc-sign": signature,
        }
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                opened_positions.update(await response.json())
    return opened_positions


async def get_market_ticker(symbol):
    await limiter.acquire()
    url = f"https://futures.mexc.com/api/v1/contract/ticker?symbol={symbol}"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=5) as response:
                if response.status == 200:
                    data: dict = await response.json()
                    if data.get("success"):
                        return data.get("data")
    except Exception:
        logger.exception(f"Ошибка при получении тикера для {symbol}")
        return None
    return None


async def get_all_market_tickers():
    await limiter.acquire()
    url = "https://futures.mexc.com/api/v1/contract/ticker"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=10) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get("success"):
                        return {item["symbol"]: item for item in data.get("data", [])}
    except Exception as e:
        logger.error(f"Ошибка при получении всех тикеров: {e}")
        return None
    return None


# mexc_futures_calls.py


async def _signed_request(
    url: str, auth_token: str, payload: dict | None, method: str = "POST"
):
    """Единый низкоуровневый запрос с тем же способом подписи, что и раньше."""
    await limiter.acquire()
    timestamp = int(time.time() * 1000)
    data_str = json.dumps(payload or {}, separators=(",", ":"))
    first_md5 = hashlib.md5(f"{auth_token}{timestamp}".encode()).hexdigest()[7:]
    signature = hashlib.md5(f"{timestamp}{data_str}{first_md5}".encode()).hexdigest()
    headers = {
        "authorization": auth_token,
        "accept-language": "en-US",
        "content-type": "application/json",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "x-mxc-nonce": str(timestamp),
        "x-mxc-sign": signature,
    }
    async with aiohttp.ClientSession() as session:
        if method == "GET":
            async with session.get(url, headers=headers) as resp:
                return await resp.json()
        else:
            async with session.post(url, headers=headers, data=data_str) as resp:
                return await resp.json()


async def _get_open_positions(auth_token: str):
    """Возвращает список всех открытых позиций аккаунта."""
    url = "https://futures.mexc.com/api/v1/private/position/open_positions"
    return await _signed_request(url, auth_token, payload=None, method="GET")


def _pick_position_id(
    open_positions_res: dict, symbol: str, position_type: int | None
) -> int | None:
    """Из ответа open_positions берём positionId нужной стороны по символу."""
    try:
        rows = (open_positions_res or {}).get("data") or []
        symbol = str(symbol).upper()
        # берем первую подходящую позицию по символу и стороне (если задана)
        if position_type in (1, 2):
            for r in rows:
                if (
                    r.get("symbol") == symbol
                    and r.get("positionType") == position_type
                    and r.get("state") == 1
                ):
                    return r.get("positionId")
        else:
            # one-way или позиция только с одной стороны — берём любую активную по символу
            for r in rows:
                if r.get("symbol") == symbol and r.get("state") == 1:
                    return r.get("positionId")
    except Exception:
        pass
    return None


async def change_leverage(
    symbol,
    leverage,
    auth_token,
    position_type: int | None = None,
    open_type: int = 1,
    holdSide: int | None = None,
):
    """
    Корректно меняет кредитное плечо под правила MEXC:
    - Если позиция по символу есть -> передаём positionId + leverage
    - Если позиции нет -> передаём openType + symbol + positionType + leverage
    Параметр holdSide (1/2) оставлен для обратной совместимости и мапится на position_type.
    position_type: 1=LONG, 2=SHORT
    open_type: 1=Isolated, 2=Cross (когда позиции нет; по умолчанию 1 как в вашем order payload)
    """
    symbol = str(symbol).upper()
    lev = int(leverage)

    # обратная совместимость: если пришёл holdSide — используем его как position_type
    if position_type not in (1, 2) and holdSide in (1, 2):
        position_type = holdSide

    # Пытаемся получить positionId (если уже есть позиция по этому символу/стороне)
    open_positions_res = await _get_open_positions(auth_token)
    pos_id = _pick_position_id(open_positions_res, symbol, position_type)

    url = "https://futures.mexc.com/api/v1/private/position/change_leverage"

    # Вариант A: по символу уже есть позиция — используем positionId + leverage
    if pos_id:
        payload = {"positionId": int(pos_id), "leverage": lev}
        logger.info(f"change_leverage A (by positionId): {payload}")
        res = await _signed_request(url, auth_token, payload, method="POST")
        return res

    # Вариант B: позиции нет — обязателен пакет openType + symbol + positionType + leverage
    # Если position_type не задан (one-way), безопасно проставим 1 (LONG) — биржа всё равно хранит единый левередж на символ при one-way.
    ptype = position_type if position_type in (1, 2) else 1
    base_payload = {
        "openType": int(open_type),
        "leverage": lev,
        "symbol": symbol,
        "positionType": int(ptype),
    }
    logger.info(
        f"change_leverage B (no position, with openType/symbol/positionType): {base_payload}"
    )
    res = await _signed_request(url, auth_token, base_payload, method="POST")

    # На некоторых аккаунтах при отсутствии позиции может требоваться другой openType; пробуем авто-ретрай 1->2 или 2->1 при 600.
    if res and not res.get("success") and res.get("code") == 600:
        flipped = 2 if int(open_type) == 1 else 1
        retry_payload = {**base_payload, "openType": flipped}
        logger.warning(
            f"change_leverage retry with flipped openType={flipped} due to 600. Prev={res}"
        )
        res2 = await _signed_request(url, auth_token, retry_payload, method="POST")
        return res2

    return res


async def cancel_all_orders(auth_token: str, symbol: str | None = None):
    """Отменить все незавершённые ОРДЕРЫ по символу (или все, если symbol=None)."""
    url = "https://futures.mexc.com/api/v1/private/order/cancel_all"
    payload = {"symbol": symbol} if symbol else {}
    return await _signed_request(url, auth_token, payload, method="POST")


async def cancel_all_plan_orders(auth_token: str, symbol: str | None = None):
    """Отменить все незавершённые ПЛАН-ордера (trigger/stop) по символу (или все)."""
    url = "https://futures.mexc.com/api/v1/private/planorder/cancel_all"
    payload = {"symbol": symbol} if symbol else {}
    return await _signed_request(url, auth_token, payload, method="POST")


async def get_order_by_id(auth_token: str, order_id: str | int):
    # GET /api/v1/private/order/get?order_id=...
    url = "https://futures.mexc.com/api/v1/private/order/get"
    params = {"orderId": str(order_id)}
    return await _signed_request(url, auth_token, params, method="GET")
