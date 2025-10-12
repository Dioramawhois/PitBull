import asyncio
import time
import platform
import sys
from loguru import logger
import os
from math import floor

from redis.asyncio import from_url
from mexc_futures_calls import mexc_call, create_plan_order, change_leverage, cancel_all_orders, cancel_all_plan_orders, get_open_positions, get_order_by_id
from token_services import read_file_async


def adjust_price_to_tick_size(price: float, price_scale: int) -> float:
    if price_scale is None:
        return price

    tick_size = 10 ** (-price_scale)
    adjusted_price = floor(price / tick_size) * tick_size

    return round(adjusted_price, price_scale)

def notify_beep():
    sys.stdout.write("\a")
    sys.stdout.flush()
    try:
        if platform.system() == "Windows": import winsound; winsound.Beep(1000, 300)
    except Exception:
        pass


async def handle_order_create(payload: dict, order_info: dict, mexc_auth: str):
    if not payload:
        logger.error(f"Создание ордера отменено: пустой payload для {order_info.get('mexc_symbol')}")
        return None
    result = await mexc_call(url_mode='create_order', data=payload, auth_token=mexc_auth)
    if result and result.get('success') and result.get('code') == 0:
        logger.info(f"Ордер создан - {order_info['mexc_symbol']}, результат: {result}")
        return result
    logger.error(f"Ошибка создания ордера для {order_info.get('mexc_symbol')}: {result}")
    return None


async def prepare_payload(limit_price, side, order, leverage, max_margin, token):
    if not limit_price or limit_price <= 0:
        logger.error(f"[{order['mexc_symbol']}] Некорректная цена: {limit_price}. Пропускаем.")
        return None

    # НЕ позволяем токеновому лимиту превышать глобальный
    token_margin = token.get('max_margin')
    if isinstance(token_margin, (int, float)) and token_margin > 0:
        margin = min(max_margin, token_margin)
    else:
        margin = max_margin

    contract_size = token.get('contractSize', 1.0) or 1.0
    symbol_name   = token.get('mexc_symbol', '')
    price_scale   = token.get('priceScale')
    vol_scale     = token.get('volScale')
    if price_scale is None or vol_scale is None:
        logger.error(f"[{order['mexc_symbol']}] Отсутствуют данные о точности! Невозможно рассчитать объем.")
        return None

    # Объём от маржи: (маржа * плечо) / цена / размер контракта
    raw_vol = (margin * int(leverage)) / float(limit_price) / float(contract_size)

    # Округляем ВНИЗ к шагу объёма, чтобы не превысить доступную маржу на копейки
    step = 10 ** (-vol_scale)
    final_volume = floor(raw_vol / step) * step
    if vol_scale == 0:
        final_volume = int(final_volume)

    if final_volume < step:
        logger.warning(f"[{order['mexc_symbol']}] Рассчитанный объем < минимального шага ({final_volume} < {step}). Пропускаем.")
        return None

    final_price = round(limit_price, price_scale)
    logger.info(f"[{order['mexc_symbol']}] Рассчитанный объем ордера: {final_volume} контрактов")

    # type — как число, не строка; openType=1 (Isolated), leverage обязателен для Isolated по доке
    return {
        "symbol":   symbol_name,
        "side":     side,           # 1=open long, 3=open short
        "leverage": int(leverage),
        "vol":      final_volume,
        "openType": 1,              # isolated
        "type":     1,              # limit
        "price":    str(final_price)
    }

# --- Основные функции с ИСПРАВЛЕНИЯМИ ---
async def fetch_position_id_for_symbol(auth_token: str, symbol: str, want_side_open: int, attempts: int = 10, delay: float = 0.3):
    """
    want_side_open: 1 = открывали LONG -> ищем positionType=1
                    3 = открывали SHORT -> ищем positionType=2
    Возвращает positionId или None.
    """
    target_ptype = 1 if want_side_open == 1 else 2
    symbol = str(symbol).upper()
    for _ in range(attempts):
        resp = await get_open_positions(auth_token)
        if resp and resp.get('success'):
            for r in resp.get('data', []) or []:
                if r.get('symbol') == symbol and r.get('state') == 1 and r.get('positionType') == target_ptype:
                    return r.get('positionId')
        await asyncio.sleep(delay)
    return None


async def handle_order(token, order, redis_client, settings):
    token['order_in_process'] = True
    symbol = token['mexc_symbol']
    logger.info(f"Начало обработки ордера для {symbol}...")
    try:
        mexc_auth = settings.get('MEXC_AUTH_TOKEN')
        max_margin = settings.get('max_margin')
        target_leverage = int(settings.get('leverage', 20))
        if not all([mexc_auth, max_margin, target_leverage]):
            logger.error(f"[{symbol}] Отсутствуют ключевые параметры в settings.json. Обработка прервана.")
            return

        max_allowed_leverage = token.get('maxLeverage', target_leverage)
        actual_leverage_to_set = min(target_leverage, max_allowed_leverage)

        # --- УСТАНОВКА ПЛЕЧА ---
        leverage_res = None
        if settings.get("IS_HEDGE_MODE", True):
            logger.info(f"[{symbol}] Режим хеджирования. Установка плеча {actual_leverage_to_set}x для LONG и SHORT.")
            long_res  = await change_leverage(symbol, actual_leverage_to_set, mexc_auth, position_type=1)
            short_res = await change_leverage(symbol, actual_leverage_to_set, mexc_auth, position_type=2)

            ok       = lambda r: bool(r and r.get('success'))
            is_2019  = lambda r: bool(r and r.get('code') == 2019)
            is_600   = lambda r: bool(r and r.get('code') == 600)

            if ok(long_res) and ok(short_res):
                leverage_res = long_res
            else:
                # 2019: есть незавершённые заявки — снимаем и повторяем проблемную сторону
                if is_2019(long_res) or is_2019(short_res):
                    logger.warning(f"[{symbol}] change_leverage вернул 2019 -> отменяю открытые ордера и повторяю.")
                    await cancel_all_orders(mexc_auth, symbol)
                    await cancel_all_plan_orders(mexc_auth, symbol)
                    await asyncio.sleep(0.25)
                    if not ok(long_res):
                        long_res  = await change_leverage(symbol, actual_leverage_to_set, mexc_auth, position_type=1)
                    if not ok(short_res):
                        short_res = await change_leverage(symbol, actual_leverage_to_set, mexc_auth, position_type=2)

                if ok(long_res) and ok(short_res):
                    leverage_res = long_res
                else:
                    # Фоллбэк: если обе стороны дали 600 (не тот режим) — пробуем One-way
                    if is_600(long_res) and is_600(short_res):
                        logger.warning(f"[{symbol}] Оба change_leverage вернули 600 в Hedge. Пробую One-way.")
                        oneway_res = await change_leverage(symbol, actual_leverage_to_set, mexc_auth)
                        if not (oneway_res and oneway_res.get('success')):
                            logger.error(f"[{symbol}] НЕ УДАЛОСЬ ИЗМЕНИТЬ ПЛЕЧО даже в One-way. LONG: {long_res}, SHORT: {short_res}, ONEWAY: {oneway_res}")
                            await redis_client.set(f"cooldown:{symbol}", 1, ex=60)
                            return
                        leverage_res = oneway_res
                    else:
                        logger.error(f"[{symbol}] НЕ УДАЛОСЬ ИЗМЕНИТЬ ПЛЕЧО в Hedge. LONG: {long_res}, SHORT: {short_res}")
                        await redis_client.set(f"cooldown:{symbol}", 1, ex=60)
                        return
        # Если One-way — просто используем плановое плечо
        if not leverage_res:
            leverage_res = {"data": {"leverage": actual_leverage_to_set}}

        final_leverage = leverage_res.get('data', {}).get('leverage', actual_leverage_to_set)

        # --- СОЗДАНИЕ ОСНОВНОГО ОРДЕРА ---
        side = 1 if order.get('percent', 0) > 0 else 3  # 1=open long, 3=open short
        limit_price = order.get('limit_price')
        payload = await prepare_payload(limit_price, side, order, final_leverage, max_margin, token)
        if not payload:
            return

        entry_mode = str(settings.get('ENTRY_ORDER_TYPE', 'limit')).lower()
        if entry_mode == 'market':
            payload['type'] = 5
            payload.pop('price', None)

        main_order_res = await handle_order_create(payload, order, mexc_auth)

        # Автоматически проверяем успех, ждём positionId (по orderId/open_positions) и ставим SL/TP reduceOnly
        pos_id = await finalize_order_and_setup_sl_tp(
            main_order_res,
            symbol=symbol,
            side=side,
            limit_price=limit_price,
            payload=payload,
            token=token,
            settings=settings,
            mexc_auth=mexc_auth,
            redis_client=redis_client
        )

        # --- Служебное: спред для мониторинга, кулдаун, бип ---
        if pos_id:
            await redis_client.set(f"initial_spread:{pos_id}", order.get('percent'))
        if settings.get("USE_COOLDOWN", True):
            success_cooldown = settings.get("COOLDOWN_SECONDS", 300)
            await redis_client.set(f"cooldown:{symbol}", 1, ex=success_cooldown)
            logger.info(f"Для {symbol} установлен кулдаун на {success_cooldown}с.")
        notify_beep()
    except Exception as ex:
        logger.error(f'Критическая ошибка в handle_order для {symbol}: {ex}', exc_info=True)
    finally:
        token['order_in_process'] = False
        logger.info(f"Обработка для {symbol} завершена.")


async def finalize_order_and_setup_sl_tp(
    main_order_res: dict,
    *,
    symbol: str,
    side: int,               # 1 = open long, 3 = open short
    limit_price: float,
    payload: dict,
    token: dict,
    settings: dict,
    mexc_auth: str,
    redis_client
) -> "int | None":
    """
    Проверяет результат создания ордера, получает positionId и выставляет SL/TP (reduceOnly) план-ордера.
    Возвращает positionId (int) или None.

    Требуются вспомогательные функции (должны быть определены в проекте):
      - wait_fill_and_position_id(auth_token, order_id, symbol, want_side_open, max_wait_s, poll_delay_s)
      - fetch_position_id_for_symbol(auth_token, symbol, want_side_open, attempts, delay)
      - create_plan_order(payload, auth_token)
    """
    # 1) Жёсткая проверка на успех + наличие data
    if not (main_order_res and main_order_res.get('success') and main_order_res.get('code') == 0):
        logger.error(f"[{symbol}] НЕ УДАЛОСЬ СОЗДАТЬ ОРДЕР. Ответ: {main_order_res}. Обработка отменена.")
        await redis_client.set(f"cooldown:{symbol}", 1, ex=60)
        return None

    data = main_order_res.get('data') or {}
    order_id = str(data.get('orderId') or '')

    # 2) Определяем режим входа (market/limit) — влияет на стратегию ожидания
    entry_mode = str(settings.get('ENTRY_ORDER_TYPE', 'limit')).lower()

    pos_id = None
    if order_id:
        # Если реализована wait_fill_and_position_id(...), пользуемся ей
        try:
            wait_s = 8.0 if entry_mode == 'market' else 60.0
            pos_id = await wait_fill_and_position_id(
                mexc_auth, order_id, symbol, want_side_open=side,
                max_wait_s=wait_s, poll_delay_s=0.5
            )
        except NameError:
            # Функции нет — падаем в универсальный фоллбэк
            pos_id = None

    # 3) Универсальный фоллбэк: ждём позицию напрямую через open_positions
    if not pos_id:
        attempts = 20 if entry_mode == 'market' else 8
        delay    = 0.3 if entry_mode == 'market' else 1.0
        pos_id = await fetch_position_id_for_symbol(
            mexc_auth, symbol, want_side_open=side,
            attempts=attempts, delay=delay
        )

    vol    = payload.get('vol')
    sl_pct = settings.get('EXCHANGE_STOP_LOSS_PERCENT')
    tp_pct = settings.get('EXCHANGE_TAKE_PROFIT_PERCENT')

    if pos_id and vol and (sl_pct or tp_pct):
        price_scale = token.get('priceScale', 8)
        tasks = []
        # правильные коды "закрыть": 4 для LONG, 2 для SHORT
        close_side = 4 if side == 1 else 2

        if tp_pct:
            if tp_pct:
                raw_tp_price = limit_price * (1 + tp_pct / 100)
                tp_price = adjust_price_to_tick_size(raw_tp_price, price_scale
            ) if side == 1 else round(
                limit_price * (1 - tp_pct / 100),
                price_scale
            )
            tp_payload = {
                "symbol": symbol,
                "vol": vol,
                "side": close_side,
                "triggerPrice": tp_price,
                "triggerType": (1 if side == 1 else 2),
                "orderType": 2,
                "positionId": pos_id,
                "reduceOnly": True
            }
            tasks.append(create_plan_order(tp_payload, mexc_auth))

        if sl_pct:
            raw_sl_price = limit_price * (1 - sl_pct / 100)
            sl_price = adjust_price_to_tick_size(raw_sl_price, price_scale
            ) if side == 1 else round(
                limit_price * (1 + sl_pct / 100),
                price_scale
            )
            sl_payload = {
                "symbol": symbol,
                "vol": vol,
                "side": close_side,
                "triggerPrice": sl_price,
                "triggerType": (2 if side == 1 else 1),
                "orderType": 2,
                "positionId": pos_id,
                "reduceOnly": True
            }
            tasks.append(create_plan_order(sl_payload, mexc_auth))

        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for res in results:
                if isinstance(res, Exception) or not (res and res.get('success')):
                    logger.error(f"[{symbol}] Не удалось установить SL/TP ордер: {res}")
    else:
        logger.warning(f"[{symbol}] Не удалось получить positionId для постановки SL/TP (orderId={order_id or '—'}, pos_id={pos_id}).")

    return pos_id


async def start_process(_tokens_info, _task_status, orders_queue):
    global tokens_info, task_status
    tokens_info, task_status = _tokens_info, _task_status
    redis_client = from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"), decode_responses=True)
    logger.info("Процесс обработки ордеров запущен и подключен к Redis.")
    while True:
        token = None
        order = None
        try:
            order = await orders_queue.get()
            settings = await read_file_async('settings.json') or {}
            mexc_symbol = order.get('mexc_symbol')
            if not mexc_symbol or mexc_symbol not in tokens_info:
                logger.error(f"Токен не найден: {mexc_symbol}. Сигнал пропущен.")
                continue
            token = tokens_info[mexc_symbol]
            if settings.get("USE_COOLDOWN", True) and await redis_client.exists(f"cooldown:{mexc_symbol}"):
                logger.info(f"Сигнал по {mexc_symbol} пропущен из-за кулдауна.")
                continue
            if token.get('is_ignored'): continue
            if token.get("order_in_process"):
                logger.warning(f"Ордер для {mexc_symbol} уже в обработке. Сигнал пропущен.")
                continue
            await handle_order(token, order, redis_client, settings)

        except asyncio.CancelledError:
            logger.warning("Задача обработки ордеров была отменена.")
            break

        except Exception as ex:
            logger.error(f'Критическая ошибка в цикле start_process: {ex}', exc_info=True)
            await asyncio.sleep(1)
        finally:
            if token: token['order_in_process'] = False

            if order is not None:
                orders_queue.task_done()

async def wait_fill_and_position_id(auth_token: str, order_id: str, symbol: str, want_side_open: int,
                                    max_wait_s: float = 60.0, poll_delay_s: float = 0.5):
    """
    Ждём, пока ордер исполнится (state=3), и берём positionId из ответа /order/get.
    Если за отведённое время не исполнился — возвращаем None.
    """
    deadline = time.time() + max_wait_s
    while time.time() < deadline:
        resp = await get_order_by_id(auth_token, order_id)
        if resp and resp.get('success'):
            data = resp.get('data') or {}
            state = data.get('state')    # 1/2/3/4/5
            pos_id = data.get('positionId')
            if state == 3 and pos_id:    # исполнен → позиция создана
                return pos_id
        await asyncio.sleep(poll_delay_s)
    return None
