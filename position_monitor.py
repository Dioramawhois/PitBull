import asyncio
import logging
import os
import time
from typing import Any
from redis.asyncio import from_url

from mexc_futures_calls import get_open_positions, mexc_call, get_all_market_tickers
from settings.auth import mexc_tokens
from token_services import read_file_async
from local_signal_generator import _read_snapshot, _iter_snapshot, _norm_pair_key, _calc_spread_pct
# Импортируем функции для создания ордеров из order_process
from order_process import prepare_payload, handle_order_create

logger = logging.getLogger(__name__)
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")


class SpreadTracker:
    def __init__(self, url, period_sec):
        self._url, self._period, self._spreads, self._task = url, period_sec, {}, None,

    async def _fetch_spreads(self):
        import aiohttp
        async with aiohttp.ClientSession() as session:
            while True:
                try:
                    snapshot = await _read_snapshot(session, self._url)
                    if snapshot:
                        current_spreads = {}
                        for raw_key, data in _iter_snapshot(snapshot):
                            pair_key = _norm_pair_key(raw_key)
                            spread = data.get("spread_pct") or _calc_spread_pct(data.get("mexc"), data.get("dex"))
                            if spread is not None: current_spreads[pair_key] = spread
                        self._spreads = current_spreads
                except Exception as e:
                    logger.error(f"Ошибка в трекере спредов: {e}")
                await asyncio.sleep(self._period)

    def start(self):
        if self._task is None: self._task = asyncio.create_task(self._fetch_spreads()); logger.info("Трекер спредов запущен.")

    def get_spreads(self):
        return self._spreads


async def close_position(position, auth_token, reason="", close_vol=None):
    symbol, hold_vol, pos_type, pos_id = position['symbol'], position['holdVol'], position['positionType'], position['positionId']
    vol_to_close = close_vol if close_vol is not None else hold_vol

    if vol_to_close > hold_vol:
        logger.warning(f"[{symbol}] Попытка закрыть {vol_to_close}, когда открыто {hold_vol}. Закрываем всю позицию.")
        vol_to_close = hold_vol

    if vol_to_close <= 0:
        logger.warning(f"[{symbol}] Объем для закрытия равен нулю. Ордер не отправлен.")
        return False

    side = 4 if pos_type == 1 else 2
    payload = {"symbol": symbol, "vol": vol_to_close, "side": side, "type": 6, "positionId": pos_id}

    log_msg_prefix = 'ЧАСТИЧНОЕ' if close_vol and close_vol < hold_vol else 'ПОЛНОЕ'
    logger.warning(f"[{symbol}] {log_msg_prefix} ЗАКРЫТИЕ ПОЗИЦИИ. Объем: {vol_to_close}. Причина: {reason}.")

    result = await mexc_call(url_mode='close_position_market', data=payload, auth_token=auth_token)

    if result and result.get('success'):
        logger.info(f"[{symbol}] УСПЕХ! Позиция успешно закрыта!")
        return True

    logger.error(f"[{symbol}] ОШИБКА ЗАКРЫТИЯ! Ответ биржи: {result}")
    return False


async def start_monitoring(tokens_info: dict[str, Any]):
    """
    Основной цикл мониторинга открытых позиций.
    Применяет различные торговые стратегии и риск-менеджмент.
    """
    logger.info("Монитор позиций запущен.")
    redis = from_url(REDIS_URL, decode_responses=True)
    spread_tracker = SpreadTracker(url="http://127.0.0.1:8001/state", period_sec=15)
    spread_tracker.start()

    while True:
        try:
            settings = await read_file_async('settings.json')
            if not settings:
                logger.error("Не удалось загрузить настройки из settings.json")
                raise Exception("Нет настроек.")

            auth_tokens = mexc_tokens
            if not auth_tokens:
                logger.error("Нет токенов аутентификации MEXC. Монитор позиций не может работать.")
                raise Exception("Нет токенов аутентификации MEXC.")

            interval = settings.get('MONITOR_INTERVAL_SECONDS', 10)

            positions_response: dict = await get_open_positions(auth_tokens)
            if not positions_response or not positions_response.get('success'):
                logger.warning(f"Не удалось получить открытые позиции: {positions_response}")
                await asyncio.sleep(interval)
                continue

            open_positions = positions_response.get('data', [])
            if not open_positions:
                await asyncio.sleep(interval)
                continue

            all_tickers = await get_all_market_tickers()
            if not all_tickers:
                logger.warning("Не удалось получить актуальные цены с биржи. Пропускаем итерацию.")
                await asyncio.sleep(interval)
                continue

            _ = spread_tracker.get_spreads()

            for pos in open_positions:
                required_keys = ['symbol', 'openAvgPrice', 'positionType', 'positionId', 'createTime', 'leverage', 'holdVol']
                if not all(k in pos for k in required_keys):
                    logger.warning(f"Получены неполные данные для позиции: {pos}. Пропускаем.")
                    continue

                symbol, entry_price, pos_type, pos_id, open_time_ms, leverage, hold_vol = \
                    pos['symbol'], pos['openAvgPrice'], pos['positionType'], pos['positionId'], pos['createTime'], pos['leverage'], pos['holdVol']

                token_details: dict | None = tokens_info.get(symbol)
                if not token_details:
                    logger.warning(f"Нет деталей для токена {symbol}. Функции Scaling Out/Pyramiding могут быть неточными.")
                    continue
                vol_scale = token_details.get('volScale', 0)

                ticker = all_tickers.get(symbol)
                if not ticker:
                    logger.warning(f"Не найден тикер для открытой позиции {symbol}. Пропускаем.")
                    continue

                current_price = ticker['lastPrice']
                base_pnl_pct = ((current_price - entry_price) / entry_price * 100) if pos_type == 1 else (
                            (entry_price - current_price) / entry_price * 100)
                pnl_pct = base_pnl_pct * leverage

                log_prefix = f"[{symbol} | {'LONG' if pos_type == 1 else 'SHORT'} | {leverage}x]"
                logger.info(f"{log_prefix} PnL: {pnl_pct:.3f}% (Вход: {entry_price}, Сейчас: {current_price})")

                # Ключи Redis для хранения состояния этой позиции
                peak_price_key = f"peak_price:{pos_id}"
                initial_spread_key = f"initial_spread:{pos_id}"
                initial_vol_key = f"initial_vol:{pos_id}"

                # --- 2. Стратегия "Pyramiding" (Добавление к прибыльной позиции) ---
                if settings.get('USE_PYRAMIDING'):
                    pyramiding_config = settings.get('PYRAMIDING_CONFIG', {})
                    pnl_threshold = pyramiding_config.get('pnl_threshold_percent', 999)

                    if pnl_pct >= pnl_threshold:
                        pyramiding_entries_key = f"pyramiding_entries:{pos_id}"
                        entries_count = int(await redis.get(pyramiding_entries_key) or 0)

                        if entries_count < pyramiding_config.get('max_entries', 0):
                            pyramiding_lock_key = f"pyramiding_lock:{pos_id}:{entries_count}"
                            # Блокировка, чтобы не отправлять повторные ордера
                            if await redis.set(pyramiding_lock_key, 1, ex=300, nx=True):
                                logger.info(f"{log_prefix} PnL > {pnl_threshold}%. Попытка добавить к позиции (вход #{entries_count + 1}).")
                                order_like = {"mexc_symbol": symbol, "limit_price": current_price}
                                new_payload = await prepare_payload(
                                    limit_price=current_price, side=(1 if pos_type == 1 else 3), order=order_like,
                                    leverage=leverage, max_margin=pyramiding_config.get('add_margin_amount', 5), token=token_details
                                )
                                if new_payload:
                                    for auth_token in auth_tokens:
                                        add_result = await handle_order_create(new_payload, order_like, auth_token)
                                        if add_result and add_result.get('success'):
                                            await redis.incr(pyramiding_entries_key)
                                            logger.info(f"{log_prefix} УСПЕШНО добавлено к позиции.")

                # --- 3. Стратегия "Scaling Out" (Частичная фиксация прибыли) ---
                if settings.get('USE_SCALING_OUT'):
                    scaling_targets = settings.get('SCALING_OUT_TARGETS', [])
                    initial_vol_str = await redis.get(initial_vol_key)
                    if not initial_vol_str:
                        await redis.set(initial_vol_key, hold_vol)
                        initial_vol = float(hold_vol)
                    else:
                        initial_vol = float(initial_vol_str)

                    target_hit = False
                    for i, target in enumerate(sorted(scaling_targets, key=lambda x: x['pnl_percent'])):
                        target_key = f"scaling_target_hit:{pos_id}:{i}"
                        if pnl_pct >= target['pnl_percent'] and not await redis.exists(target_key):
                            vol_to_close = initial_vol * target['close_fraction']
                            final_vol_to_close = round(vol_to_close, vol_scale)
                            if vol_scale == 0: final_vol_to_close = int(final_vol_to_close)

                            reason = f"Scaling Out (Цель #{i + 1}: {target['pnl_percent']}%)"
                            if await close_position(pos, auth_token, reason, close_vol=final_vol_to_close):
                                await redis.set(target_key, 1, ex=86400)  # Помечаем цель как достигнутую
                                target_hit = True
                            break  # Выходим из цикла по целям, т.к. размер позиции изменился
                    if target_hit:
                        continue  # Переходим к следующей позиции в общем списке

                # --- 4. Жёсткий Stop-Loss: всегда активен, независимо от трейлинга ---
                stop_loss_pct = settings.get('EXCHANGE_STOP_LOSS_PERCENT')
                if stop_loss_pct and pnl_pct <= -abs(stop_loss_pct):
                    reason = f"Stop Loss ({pnl_pct:.2f}%)"
                    if await close_position(pos, auth_token, reason):
                        # Кулдаун после убытка, очистка следов
                        loss_cooldown = settings.get('LOSS_COOLDOWN_SECONDS', 900)
                        await redis.set(f"cooldown:{symbol}", 1, ex=loss_cooldown)
                        await asyncio.gather(
                            redis.delete(peak_price_key),
                            redis.delete(initial_spread_key),
                            redis.delete(initial_vol_key),
                        )
                        continue  # Позиция закрыта — к следующей

                # --- 5. Trailing Stop: только после активации в плюсе ---
                use_trailing = settings.get('USE_TRAILING_STOP', False)
                activation_pct = settings.get('TRAILING_ACTIVATION_PERCENT', 0.5)
                if use_trailing and pnl_pct >= activation_pct:
                    trailing_pct = settings.get('TRAILING_PERCENT', 0.2)
                    peak_price_str = await redis.get(peak_price_key)
                    peak_price = float(peak_price_str) if peak_price_str else entry_price

                    if pos_type == 1:  # LONG
                        if current_price > peak_price:
                            peak_price = current_price
                            await redis.set(peak_price_key, peak_price)
                        trigger_price = peak_price * (1 - trailing_pct / 100)
                        trigger_hit = current_price <= trigger_price
                    else:  # SHORT
                        if current_price < peak_price:
                            peak_price = current_price
                            await redis.set(peak_price_key, peak_price)
                        trigger_price = peak_price * (1 + trailing_pct / 100)
                        trigger_hit = current_price >= trigger_price

                    if trigger_hit:
                        reason = f"Trailing Stop (откат {trailing_pct}% от пика {peak_price})"
                        if await close_position(pos, auth_token, reason):
                            await asyncio.gather(
                                redis.delete(peak_price_key),
                                redis.delete(initial_spread_key),
                                redis.delete(initial_vol_key),
                            )
                            continue

                # --- 6. Тайм-аут позиции: выполняется независимо от трейлинга ---
                max_pos_hours = settings.get('MAX_POSITION_HOURS')
                if max_pos_hours:
                    age_hours = (time.time() * 1000 - open_time_ms) / 3_600_000
                    if age_hours > max_pos_hours:
                        reason = "Таймаут позиции"
                        if await close_position(pos, auth_token, reason):
                            timeout_cooldown = settings.get('TIMEOUT_COOLDOWN_SECONDS', 600)
                            await redis.set(f"cooldown:{symbol}", 1, ex=timeout_cooldown)
                            logger.warning(f"Для {symbol} установлен кулдаун на {timeout_cooldown}с после таймаута.")
                            await asyncio.gather(
                                redis.delete(peak_price_key),
                                redis.delete(initial_spread_key),
                                redis.delete(initial_vol_key),
                            )
                            continue

            await asyncio.sleep(interval)

        except Exception as e:
            logger.error(f"Критическая ошибка в цикле мониторинга позиций: {e}", exc_info=True)
            await asyncio.sleep(30)
