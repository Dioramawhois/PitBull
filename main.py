import asyncio
import sys

import aiohttp
from loguru import logger

import fast_api
import local_signal_generator
import order_process
import position_monitor
import token_services

tokens_info = {}
task_status = {"running": False, "paused": False, 'restart_futures_websocket': False}
orders_queue = asyncio.Queue()

logger.remove()
logger.add(sys.stderr, level="INFO")


async def main():
    global tokens_info, task_status
    print("Загрузка актуальной информации о контрактах с MEXC...")
    contracts_data = await token_services.get_all_mexc_contracts_info()
    if not contracts_data:
        logger.error("Не удалось загрузить данные о контрактах с MEXC. Проверьте интернет-соединение.")
        raise SystemExit("Завершение работы из-за ошибки загрузки данных.")
    else:
        logger.info(f"С MEXC загружена информация о {len(contracts_data)} контрактах.")
    tokens_info = await token_services.read_file_async('tokens_info_dict.json') or {}
    logger.info(f"Загружено {len(tokens_info)} токенов из локального файла.")
    for symbol, contract_details in contracts_data.items():
        if symbol in tokens_info:
            tokens_info[symbol]['priceScale'] = contract_details.get('priceScale', 8)
            tokens_info[symbol]['volScale'] = contract_details.get('volScale', 0)
            tokens_info[symbol]['contractSize'] = contract_details.get('contractSize', 1.0)
            tokens_info[symbol]['maxLeverage'] = contract_details.get('maxLeverage', 20)
        else:
            tokens_info[symbol] = {
                'mexc_symbol': symbol,
                'base_coin_name': contract_details.get('baseCoin', symbol.replace('_USDT', '')),
                'priceScale': contract_details.get('priceScale', 8),
                'volScale': contract_details.get('volScale', 0),
                'contractSize': contract_details.get('contractSize', 1.0),
                'maxLeverage': contract_details.get('maxLeverage', 20),  # <-- И ЭТУ
                'is_ignored': True,
                'is_normik': False,
                'custom_percent': None,
                'max_margin': None
            }

    await token_services.save_file_async('tokens_info_dict.json', token_services.strict_token_field(tokens_info))
    logger.info(f"Информация о токенах обновлена и сохранена. Всего токенов: {len(tokens_info)}")

    try:
        logger.info("Попытка обновить список токенов с удаленного сервера (может не работать локально)...")
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=5)) as session:
            async with session.get('http://193.124.114.27:3000/all_tokens_info'):
                pass
    except Exception as e:
        logger.warning(f"Не удалось обновить токены с удаленного сервера (это нормально для локального режима): {e}")
    
    signal_task = asyncio.create_task(local_signal_generator.start_polling(orders_queue, tokens_info))
    process_task = asyncio.create_task(order_process.start_process(tokens_info, task_status, orders_queue))
    monitor_task = asyncio.create_task(position_monitor.start_monitoring(tokens_info))
    api_task = asyncio.create_task(fast_api.serve(tokens_info, task_status))

    await asyncio.gather(signal_task, process_task, monitor_task, api_task)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Приложение остановлено.")