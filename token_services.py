import json
import logging
import os
from pathlib import Path

import aiohttp

# 1. Импортируем наш глобальный "регулировщик"
from rate_limiter import limiter

try:
    import aiofiles
except ImportError:
    aiofiles = None

BASE_DIR = Path(__file__).resolve().parent
logger = logging.getLogger(__name__)

def resolve_path(path: str | os.PathLike) -> Path:
    p = Path(path)
    return p if p.is_absolute() else (BASE_DIR / p)

async def read_file_async(path: str | os.PathLike) -> dict | None:
    p = resolve_path(path)
    if not p.exists():
        return None
    try:
        if aiofiles is None:
            with p.open("r", encoding="utf-8") as f:
                return json.load(f)
        async with aiofiles.open(p, "r", encoding="utf-8") as f:
            raw = await f.read()
        return json.loads(raw)
    except json.JSONDecodeError as e:
        logger.error(f"[read_file_async] Invalid JSON in {p}: {e}")
        return None

async def save_file_async(path: str | os.PathLike, data) -> None:
    p = resolve_path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    txt = json.dumps(data, ensure_ascii=False, indent=4)
    if aiofiles is None:
        with tmp.open("w", encoding="utf-8") as f:
            f.write(txt)
        os.replace(tmp, p)
        return
    async with aiofiles.open(tmp, "w", encoding="utf-8") as f:
        await f.write(txt)
    os.replace(tmp, p)

async def save_tokens(tokens_dict):
    await save_file_async("tokens_info_dict.json", tokens_dict)


def strict_token_field(tokens_info):
    filtered_data = {}
    for mexc_symbol, token in tokens_info.items():
        custom_percent = token.get('custom_percent', None)
        is_ignored = token.get('is_ignored', True)
        is_normik = token.get('is_normik', False)
        max_margin = token.get('max_margin')

        stricted_token = {
            'mexc_symbol': mexc_symbol,
            'base_coin_name': token['base_coin_name'],
            'custom_percent': custom_percent,
            'is_ignored': is_ignored,
            'is_normik': is_normik,
            'max_margin': max_margin
        }
        filtered_data[mexc_symbol] = stricted_token
    return filtered_data


async def get_tokens_info():
    async with aiohttp.ClientSession() as session:
        async with session.post('http://193.124.114.27:3000/all_tokens_info') as response:
            if response.status == 200:
                return await response.json()

async def get_all_mexc_contracts_info():
    """Получает информацию о всех фьючерсных контрактах с MEXC."""
    # 2. Ждем "разрешения" от регулировщика
    await limiter.acquire()
    url = "https://futures.mexc.com/api/v1/contract/detail"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=10) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get("success"):
                        return {item['symbol']: item for item in data.get('data', [])}
    except Exception as e:
        logger.error(f"Ошибка при получении данных о контрактах MEXC: {e}")
