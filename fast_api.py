import asyncio
import logging
import os
from typing import Any, Dict, List, Optional

import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from redis.asyncio import from_url

from redis_repo import TokensRepo
from token_services import read_file_async, save_file_async, strict_token_field

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

TOKENS_JSON = "tokens_info_dict.json"
TOKENS_LOCK = asyncio.Lock()
logger = logging.getLogger(__name__)

tokens_info: Dict[str, Any] = {}
task_status: Dict[str, Any] = {}

app = FastAPI(
    title="Control API",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
    allow_credentials=True,
)

class TokenInfo(BaseModel):
    mexc_symbol: str
    base_coin_name: str
    custom_percent: Optional[float] = Field(None, ge=0)
    is_ignored: bool = True
    is_normik: bool = False
    max_margin: Optional[int] = Field(None, ge=0)

class TokenUpdate(BaseModel):
    custom_percent: Optional[float] = Field(None, ge=0)
    is_ignored: Optional[bool] = None
    is_normik: Optional[bool] = None
    max_margin: Optional[int] = Field(None, ge=0)

def set_context(_tokens_info, _task_status):
    global tokens_info, task_status
    tokens_info = _tokens_info
    task_status = _task_status


async def serve(_tokens_info, _task_status):
    set_context(_tokens_info, _task_status)
    host = os.getenv("API_HOST", "127.0.0.1")
    port = int(os.getenv("API_PORT", "4332"))

    config = uvicorn.Config(app, host=host, port=port, log_level="info", reload=False)
    server = uvicorn.Server(config)
    logger.info(f"FastAPI (Control) сервер запущен на http://{host}:{port}")
    await server.serve()


@app.on_event("startup")
async def load_tokens_on_startup():
    """
    Эта функция теперь не читает данные, а СИНХРОНИЗИРУЕТ Redis с уже
    загруженными и обновленными данными из main.py.
    """
    global tokens_info, repo

    logger.info("FastAPI startup: Инициализация подключения к Redis...")
    r = from_url(REDIS_URL, decode_responses=False)
    repo = TokensRepo(r)

    if tokens_info:
        logger.info(f"Синхронизация {len(tokens_info)} токенов с Redis...")
        try:
            token_list = list(tokens_info.values())
            await repo.upsert_many(token_list)
            logger.info("Redis успешно синхронизирован с актуальными данными о токенах.")
        except Exception as e:
            logger.error(f"Ошибка при синхронизации токенов с Redis: {e}", exc_info=True)
    else:
        logger.warning("Словарь tokens_info пуст на момент старта FastAPI. Redis не обновлен.")


@app.get("/tokens", response_model=List[TokenInfo])
async def list_tokens():
    return [TokenInfo(**t) for t in tokens_info.values()]

@app.post("/replace_tokens")
async def replace_tokens(new_tokens: dict):
    async with TOKENS_LOCK:
        tokens_info.clear()
        tokens_info.update(new_tokens or {})
        await repo.upsert_many(tokens_info.values())
        await save_file_async(TOKENS_JSON, strict_token_field(tokens_info))
    return {"ok": True, "count": len(tokens_info)}

@app.get("/tokens/{symbol}", response_model=TokenInfo)
async def get_token(symbol: str):
    sym = symbol.upper()
    item = tokens_info.get(sym)
    if not item:
        raise HTTPException(status_code=404, detail="token not found")
    return TokenInfo(**item)

@app.patch("/tokens/{symbol}", response_model=TokenInfo, tags=["Tokens"])
async def patch_token(symbol: str, patch: TokenUpdate):
    sym = symbol.upper()
    async with TOKENS_LOCK:
        cur = tokens_info.get(sym)
        if not cur:
            raise HTTPException(status_code=404, detail="token not found")
        for k, v in patch.model_dump(exclude_unset=True).items():
            cur[k] = v
        await repo.upsert(cur)
        await save_file_async(TOKENS_JSON, strict_token_field(tokens_info))
        return TokenInfo(**cur)


class BulkUpdateItem(BaseModel):
    symbol: str
    data: TokenUpdate

@app.patch("/tokens:bulk", tags=["Tokens"])
async def bulk_patch(items: List[BulkUpdateItem]):
    updated = 0
    async with TOKENS_LOCK:
        for it in items:
            sym = it.symbol.upper()
            cur = tokens_info.get(sym)
            if not cur:
                continue
            for k, v in it.data.model_dump(exclude_unset=True).items():
                cur[k] = v
            await repo.upsert(cur)
            updated += 1
        await save_file_async(TOKENS_JSON, strict_token_field(tokens_info))
    return {"ok": True, "updated": updated}

@app.post("/reload_tokens")
async def reload_tokens():
    data = await read_file_async(TOKENS_JSON)
    if not isinstance(data, dict):
        raise HTTPException(status_code=500, detail="cannot read tokens json")
    async with TOKENS_LOCK:
        tokens_info.clear()
        tokens_info.update(data)
        await repo.upsert_many(tokens_info.values())
    return {"ok": True, "count": len(tokens_info)}

@app.post("/start-compare", tags=["Compare Task"])
async def start_compare_endpoint():
    global background_task, task_status, tokens_info

    if task_status['paused'] is False:
        raise HTTPException(status_code=400, detail="Compare task is not on pause")

    task_status["paused"] = False
    return {"message": "Compare task started."}

    
@app.post("/stop-compare", tags=["Compare Task"])
async def stop_compare_endpoint():
    global background_task, task_status

    if task_status["paused"]:
        raise HTTPException(status_code=400, detail="Compare task is on pause")

    task_status["paused"] = True
    return {"message": "Compare task paused."}


@app.post("/clear_in_process", tags=["Compare Task"])
async def clear_in_process():

    for tokens in tokens_info.values():
        if tokens.get('order_in_process', False):
            tokens['order_in_process'] = False
    msg = "Cleared in process"
    logger.info(msg)
    return {'msg': msg}


@app.post("/ignore_token/{symbol}", tags=["Tokens Interaction"])
async def ignore_token(symbol: str):
    sym = symbol.upper()
    token = tokens_info.get(sym)
    if not token:
        return {'msg': f"Cannot find token with provided symbol - {sym}"}

    if token.get("is_ignored", False):
        return {'msg': f"Token is already ignored - {sym}"}

    token['is_ignored'] = True

    filtered_data = strict_token_field(tokens_info)
    await save_file_async('tokens_info_dict.json', filtered_data)
    logger.info(f"{sym} now is ignored")
    return {'msg': f"{sym} now is ignored"}


@app.post("/unignore_token/{symbol}", tags=["Tokens Interaction"])
async def unignore_token(symbol: str):
    global task_status
    sym = symbol.upper()
    token = tokens_info.get(sym)
    if not token:
        return {'msg': f"Cannot find token with provided symbol - {sym}"}

    task_status['restart_futures_websocket'] = True
    if not token.get("is_ignored", True):
        return {'msg': f"Token is not ignored - {sym}"}

    token['is_ignored'] = False
    filtered_data = strict_token_field(tokens_info)
    await save_file_async('tokens_info_dict.json', filtered_data)
    logger.info(f"{sym} now is not ignored")
    return {'msg': f"{sym} now is not ignored"}


@app.post("/set_max_margin/{token_symbol}/{margin}", tags=["Tokens Interaction"])
async def set_max_margin(token_symbol: str, margin: float):

    token = next((t for t in tokens_info.values() if t['base_coin_name'] == token_symbol), None)
    if not token:
        return {'msg': f"Cannot find token with provided symbol - {token_symbol}"}

    token['max_margin'] = margin

    filtered_data = strict_token_field(tokens_info)
    await save_file_async('tokens_info_dict.json', filtered_data)
    logger.info(f"{token_symbol} now obtain max margin - {margin}")
    return {'msg': f"{token_symbol} now obtain max margin - {margin}"}


@app.post("/unset_normik_pair/{token_symbol}", tags=["Tokens Interaction"])
async def unset_normik_pair(token_symbol: str):

    token = next((t for t in tokens_info.values() if t['base_coin_name'] == token_symbol), None)
    if not token:
        return {'msg': f"Cannot find token with provided symbol - {token_symbol}"}

    if token.get("is_normik", False) is False:
        return {'msg': f"Token is not normik - {token_symbol}"}

    token['is_normik'] = False
    filtered_data = strict_token_field(tokens_info)
    await save_file_async('tokens_info_dict.json', filtered_data)
    logger.info(f"{token_symbol} now is not normik")
    return {'msg': f"{token_symbol} now is not normik"}


@app.post("/set_custom_percent/{pair_symbol}/{percent}", tags=["Tokens Interaction"])
async def set_custom_percent(pair_symbol: str, percent: str):

    if pair_symbol not in tokens_info:
        return {'msg': f"Cannot find pair with provided symbol - {pair_symbol}"}

    token = tokens_info[pair_symbol]

    try:
        percent_int = int(percent)
        custom_percent = token.get('custom_percent', None)
        if custom_percent and custom_percent == percent_int:
            return {'msg': f"{pair_symbol} already obtain this percent: {percent}"}

        if percent_int == 0:
            token['custom_percent'] = None
        else:
            token['custom_percent'] = percent_int
    except ValueError:
        return {'msg': f"Invalid percent pair_symbol - {percent}"}

    filtered_data = strict_token_field(tokens_info)
    await save_file_async('tokens_info_dict.json', filtered_data)
    logger.info(f"{pair_symbol} now obtain custom percent: {percent}")
    return {'msg': f"{pair_symbol} now obtain custom percent: {percent}"}


@app.get("/active_cooldowns", response_model=List[Dict[str, Any]], tags=["Tokens Information"])
async def get_active_cooldowns():
    """
    Просмотр тоукенов которые находятся в кеше Redis.
    """
    global repo
    if not repo:
        raise HTTPException(status_code=500, detail="Redis repo not initialized")

    cooldowns = []
    # Skenujeme klíče v Redis, které odpovídají našemu vzoru 'cooldown:*'
    # Používáme scan místo keys, aby nedošlo k blokování Redis při velkém množství klíčů
    async for key_bytes in repo.r.scan_iter("cooldown:*"):
        # Декодируем байтовый ключ в строку
        key_str = key_bytes.decode('utf-8')

        ttl = await repo.r.ttl(key_bytes)
        symbol = key_str.split(":", 1)[1]

        cooldowns.append({
            "symbol": symbol,
            "cooldown_seconds_remaining": ttl
        })

    logger.info(f"Načteno {len(cooldowns)} aktivních cooldownů z Redis.")
    return cooldowns


@app.post("/clear_cooldown/{symbol}", tags=["Tokens Interaction"], summary="Ручной сброс кулдауна для токена по символу")
async def clear_cooldown(symbol: str):
    global repo
    if not repo:
        raise HTTPException(status_code=500, detail="Redis repo not initialized")

    sym = symbol.upper()
    cooldown_key = f"cooldown:{sym}"

    deleted_count = await repo.r.delete(cooldown_key)

    if deleted_count > 0:
        logger.info(f"Ручной сброс кулдауна для токена: {sym}")
        return {"ok": True, "message": f"Cooldown for {sym} has been cleared."}
    else:
        logger.info(f"Попытка сброса кулдауна для {sym}, но активный кулдаун не найден.")
        return {"ok": True, "message": f"No active cooldown found for {sym}."}



@app.post("/clear_cooldown/all", tags=["Tokens Interaction"], summary="Сброс кулдаунов для всех токенов")
async def clear_cooldown_all():
    global repo
    if not repo:
        raise HTTPException(status_code=500, detail="Redis repo not initialized")

    pattern = "cooldown:*"
    keys_to_delete = [key async for key in repo.r.scan_iter(pattern)]

    if not keys_to_delete:
        logger.info("Попытка очистить кулдауны, но активных кулдаунов не найдено.")
        return {"ok": True, "deleted": 0, "message": "No active cooldowns found."}

    deleted = await repo.r.delete(*keys_to_delete)

    symbols = [key.decode("utf-8").split(":", 1)[1] for key in keys_to_delete if b":" in key]
    logger.info(
        "Сброс кулдаунов для %d токенов: %s",
        deleted,
        ", ".join(symbols) if symbols else "symbols unavailable",
    )
    return {"ok": True, "deleted": deleted, "message": "All cooldowns have been cleared."}


@app.post("/set_max_margin/{token_symbol}/{margin}", tags=["Tokens Interaction"])
async def set_normik_pair(token_symbol: str, margin: float):

    token = next((t for t in tokens_info.values() if t['base_coin_name'] == token_symbol), None)
    if not token:
        return {'msg': f"Cannot find token with provided symbol - {token_symbol}"}

    token['max_margin'] = margin

    filtered_data = strict_token_field(tokens_info)
    await save_file_async('tokens_info_dict.json', filtered_data)
    logger.info(f"{token_symbol} now obtain max margin - {margin}")
    return {'msg': f"{token_symbol} now obtain max margin - {margin}"}



@app.get("/ignored_pairs", response_model=List[str], tags=["Tokens Information"])
async def ignored_pairs():
    return [token['mexc_symbol'] for token in tokens_info.values() if token.get('is_ignored', True)]


@app.get("/unignored_pairs", response_model=List[str], tags=["Tokens Information"])
async def unignored_pairs():
    return [token['mexc_symbol'] for token in tokens_info.values() if token.get('is_ignored', True) is False]


@app.get("/normik_pairs",  tags=["Tokens Information"])
async def normik_pairs():
    pairs = []
    for mexc_symbol, token in tokens_info.items():
        if token.get('is_normik', False):
            is_ignored = token.get("is_ignored", None)
            is_normik = token.get("is_normik", None)

            pairs.append({
                "mexc_symbol": mexc_symbol,
                "base_coin_name": token['base_coin_name'],
                "is_ignored": is_ignored,
                "is_normik": is_normik,
            })

    return pairs
@app.get("/not_normik_pairs",  tags=["Tokens Information"])
async def not_normik_pairs():
    pairs = []
    for mexc_symbol, token in tokens_info.items():
        if token.get('is_ignored', True) is False and token.get('is_normik', False) is False:
            is_ignored = token.get("is_ignored", None)
            is_normik = token.get("is_normik", None)

            pairs.append({
                "mexc_symbol": mexc_symbol,
                "base_coin_name": token['base_coin_name'],
                "is_ignored": is_ignored,
                "is_normik": is_normik,
            })

    return pairs

@app.get("/token_info/{token_symbol}", response_model=dict, tags=["Tokens Information"])
async def pair_info(token_symbol: str):
    token = next((t for t in tokens_info.values() if t['base_coin_name'] == token_symbol), None)

    if not token:
        return {'msg': f"Cannot find pair with provided symbol - {token_symbol}"}

    return token


