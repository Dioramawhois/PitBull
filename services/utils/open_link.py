# где-то рядом с импортами
from contextlib import asynccontextmanager

from loguru import logger

from services.browser import EngineBrowser


def build_mexc_url(pair: str) -> str:
    return f"https://www.mexc.com/exchange/{pair}"


def build_dex_url(chain: str, pair_address: str) -> str:
    ch = chain.strip().lower()
    addr = pair_address.strip()
    return f"https://dextools.com/{ch}/{addr}"


def collect_order_urls(order_info: dict) -> dict:
    mexc_symbol = (order_info.get("mexc_symbol") or "").strip()
    quote_symbol = (order_info.get("quote_symbol") or "USDT").strip()
    dex_chain = (order_info.get("dex_chain") or "").strip()
    pair_address = (order_info.get("pair_address") or "").strip()

    urls = {}
    if mexc_symbol:
        urls["mexc_url"] = build_mexc_url(mexc_symbol)
    if dex_chain and pair_address:
        urls["gmgn_url"] = build_dex_url(dex_chain, pair_address)  # положим в gmgn_url, т.к. класс так ожидает
    return urls


@asynccontextmanager
async def browser_ctx():
    # единый конфиг, без авторизации, без фокусов
    async with EngineBrowser(
        profile_name="TradeOps",
        headless=False,                 # если хочешь тихо в фоне, поставь True
        navigation_timeout_ms=45_000,
        ignore_https_errors=False,
        force_chrome_channel=False,
        sound_path=None,
    ) as eb:
        yield eb


async def open_pair_links(order_info: dict) -> None:
    urls = collect_order_urls(order_info)
    if not urls:
        logger.warning("Нечего открывать: ни MEXC, ни DEX URL не собраны из order_info=%r", order_info)
        return
    # EngineBrowser ожидает структуру Order
    order_payload = {"mexc_url": urls.get("mexc_url", ""), "gmgn_url": urls.get("gmgn_url", "")}
    async with browser_ctx() as eb:
        await eb.open_order(order_payload, gmgn_is_needed=True, keep_open_seconds=None)
        # keep_open_seconds=None и headless=False: окно живет, пока пользователь не закроет.
        # Автозакрытие движка при ручном закрытии уже реализовано внутри EngineBrowser.
