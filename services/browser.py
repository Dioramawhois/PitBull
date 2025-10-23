import asyncio
import multiprocessing as mp
import time
import webbrowser
from typing import Iterable, Optional, TypedDict
from urllib.parse import urlparse

from loguru import logger

__all__ = ("EngineBrowser", "Order", "open_links")


class Order(TypedDict, total=False):
    mexc_url: str
    gmgn_url: str


def _is_url(value: Optional[str]) -> bool:
    if not value:
        return False
    parsed = urlparse(value)
    return parsed.scheme in ("http", "https") and bool(parsed.netloc)


def _open_urls_worker(
    urls: list[str], browser_name: Optional[str], delay: float
) -> None:
    try:
        controller = webbrowser.get(browser_name) if browser_name else webbrowser.get()
    except webbrowser.Error:
        controller = webbrowser.get()

    for idx, url in enumerate(urls):
        if idx == 0:
            controller.open(url, new=2)
        else:
            controller.open_new_tab(url)
        if idx + 1 < len(urls) and delay > 0:
            time.sleep(delay)


class EngineBrowser:
    """
    Минимальный сервис для открытия ссылок в существующем браузере.

    URL открываются в отдельном процессе, чтобы не блокировать основной код.
    Первый адрес открывается как новая вкладка/окно, остальные — как вкладки.
    """

    def __init__(
        self, *, browser: Optional[str] = None, delay_between_tabs: float = 0.2
    ) -> None:
        self.browser = browser
        self.delay_between_tabs = delay_between_tabs

    async def __aenter__(self) -> "EngineBrowser":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:  # noqa: ANN001
        await self.close()

    async def open_order(
        self, order: Order | dict, *, gmgn_is_needed: bool = True
    ) -> None:
        urls = self._collect_urls(order, gmgn_is_needed=gmgn_is_needed)
        if not urls:
            raise ValueError("В order нет валидных URL. Нужны mexc_url и/или gmgn_url.")
        await self.open_urls(urls)

    async def open_urls(self, urls: Iterable[str]) -> None:
        urls_list = list(urls)
        valid_urls = [url for url in urls_list if _is_url(url)]
        if not valid_urls:
            logger.warning("Ни одного валидного URL для открытия: %s", urls_list)
            return
        logger.info("Открываю ссылки в браузере: %s", valid_urls)
        self._spawn_process(valid_urls)
        await asyncio.sleep(0)

    async def close(self) -> None:
        # Метод оставлен для совместимости с предыдущей реализацией.
        return None

    def _collect_urls(self, order: Order | dict, *, gmgn_is_needed: bool) -> list[str]:
        urls: list[str] = []
        mexc = (order.get("mexc_url") or "").strip()
        gmgn = (order.get("gmgn_url") or "").strip()
        if _is_url(mexc):
            urls.append(mexc)
        if gmgn_is_needed and _is_url(gmgn):
            urls.append(gmgn)
        return urls

    def _spawn_process(self, urls: list[str]) -> None:
        process = mp.Process(
            target=_open_urls_worker,
            args=(urls, self.browser, self.delay_between_tabs),
            daemon=True,
        )
        process.start()


async def open_links(
    urls: Iterable[str],
    *,
    browser: Optional[str] = None,
    delay_between_tabs: float = 0.2,
) -> None:
    async with EngineBrowser(
        browser=browser, delay_between_tabs=delay_between_tabs
    ) as eb:
        await eb.open_urls(urls)
