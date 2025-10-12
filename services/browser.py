import asyncio
from loguru import logger
import os
import platform
import shutil
import signal
from pathlib import Path
from typing import TypedDict, Iterable, Optional
from urllib.parse import urlparse

from playwright.async_api import async_playwright, TimeoutError as PWTimeoutError


# ----------------------- typing -----------------------

class Order(TypedDict, total=False):
    mexc_url: str
    gmgn_url: str



class EngineBrowser:
    """
    Асинхронный одноразовый/многоразовый лаунчер движка (Playwright Chromium)
    с persistent-профилем и удобными методами открытия ссылок.

    Фишка: движок автоматически завершится, если пользователь вручную закроет все окна.
    """

    def __init__(
        self,
        profile_name: str = "Default",
        *,
        headless: bool = True,
        user_data_root: Optional[str | Path] = None,
        navigation_timeout_ms: int = 20_000,
        sound_path: Optional[str] = '',
        ignore_https_errors: bool = False,
        force_chrome_channel: bool = False,
    ) -> None:
        self.profile_name = profile_name
        self.headless = headless
        self.user_data_root = Path(user_data_root) if user_data_root else Path(
            os.getenv("XDG_CACHE_HOME", Path.home() / ".cache")
        ) / "engine-profiles"
        self.navigation_timeout_ms = navigation_timeout_ms
        self.sound_path = sound_path
        self.ignore_https_errors = ignore_https_errors
        self.force_chrome_channel = force_chrome_channel

        self._pw = None              # playwright instance
        self._context = None         # persistent browser context
        self._user_data_dir = self.user_data_root / self.profile_name

        self._closing_lock = asyncio.Lock()
        self._closed = False

    # ---------- async context management ----------

    async def __aenter__(self) -> "EngineBrowser":
        await self._ensure_profile_dir()
        self._pw = await async_playwright().start()

        launch_kwargs = dict(
            user_data_dir=str(self._user_data_dir),
            headless=self.headless,
            ignore_https_errors=self.ignore_https_errors,
            args=[
                "--no-default-browser-check",
                "--disable-dev-shm-usage",
                "--no-first-run",
                "--disable-background-networking",
                "--disable-renderer-backgrounding",
            ],
        )
        if self.force_chrome_channel:
            launch_kwargs["channel"] = "chrome"

        try:
            self._context = await self._pw.chromium.launch_persistent_context(**launch_kwargs)
        except Exception as exc:
            if not self.force_chrome_channel:
                logger.warning("Chromium launch failed (%s). Retrying with channel=chrome.", exc)
                launch_kwargs["channel"] = "chrome"
                self._context = await self._pw.chromium.launch_persistent_context(**launch_kwargs)
            else:
                await self._safe_stop_pw()
                raise

        # Подписки на уровне контекста
        self._context.on("close", self._on_context_closed)
        self._context.on("page", self._on_new_page)

        # Уже открытые страницы (иногда стартуют сервисные)
        for page in self._context.pages:
            self._attach_page_listeners(page)

        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    # ---------- public API ----------

    async def open_order(
        self,
        order: Order,
        *,
        gmgn_is_needed: bool = True,
        keep_open_seconds: Optional[float] = None,   # если None и headless=False — ждём Ctrl+C
    ) -> None:
        urls = self._collect_urls(order, gmgn_is_needed=gmgn_is_needed)
        if not urls:
            raise ValueError("В order нет валидных URL. Нужны mexc_url и/или gmgn_url.")

        asyncio.create_task(self._play_sound_async(self.sound_path))
        await self._open_urls(urls)

        # если GUI — держим окно
        if not self.headless:
            if keep_open_seconds is None:
                logger.info("GUI открыт. Нажми Ctrl+C, чтобы закрыть.")
                await self._wait_forever_until_sigint()
            else:
                await asyncio.sleep(keep_open_seconds)

    async def close(self) -> None:
        # Идемпатентное закрытие
        async with self._closing_lock:
            if self._closed:
                return
            self._closed = True

            try:
                if self._context:
                    try:
                        await self._context.close()
                    finally:
                        self._context = None
            finally:
                await self._safe_stop_pw()

    # ---------- internals ----------

    async def _ensure_profile_dir(self) -> None:
        self.user_data_root.mkdir(parents=True, exist_ok=True)
        self._user_data_dir.mkdir(parents=True, exist_ok=True)

    def _collect_urls(self, order: Order, *, gmgn_is_needed: bool) -> list[str]:
        urls: list[str] = []
        mexc = (order.get("mexc_url") or "").strip()
        gmgn = (order.get("gmgn_url") or "").strip()
        if self._is_url(mexc):
            urls.append(mexc)
        if gmgn_is_needed and self._is_url(gmgn):
            urls.append(gmgn)
        return urls

    async def _open_urls(self, urls: Iterable[str]) -> None:
        if not self._context:
            raise RuntimeError("Контекст браузера не запущен. Используй 'async with EngineBrowser(...)'.")

        pages = [await self._context.new_page() for _ in urls]
        for page in pages:
            self._attach_page_listeners(page)

        await asyncio.gather(*(self._goto_with_logs(p, u) for p, u in zip(pages, urls)))

    async def _goto_with_logs(self, page, url: str) -> None:
        try:
            page.set_default_navigation_timeout(self.navigation_timeout_ms)
            await page.goto(url, timeout=self.navigation_timeout_ms, wait_until="domcontentloaded")
            logger.info(f"Opened {url}")
        except PWTimeoutError:
            logger.warning("Timeout opening %s", url)
        except Exception as exc:
            logger.warning("Failed to open %s: %s", url, exc)

    # ---------- auto-shutdown on manual window close ----------

    def _attach_page_listeners(self, page) -> None:
        # Используем lambda -> create_task, чтобы не блокировать событийный цикл Playwright
        page.on("close", lambda *_: asyncio.create_task(self._on_page_closed()))
        page.on("crash", lambda *_: asyncio.create_task(self._on_page_closed()))

    def _on_new_page(self, page) -> None:
        self._attach_page_listeners(page)

    async def _on_page_closed(self) -> None:
        # Даем Playwrightу завершить удаление страницы из self._context.pages
        await asyncio.sleep(0)
        if self._context is None:
            return
        try:
            remaining = len(self._context.pages)
        except Exception:
            remaining = 0

        if remaining == 0:
            logger.info("Все окна закрыты пользователем. Завершаем движок.")
            await self.close()

    def _on_context_closed(self, *_):
        # Контекст закрыт пользователем или системой; инициируем общее закрытие
        asyncio.create_task(self.close())

    # ---------- misc ----------

    @staticmethod
    def _is_url(value: Optional[str]) -> bool:
        if not value:
            return False
        try:
            p = urlparse(value)
            return p.scheme in ("http", "https") and bool(p.netloc)
        except Exception:
            return False

    async def _wait_forever_until_sigint(self) -> None:
        stop = asyncio.Event()

        def _handler(*_):  # noqa: ANN001
            stop.set()

        try:
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGINT, signal.SIGTERM):
                try:
                    loop.add_signal_handler(sig, _handler)
                except NotImplementedError:
                    pass
        except RuntimeError:
            pass

        try:
            await stop.wait()
        except KeyboardInterrupt:
            pass

    async def _play_sound_async(self, sound_path: Optional[str]) -> None:
        if not sound_path:
            return
        p = Path(sound_path)
        if not p.exists():
            return

        try:
            if platform.system() == "Windows":
                import winsound  # type: ignore

                def _play():
                    winsound.PlaySound(str(p), winsound.SND_FILENAME | winsound.SND_ASYNC)
                await asyncio.to_thread(_play)
                return

            if platform.system() == "Darwin":
                if shutil.which("afplay"):
                    proc = await asyncio.create_subprocess_exec("afplay", str(p))
                    asyncio.create_task(proc.communicate())
                    return

            for player in ("paplay", "aplay", "play"):
                if shutil.which(player):
                    proc = await asyncio.create_subprocess_exec(player, str(p))
                    asyncio.create_task(proc.communicate())
                    return
        except Exception:
            # звук — приятный бонус, не причина падать
            pass

    async def _safe_stop_pw(self) -> None:
        try:
            if self._pw:
                await self._pw.stop()
        finally:
            self._pw = None


# ----------------------- demo ----------------------

if __name__ == "__main__":
    demo_order: Order = {
        "mexc_url": "https://example.com/mexc",
        "gmgn_url": "https://example.com/gmgn",
    }

    async def main() -> None:
        async with EngineBrowser(
            profile_name="TradeOps",
            headless=False,
            navigation_timeout_ms=45_000,
            ignore_https_errors=False,
            force_chrome_channel=False,
            sound_path="sounds/alert.mp3",
        ) as eb:
            await eb.open_order(demo_order, gmgn_is_needed=True, keep_open_seconds=None)

    asyncio.run(main())
