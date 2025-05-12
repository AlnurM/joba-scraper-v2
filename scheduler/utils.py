from config import RETRY_COUNT, WS_ENDPOINT, SCRAPERAPI_KEY
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from loguru import logger
import asyncio
import httpx
from playwright.async_api import Page


async def retry(func, *args, retries=RETRY_COUNT, **kwargs):

    from httpx import ReadTimeout

    last_exc = None
    for i in range(1, retries + 1):
        try:
            return await func(*args, **kwargs)
        except PlaywrightTimeoutError as e:
            logger.warning(f"Stage timeout ({i}/{retries}): {e}")
            last_exc = e
        except ReadTimeout as e:
            logger.warning(f"HTTP timeout ({i}/{retries}): {e}")
            last_exc = e
        except Exception as e:
            logger.exception(f"Unexpected error (won't retry): {e}")
            last_exc = e
            break
        # небольшая пауза перед следующей попыткой
        await asyncio.sleep(1)
    logger.error(f"Skipping after {retries} retries, last error: {last_exc}")
    return None



async def _fetch_and_render(url: str) -> str:
    api_url = f"http://api.scraperapi.com?api_key={SCRAPERAPI_KEY}&url={url}"
    async with httpx.AsyncClient(timeout=120) as client:
        resp = await client.get(api_url)
        logger.info(f"ScraperAPI status: {resp.status_code}")
        resp.raise_for_status()
        html = resp.text

    p = await async_playwright().start()
    browser = await p.chromium.connect(WS_ENDPOINT)
    context = await browser.new_context()
    page = await context.new_page()
    page.set_default_navigation_timeout(60_000)

    try:
        await page.goto(url, wait_until="domcontentloaded")
        await page.wait_for_load_state("networkidle", timeout=60_000)
        await keep_session_alive(page, timeout_ms=60000)
    except PlaywrightTimeoutError:
        logger.warning(f"Playwright timeout on {url}, grabbing partial content")

    final_html = await page.content()

    await browser.close()
    await p.stop()

    return final_html



def split_html(html: str, parts: int) -> list[str]:
    length = len(html)
    chunk_size = length // parts
    slices: list[str] = []
    for i in range(parts):
        start = i * chunk_size
        end = (i + 1) * chunk_size if i < parts - 1 else length
        slices.append(html[start:end])
    return slices


async def keep_session_alive(page: Page, timeout_ms: int = 60000):
    cdp_session = await page.context.new_cdp_session(page)
    await cdp_session.send("Browserless.reconnect", {"timeout": timeout_ms})