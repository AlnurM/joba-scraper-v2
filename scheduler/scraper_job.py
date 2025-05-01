import asyncio
import re
import json
from datetime import datetime
from urllib.parse import urljoin, urlparse

import httpx
from loguru import logger
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
import anthropic
from motor.motor_asyncio import AsyncIOMotorClient

from config import (
    SCRAPERAPI_KEY, WS_ENDPOINT,
    ANTHROPIC_API_KEY,
    MONGO_URL, MONGO_DB, MONGO_COLLECTION,
    RETRY_COUNT
)
from rules import SYSTEM_RULES, USER_PROMPT_TEMPLATE


TARGET_URLS = set()


mongo_client = AsyncIOMotorClient(MONGO_URL)
db           = mongo_client[MONGO_DB]
collection   = db[MONGO_COLLECTION]

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
    except PlaywrightTimeoutError:
        logger.warning(f"Playwright timeout on {url}, grabbing partial content")

    final_html = await page.content()

    await browser.close()
    await p.stop()

    return final_html


async def fetch_html(url: str) -> str | None:
    return await retry(_fetch_and_render, url)

async def identify_selectors(html: str) -> dict | None:
    try:
        filtered = re.sub(r'<(script|style|header|footer)[\s\S]*?</\1>', '', html)
        prompt = USER_PROMPT_TEMPLATE.format(system_rules=SYSTEM_RULES, html=filtered)
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        resp = await asyncio.to_thread(
            client.messages.create,
            model="claude-3-5-sonnet-20241022",
            max_tokens=500,
            temperature=0,
            system=[{"type":"text","text":SYSTEM_RULES}],
            messages=[{"role":"user","content":prompt}]
        )
        sel_map = json.loads(resp.content[0].text.strip())
        sel_map.pop("department_title", None)
        return sel_map
    except Exception as e:
        logger.exception(f"identify_selectors error: {e}")
        return None

async def fetch_and_extract(url: str, selectors: dict) -> list[dict]:
    api_url = f"http://api.scraperapi.com?api_key={SCRAPERAPI_KEY}&url={url}"
    async with httpx.AsyncClient(timeout=120) as client:
        resp = await client.get(api_url)
        resp.raise_for_status()
        raw_html = resp.text

    p = await async_playwright().start()
    browser = await p.chromium.connect(WS_ENDPOINT)
    page = await browser.new_page()
    page.set_default_navigation_timeout(120_000)

    await page.set_content(raw_html, wait_until="domcontentloaded")
    await page.wait_for_load_state("networkidle", timeout=120_000)

    items = []
    css_container = selectors["item_container"][0]
    elements = await page.query_selector_all(css_container)
    for el in elements:
        row = {"job_title": "", "job_location": "", "job_url": "", "source_url": url}
        # title
        for sel in selectors["job_title"]:
            node = await el.query_selector(sel)
            if node:
                row["job_title"] = (await node.inner_text()).strip()
                break
        # location
        for sel in selectors["job_location"]:
            node = await el.query_selector(sel)
            if node:
                row["job_location"] = (await node.inner_text()).strip()
                break
        # url
        for sel in selectors["job_url"]:
            node = await el.query_selector(sel)
            if not node:
                continue
            tag = (await (await node.get_property("tagName")).json_value()).lower()
            if tag == "a":
                href = await node.get_attribute("href") or ""
            else:
                a = await el.query_selector("a")
                href = await a.get_attribute("href") if a else ""
            if href:
                p_u = urlparse(href)
                if not p_u.scheme and not p_u.netloc:
                    href = urljoin(url, href)
                row["job_url"] = href
                break
        items.append(row)

    await browser.close()
    await p.stop()
    return items


async def scrape_url(url: str):
    logger.info(f"Scraping {url}")
    html = await retry(_fetch_and_render, url)
    if not html:
        return
    sels = await identify_selectors(html)
    if not sels:
        return
    data = await retry(fetch_and_extract, url, sels)
    if not data:
        return
    ts = datetime.utcnow()
    for row in data:
        row["scraped_at"] = ts
        row["source_url"] = url
        try:
            await collection.insert_one(row)
            logger.info(f"Inserted: {row['job_title']}")
        except Exception as e:
            logger.exception(f"Mongo insert failed: {e}")

async def scrape_all():
    if not TARGET_URLS:
        logger.info("URL list empty, skipping scrape_all")
        return
    for u in list(TARGET_URLS):
        await scrape_url(u)

async def periodic_scrape_loop():
    while True:
        if TARGET_URLS:
            # есть что сканировать — запустить полный цикл и лечь спать 24 ч.
            await scrape_all()
            logger.info("Scrape cycle complete, sleeping 24h")
            await asyncio.sleep(24 * 3600)
        else:
            # пока нет URL — ждём появления первой задачи
            logger.info("No URLs yet, waiting for first URL…")
            await asyncio.sleep(10)


async def ensure_collection():
    existing = await db.list_collection_names()
    if MONGO_COLLECTION not in existing:
        await db.create_collection(MONGO_COLLECTION)
        logger.info(f"Created collection {MONGO_COLLECTION}")