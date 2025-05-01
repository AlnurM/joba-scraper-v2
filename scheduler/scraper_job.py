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
    RETRY_COUNT, TARGET_BASE
)
from rules import SYSTEM_RULES, USER_PROMPT_TEMPLATE


TARGET_URLS = set()


mongo_client = AsyncIOMotorClient(MONGO_URL)
db           = mongo_client[MONGO_DB]
collection   = db[MONGO_COLLECTION]

async def retry(func, *args, retries=RETRY_COUNT, **kwargs):
    for i in range(1, retries + 1):
        try:
            return await func(*args, **kwargs)
        except PlaywrightTimeoutError as e:
            logger.warning(f"Stage timeout ({i}/{retries}): {e}")
            if i == retries:
                logger.error("Skipping after max retries")
                return None
        except Exception as e:
            logger.exception(f"Unexpected error: {e}")
            return None

async def _fetch_and_render(url: str) -> str:
    api_url = f"http://api.scraperapi.com?api_key={SCRAPERAPI_KEY}&url={url}"
    async with httpx.AsyncClient(timeout=60) as client:
        resp = await client.get(api_url)
        resp.raise_for_status()
        html = resp.text

    p = await async_playwright().start()
    browser = await p.chromium.connect(WS_ENDPOINT)
    page = await browser.new_page()
    page.set_default_navigation_timeout(120_000)
    await page.set_content(html, wait_until="domcontentloaded")
    await page.wait_for_load_state("networkidle", timeout=120_000)
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

async def extract_data(html: str, selectors: dict) -> list:
    async def _inner():
        p = await async_playwright().start()
        browser = await p.chromium.connect(WS_ENDPOINT)
        page = await browser.new_page()
        await page.set_content(html, wait_until="domcontentloaded")

        items = []
        css = selectors["item_container"][0]
        raw = await page.query_selector_all(css)
        if not raw:
            cls = css.lstrip('.')
            raw = await page.query_selector_all(f'[class*="{cls}"]')

        for el in raw:
            row = {"job_title":"", "job_location":"", "job_url":"", "source_url":""}
            for sel in selectors.get("job_title", []):
                node = await el.query_selector(sel)
                if node:
                    row["job_title"] = (await node.inner_text()).strip()
                    break
            for sel in selectors.get("job_location", []):
                node = await el.query_selector(sel)
                if node:
                    row["job_location"] = (await node.inner_text()).strip()
                    break
            url = ""
            for sel in selectors.get("job_url", []):
                node = await el.query_selector(sel)
                if not node: continue
                tag = (await (await node.get_property("tagName")).json_value()).lower()
                if tag == "a":
                    url = await node.get_attribute("href") or ""
                else:
                    link = await node.query_selector("a")
                    url = await link.get_attribute("href") if link else ""
                break
            if not url:
                tag = (await (await el.get_property("tagName")).json_value()).lower()
                if tag == "a":
                    url = await el.get_attribute("href") or ""
                else:
                    link = await el.query_selector("a")
                    url = await link.get_attribute("href") if link else ""
            if url:
                p_u = urlparse(url)
                if not p_u.scheme and not p_u.netloc:
                    url = urljoin(TARGET_BASE, url)
            row["job_url"] = url
            items.append(row)

        await browser.close()
        await p.stop()
        return items

    res = await retry(_inner)
    return res or []

async def scrape_url(url: str):
    logger.info(f"➡️ Scraping {url}")
    html = await fetch_html(url)
    if not html:
        logger.error("No HTML returned")
        return
    sels = await identify_selectors(html)
    if not sels or not sels.get("item_container"):
        logger.error("No selectors, skipping URL")
        return
    data = await extract_data(html, sels)
    if not data:
        logger.warning("No data found")
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