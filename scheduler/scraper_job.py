import asyncio
import re
import json
from datetime import datetime
from urllib.parse import urljoin, urlparse
import hashlib
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
from scheduler.scraper_into_job import identify_detail_selectors, fetch_and_extract_details

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
    logger.info(f"Starting scrape for {url}")

    html = await retry(_fetch_and_render, url)
    if not html:
        return

    sels = await retry(identify_selectors, html)
    if not sels:
        return

    base_rows = await retry(fetch_and_extract, url, sels)
    if not base_rows:
        return

    ts = datetime.utcnow()
    merged: dict[str, dict] = {}

    for row in base_rows:
        job_url = row.get("job_url")
        if not job_url:
            continue

        detail_html = await retry(_fetch_and_render, job_url)
        detail_sels = await identify_detail_selectors(detail_html) if detail_html else None

        details = {}
        if detail_sels:
            details = await fetch_and_extract_details(job_url, detail_sels) or {}

        record = {
            **row,
            "search_vector": f"{row['job_title']} {row['job_location']}".lower(),
            "scraped_at": ts,
            "source_url": url,
            "salary": details.get("salary", "unknown"),
            "description_html": details.get("description_html", ""),
            "description_class": details.get("description_class", ""),
            "job_location_detail": details.get("job_location_detail"),
        }

        uid_src = f"{record.get('job_url','')}|{record.get('job_title','')}|{record.get('job_location','')}"
        uid = hashlib.sha256(uid_src.encode("utf-8")).hexdigest()
        record["uid"] = uid

        merged[uid] = record

    existing = {
        doc["uid"]: doc
        async for doc in collection.find({"source_url": url})
    }

    for uid, doc in merged.items():
        if uid in existing:
            orig = {k: v for k, v in existing[uid].items() if k != "_id"}
            if doc != orig:
                await collection.update_one(
                    {"uid": uid},
                    {"$set": doc}
                )
                logger.info(f"Updated: {doc['job_title']}")
        else:
            await collection.insert_one(doc)
            logger.info(f"Inserted new: {doc['job_title']}")

    stale = set(existing) - set(merged)
    for uid in stale:
        old = existing[uid]
        await collection.delete_one({"_id": old["_id"]})
        logger.info(f"Deleted stale: {old.get('job_title')}")

async def scrape_all():
    if not TARGET_URLS:
        logger.info("URL list empty, skipping scrape_all")
        return
    for u in list(TARGET_URLS):
        await scrape_url(u)

async def periodic_scrape_loop():
    while True:
        if TARGET_URLS:
            await scrape_all()
            logger.info("Scrape cycle complete, sleeping 24h")
            await asyncio.sleep(24 * 3600)
        else:
            logger.info("No URLs yet, waiting for first URL…")
            await asyncio.sleep(60)


async def ensure_collection():
    existing = await db.list_collection_names()
    if MONGO_COLLECTION not in existing:
        await db.create_collection(MONGO_COLLECTION)
        logger.info(f"Created collection {MONGO_COLLECTION}")

    await collection.create_index(
        [("uid", 1)],
        unique=True,
        name="idx_uid"
    )
    await collection.create_index(
        [("search_vector", "text")],
        name="idx_text_search"
    )
    logger.info("Indexes ensured: idx_job_source, idx_text_search")