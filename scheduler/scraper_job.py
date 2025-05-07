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
    MONGO_SELECTORS_COLLECTION
)
from rules import SYSTEM_RULES, USER_PROMPT_TEMPLATE
from scheduler.scraper_into_job import identify_detail_selectors, fetch_and_extract_details
from scheduler.utils import retry, _fetch_and_render
TARGET_URLS = set()


mongo_client = AsyncIOMotorClient(MONGO_URL)
db           = mongo_client[MONGO_DB]
collection   = db[MONGO_COLLECTION]
selectors_collection  = db[MONGO_SELECTORS_COLLECTION]


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
    html = await retry(_fetch_and_render, url)
    if not html:
        return []

    p = await async_playwright().start()
    browser = await p.chromium.connect(WS_ENDPOINT)
    page = await browser.new_page()
    page.set_default_navigation_timeout(120_000)
    await page.set_content(html, wait_until="domcontentloaded")
    await page.wait_for_load_state("networkidle", timeout=120_000)

    items = []
    css_container = selectors["item_container"][0]
    elements = await page.query_selector_all(css_container)
    if not elements:
        cls = css_container.lstrip('.')
        fallback = f'[class*="{cls}"]'
        elements = await page.query_selector_all(fallback)
        logger.warning(f"No elements for '{css_container}', fallback to '{fallback}' found {len(elements)}")

    else:
        logger.info(f"Found {len(elements)} elements for item_container '{css_container}'")

    for el in elements:
        row = {"job_title": "", "job_location": "", "job_url": "", "source_url": url}

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

        for sel in selectors.get("job_url", []):
            node = await el.query_selector(sel)
            if not node:
                continue
            tag = (await (await node.get_property("tagName")).json_value()).lower()
            href = ""
            if tag == "a":
                href = await node.get_attribute("href") or ""
            else:
                link = await node.query_selector("a")
                href = await link.get_attribute("href") if link else ""
            if href:
                pu = urlparse(href)
                if not pu.scheme and not pu.netloc:
                    href = urljoin(url, href)
                row["job_url"] = href
                break

        if row["job_title"] or row["job_url"]:
            items.append(row)

    await browser.close()
    await p.stop()
    logger.info(f"Extracted {len(items)} rows from {url}")
    return items



async def scrape_url(url: str):
    try:
        site_key = urlparse(url).netloc
        logger.info(f"=== Scraping list page: {url}")
        html = await retry(_fetch_and_render, url)
        if not html:
            logger.warning("Не удалось получить HTML списка вакансий")
            return

        sel_doc = await selectors_collection.find_one({"site_url": site_key})
        if sel_doc and all(k in sel_doc for k in ("item_container","job_title","job_location","job_url")):
            list_sels = {
                "item_container": sel_doc["item_container"],
                "job_title":      sel_doc["job_title"],
                "job_location":   sel_doc["job_location"],
                "job_url":        sel_doc["job_url"],
            }
        else:
            list_sels = await retry(identify_selectors, html)
            if not list_sels:
                logger.error("Failed to identify list-page selectors")
                return
            await selectors_collection.update_one(
                {"site_url": site_key},
                {"$set": {"site_url": site_key, **list_sels}},
                upsert=True
            )
            logger.info(f"Saved list selectors for {site_key}: {list_sels}")

        base_rows = await retry(fetch_and_extract, url, list_sels)
        if not base_rows:
            logger.warning("List-page extraction failed on first attempt, retrying selectors")
            list_sels = await retry(identify_selectors, html)
            if not list_sels:
                return
            await selectors_collection.update_one(
                {"site_url": site_key},
                {"$set": list_sels}
            )
            base_rows = await retry(fetch_and_extract, url, list_sels)
            if not base_rows:
                logger.error("List-page extraction ultimately failed")
                return

        sel_doc = await selectors_collection.find_one({"site_url": site_key})
        desc_sels = sel_doc.get("description") if sel_doc else None

        if not desc_sels and base_rows:
            first_job_url = base_rows[0].get("job_url")
            if first_job_url:
                detail_html = await retry(_fetch_and_render, first_job_url)
                if detail_html:
                    candidate = await identify_detail_selectors(detail_html)
                    await selectors_collection.update_one(
                        {"site_url": site_key},
                        {"$set": {"description": candidate}},
                        upsert=True
                    )
                    desc_sels = candidate
                    logger.info(f"Saved detail selectors for {site_key}: {desc_sels}")
                else:
                    logger.warning("Failed to fetch detail page for selector identification")

        ts = datetime.utcnow()
        results = []
        for row in base_rows:
            job_url = row.get("job_url")
            details = {}
            if desc_sels:
                details = await fetch_and_extract_details(job_url, desc_sels) or {}
            merged = {
                **row,
                **details,
                "scraped_at": ts,
                "site": site_key,
            }
            results.append(merged)

        if results:
            await collection.insert_many(results)
            logger.info(f"Inserted {len(results)} jobs for {site_key}")

    except Exception:
        logger.exception(f"Critical error in scrape_url for {url}")

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

    if MONGO_SELECTORS_COLLECTION not in existing:
        await db.create_collection(MONGO_SELECTORS_COLLECTION)
        logger.info(f"Created collection {MONGO_SELECTORS_COLLECTION}")

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