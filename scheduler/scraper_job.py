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
    logger.info(f"Starting scrape for {url}")
    site_key = urlparse(url).netloc

    html = await retry(_fetch_and_render, url)
    if not html:
        return

    sel_doc = await selectors_collection.find_one({"site_url": site_key})
    if sel_doc and all(k in sel_doc for k in ("item_container","job_title","job_location","job_url")):
        sels = {
            "item_container": sel_doc["item_container"],
            "job_title":      sel_doc["job_title"],
            "job_location":   sel_doc["job_location"],
            "job_url":        sel_doc["job_url"],
        }
    else:
        sels = await retry(identify_selectors, html)
        if not sels:
            return
        await selectors_collection.update_one(
            {"site_url": site_key},
            {"$set": {
                "site_url":       site_key,
                **sels
            }},
            upsert=True
        )

    base_rows = await retry(fetch_and_extract, url, sels)
    if not base_rows:
        logger.warning("List-page extraction failed, re-identifying selectors")
        sels = await retry(identify_selectors, html)
        if not sels:
            return
        await selectors_collection.update_one(
            {"site_url": site_key},
            {"$set": {
                **sels
            }}
        )
        base_rows = await retry(fetch_and_extract, url, sels)
        if not base_rows:
            return

    ts = datetime.utcnow()
    merged: dict[str, dict] = {}

    for row in base_rows:
        job_url = row.get("job_url")
        if not job_url:
            continue

        detail_sel_doc = await selectors_collection.find_one({"site_url": site_key})
        desc_sels = detail_sel_doc.get("description") if detail_sel_doc else None

        if not desc_sels:
            detail_html = await retry(_fetch_and_render, job_url)
            desc_sels = await identify_detail_selectors(detail_html) if detail_html else None
            if desc_sels:
                await selectors_collection.update_one(
                    {"site_url": site_key},
                    {"$set": {"description": desc_sels["description"]}},
                    upsert=True
                )

        details = {}
        if desc_sels:
            details = await fetch_and_extract_details(job_url, desc_sels) or {}

        record = {
            **row,
            "search_vector":       f"{row['job_title']} {row['job_location']}".lower(),
            "scraped_at":          ts,
            "source_url":          url,
            "description_html":    details.get("description_html", ""),
            "description_class":   details.get("description_class", "")
        }
        uid_src = f"{record['job_url']}|{record['job_title']}|{record['job_location']}"
        record["uid"] = hashlib.sha256(uid_src.encode()).hexdigest()
        merged[record["uid"]] = record

    existing = {
        doc["uid"]: doc
        async for doc in collection.find({"source_url": url})
    }

    for uid, doc in merged.items():
        if uid in existing:
            orig = {k: v for k, v in existing[uid].items() if k != "_id"}
            if doc != orig:
                await collection.update_one({"uid": uid}, {"$set": doc})
                logger.info(f"Updated: {doc['job_title']}")
        else:
            await collection.insert_one(doc)
            logger.info(f"Inserted new: {doc['job_title']}")

    for uid in set(existing) - set(merged):
        await collection.delete_one({"uid": uid})
        logger.info(f"Deleted stale: {existing[uid]['job_title']}")

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