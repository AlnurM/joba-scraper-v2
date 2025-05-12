import asyncio
import re
import json
from datetime import datetime
from urllib.parse import urljoin, urlparse
import hashlib
import httpx
from loguru import logger
from pymongo.errors import BulkWriteError
from playwright.async_api import Page, async_playwright, TimeoutError as PlaywrightTimeoutError
import anthropic
from motor.motor_asyncio import AsyncIOMotorClient
from typing import List, Dict
from config import (
    HTML_SPLIT_COUNT, WS_ENDPOINT,
    ANTHROPIC_API_KEY,
    MONGO_URL, MONGO_DB, MONGO_COLLECTION,
    MONGO_SELECTORS_COLLECTION,
    MAX_PARALLEL_TASKS
)
from rules import SYSTEM_RULES, USER_PROMPT_TEMPLATE
from scheduler.scraper_into_job import identify_detail_selectors, fetch_and_extract_details
from scheduler.utils import (retry, _fetch_and_render, 
                             split_html, with_fresh_session)
 #, keep_session_alive Если куплена подписка на browserless, то можно использовать keep_session_alive
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
        parts = split_html(filtered, HTML_SPLIT_COUNT)

        combined = {k: [] for k in ("item_container","job_title","job_location","job_url")}
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

        for idx, part in enumerate(parts, start=1):
            prompt = USER_PROMPT_TEMPLATE.format(
                system_rules=SYSTEM_RULES,
                part_index=idx,
                total_parts=HTML_SPLIT_COUNT,
                html=part
            )
            resp = await asyncio.to_thread(
                client.messages.create,
                model="claude-3-5-sonnet-20241022",
                max_tokens=500,
                temperature=0,
                system=[{"type":"text","text":SYSTEM_RULES}],
                messages=[{"role":"user","content":prompt}]
            )
            text = resp.content[0].text.strip()
            try:
                sel_map = json.loads(text)
            except json.JSONDecodeError:
                logger.warning(f"Part {idx}/{HTML_SPLIT_COUNT}: invalid JSON, skipping:\n{text[:100]}")
                continue

            for key in combined:
                if key in sel_map and isinstance(sel_map[key], list):
                    combined[key].extend(sel_map[key])

        for key in combined:
            seen = set()
            combined[key] = [s for s in combined[key] if not (s in seen or seen.add(s))]

        if not combined["item_container"]:
            return None

        return combined

    except Exception as e:
        logger.exception(f"identify_selectors error: {e}")
        return None



async def _extract_list(page: Page, url: str, selectors: dict) -> list[dict]:
    try:
        await page.goto(url, wait_until="domcontentloaded")
        await page.wait_for_load_state("networkidle", timeout=120_000)
        #await keep_session_alive(page, timeout_ms=60000)  # Разкоментить и чуть чуть поменять если куплена подписка на browserless
    except PlaywrightTimeoutError:
        logger.warning(f"Timeout при загрузке списка {url}, используем частичный DOM")
    except Exception as e:
        logger.warning(f"Ошибка навигации к списку {url}: {e!r}, используем частичный DOM")

    items = []

    lst = selectors.get("item_container") or []
    if not lst:
        logger.error(f"Нет селектора item_container для {url}, пропускаем.")
        return items
    css_container = lst[0]

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
            try:
                node = await el.query_selector(sel)
                if node:
                    row["job_title"] = (await node.inner_text()).strip()
                    break
            except Exception as e:
                logger.debug(f"Error querying title selector {sel}: {e!r}")

        for sel in selectors.get("job_location", []):
            try:
                node = await el.query_selector(sel)
                if node:
                    row["job_location"] = (await node.inner_text()).strip()
                    break
            except Exception as e:
                logger.debug(f"Error querying location selector {sel}: {e!r}")

        for sel in selectors.get("job_url", []):
            try:
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
            except Exception as e:
                logger.debug(f"Error querying URL selector {sel}: {e!r}")

        if row["job_title"] or row["job_url"]:
            items.append(row)

    return items

async def fetch_and_extract(url, selectors):
    return await with_fresh_session(_extract_list, url, selectors)



async def scrape_url(url: str):
    try:
        site_key = urlparse(url).netloc
        logger.info(f"=== Scraping list page: {url}")

        html = await retry(_fetch_and_render, url)
        if not html:
            logger.warning("Error fetching HTML")
            return

        sel_doc = await selectors_collection.find_one({"site_url": site_key})
        if sel_doc and all(k in sel_doc for k in ("item_container","job_title","job_location","job_url")):
            list_sels = {k: sel_doc[k] for k in ("item_container","job_title","job_location","job_url")}
        else:
            list_sels = await retry(identify_selectors, html)
            if not list_sels:
                logger.error("Error identifying selectors")
                return
            await selectors_collection.update_one(
                {"site_url": site_key},
                {"$set": {"site_url": site_key, **list_sels}},
                upsert=True
            )
            logger.info(f"Saved list selectors for {site_key}: {list_sels}")

        base_rows = await retry(fetch_and_extract, url, list_sels)
        if not base_rows:
            logger.error("Error extracting base rows")
            return

        sel_doc = await selectors_collection.find_one({"site_url": site_key})
        desc_sels = sel_doc.get("description") if sel_doc else None

        if not desc_sels and base_rows:
            first_url = base_rows[0].get("job_url")
            if first_url:
                detail_html = await retry(_fetch_and_render, first_url)
                if detail_html:
                    candidate = await identify_detail_selectors(detail_html) or []
                    await selectors_collection.update_one(
                        {"site_url": site_key},
                        {"$set": {"description": candidate}},
                        upsert=True
                    )
                    desc_sels = candidate
                    logger.info(f"Saved detail selectors for {site_key}: {desc_sels}")
                else:
                    logger.warning("Error fetching detail HTML for first job")

        ts = datetime.utcnow()
        semaphore = asyncio.Semaphore(MAX_PARALLEL_TASKS)
        async def process_row(row: dict) -> dict | None:
            job_url = row.get("job_url")
            if not job_url:
                return None

            details: dict = {}
            if desc_sels:
                await semaphore.acquire()
                try:
                    raw = await fetch_and_extract_details(job_url, desc_sels) or {}
                finally:
                    semaphore.release()

                for k, v in raw.items():
                    if isinstance(v, str):
                        raw[k] = re.sub(r"<[^>]+>", "", v).strip()
                details = raw

            key_obj = {
                "url":      job_url,
                "title":    row.get("job_title"),
                "location": row.get("job_location"),
            }
            key_str = json.dumps(key_obj, sort_keys=True, ensure_ascii=False)
            uid = hashlib.md5(key_str.encode("utf-8")).hexdigest()

            return {
                **row,
                **details,
                "uid":        uid,
                "scraped_at": ts,
                "site":       site_key,
            }

        tasks = [asyncio.create_task(process_row(r)) for r in base_rows]
        results = await asyncio.gather(*tasks)

        docs = [r for r in results if r]
        if docs:
            try:
                await collection.insert_many(docs, ordered=False)
                logger.info(f"Inserted {len(docs)} jobs for {site_key}")
            except BulkWriteError as bwe:
                num_dup = len(bwe.details.get("writeErrors", []))
                logger.warning(f"Bulk write error: пропущено {num_dup} дубликатов uid")

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


