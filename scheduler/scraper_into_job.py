# scheduler/scraper_into_job.py

import asyncio
import re
import json
from urllib.parse import urljoin, urlparse
from loguru import logger
from playwright.async_api import Page, async_playwright, TimeoutError as PlaywrightTimeoutError
import anthropic
from motor.motor_asyncio import AsyncIOMotorClient
from typing import List, Dict
from bs4 import BeautifulSoup
from config import (
    WS_ENDPOINT,
    ANTHROPIC_API_KEY,
    MONGO_URL, MONGO_DB, MONGO_COLLECTION,
    HTML_SPLIT_COUNT
)
from scheduler.utils import (retry, _fetch_and_render, 
                             split_html)#, with_fresh_session)
 #, keep_session_alive Если куплена подписка на browserless, то можно использовать keep_session_alive
from scheduler.rules_for_jobs_url import SYSTEM_RULES_DETAIL, USER_PROMPT_DETAIL


mongo_client = AsyncIOMotorClient(MONGO_URL)
db           = mongo_client[MONGO_DB]
collection   = db[MONGO_COLLECTION]

async def identify_detail_selectors(html: str) -> list[str] | None:
    try:
        filtered = re.sub(r'<(script|style|header|footer)[\s\S]*?</\1>', '', html)
        parts = split_html(filtered, HTML_SPLIT_COUNT)

        selectors: list[str] = []
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

        for idx, part in enumerate(parts, start=1):
            prompt = USER_PROMPT_DETAIL.format(
                system_rules=SYSTEM_RULES_DETAIL,
                part_index=idx,
                total_parts=HTML_SPLIT_COUNT,
                html=part
            )
            resp = await asyncio.to_thread(
                client.messages.create,
                model="claude-3-5-sonnet-20241022",
                max_tokens=200,
                temperature=0,
                system=[{"type":"text","text":SYSTEM_RULES_DETAIL}],
                messages=[{"role":"user","content":prompt}]
            )
            text = resp.content[0].text.strip()
            try:
                sel_map = json.loads(text)
            except json.JSONDecodeError:
                logger.warning(f"Detail part {idx}/{HTML_SPLIT_COUNT}: invalid JSON, skipping.")
                continue
            if isinstance(sel_map.get("description"), list):
                selectors.extend(sel_map["description"])

        seen = set()
        unique = [s for s in selectors if not (s in seen or seen.add(s))]
        return unique if unique else None

    except Exception as e:
        logger.exception(f"identify_detail_selectors error: {e}")
        return None

async def fetch_and_extract_details(url: str, selectors: list[str]) -> dict:
    html = await retry(_fetch_and_render, url)
    result = {"description_html": "", "description_class": ""}
    if not html:
        return result

    soup = BeautifulSoup(html, "html.parser")
    for sel in selectors or []:
        el = soup.select_one(sel)
        if el:
            result["description_html"] = str(el)
            result["description_class"] = " ".join(el.get("class", [])) or ""
            break
    return result

async def scrape_job_details():
    async for doc in collection.find({}):
        job_url = doc.get("job_url")
        if not job_url:
            continue

        logger.info(f"Fetching description for {job_url}")
        html = await retry(_fetch_and_render, job_url)
        if not html:
            continue

        selectors = await identify_detail_selectors(html)
        if not selectors:
            continue

        details = await fetch_and_extract_details(job_url, selectors)
        if not details["description_html"]:
            continue

        await collection.update_one(
            {"_id": doc["_id"]},
            {"$set": details}
        )
        logger.info(f"Updated description for {job_url}")



async def run_details_job():
    await scrape_job_details()


"""async def _extract_details(page: Page, url: str, selectors: list[str]) -> dict:
    result = {"description_html": "", "description_class": ""}
    try:
        #await page.goto(url, wait_until="domcontentloaded")
        #await page.wait_for_load_state("networkidle", timeout=120_000)
        #await keep_session_alive(page, timeout_ms=60000)  # Разкоментить и чуть чуть поменять если куплена подписка на browserless
        await page.goto(url)
    except PlaywrightTimeoutError:
        logger.warning(f"Timeout при загрузке detail {url}, используем частичный DOM")
    except Exception as e:
        logger.warning(f"Ошибка навигации к detail {url}: {e!r}, используем частичный DOM")

    for sel in selectors or []:
        try:
            node = await page.query_selector(sel)
            if node:
                result["description_html"] = await node.inner_html()
                result["description_class"] = (await node.get_attribute("class")) or ""
                break
        except Exception as e:
            logger.warning(f"Error extracting description with selector {sel} on {url}: {e!r}")

    return result

async def fetch_and_extract_details(url: str, selectors: list[str]) -> dict:
    return await with_fresh_session(_extract_details, url, selectors)
    
async def scrape_job_details():
    async for doc in collection.find({}):
        job_url = doc.get("job_url")
        if not job_url:
            continue

        logger.info(f"Fetching description for {job_url}")
        html = await retry(_fetch_and_render, job_url)
        if not html:
            continue

        selectors = await identify_detail_selectors(html)
        if not selectors:
            continue

        details = await fetch_and_extract_details(job_url, selectors)
        if not details["description_html"]:
            continue

        await collection.update_one(
            {"_id": doc["_id"]},
            {"$set": details}
        )
        logger.info(f"✔ Updated description for {job_url}")"""