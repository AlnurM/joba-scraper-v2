# scheduler/scraper_into_job.py

import asyncio
import re
import json
from urllib.parse import urljoin, urlparse
from loguru import logger
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
import anthropic
from motor.motor_asyncio import AsyncIOMotorClient

from config import (
    WS_ENDPOINT,
    ANTHROPIC_API_KEY,
    MONGO_URL, MONGO_DB, MONGO_COLLECTION,
    HTML_SPLIT_COUNT
)
from scheduler.utils import retry, _fetch_and_render, split_html, keep_session_alive
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
                html=part,
                part_index=idx,
                total_parts=HTML_SPLIT_COUNT
            )
            resp = await asyncio.to_thread(
                client.messages.create,
                model="claude-3-5-sonnet-20241022",
                max_tokens=200,
                temperature=0,
                system=[{"type":"text","text":SYSTEM_RULES_DETAIL}],
                messages=[{"role":"user","content":prompt}]
            )
            sel_map = json.loads(resp.content[0].text.strip())
            selectors.extend(sel_map.get("description", []))

        seen = set()
        return [s for s in selectors if not (s in seen or seen.add(s))]

    except Exception as e:
        logger.exception(f"identify_detail_selectors error: {e}")
        return None

async def fetch_and_extract_details(url: str, selectors: list[str]) -> dict:
    result = {"description_html": "", "description_class": ""}
    html = await retry(_fetch_and_render, url)
    if not html:
        return result

    p = await async_playwright().start()
    browser = await p.chromium.connect(WS_ENDPOINT)
    page = await browser.new_page()
    page.set_default_navigation_timeout(120_000)
    await page.set_content(html, wait_until="domcontentloaded")
    await page.wait_for_load_state("networkidle", timeout=120_000)
    await keep_session_alive(page, timeout_ms=60000)

    for sel in selectors:
        if not sel:
            continue
        try:
            node = await page.query_selector(sel)
            if node:
                result["description_html"] = await node.inner_html()
                result["description_class"] = (await node.get_attribute("class")) or ""
                break
        except PlaywrightTimeoutError:
            logger.warning(f"Timeout extracting description with selector {sel} on {url}")

    await browser.close()
    await p.stop()
    return result

async def scrape_job_details():
    async for doc in collection.find({}):
        job_url = doc.get("job_url")
        if not job_url:
            continue

        logger.info(f"👉 Fetching description for {job_url}")
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
        logger.info(f"✔ Updated description for {job_url}")
async def run_details_job():
    await scrape_job_details()