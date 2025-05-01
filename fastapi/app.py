import asyncio
from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, AnyUrl
from loguru import logger
import jwt

from config import JWT_SECRET
from scheduler.scraper_job import TARGET_URLS, periodic_scrape_loop, ensure_collection, scrape_all

app = FastAPI()
security = HTTPBearer()

class UrlsRequest(BaseModel):
    urls: list[AnyUrl]
    force: bool = False


async def verify_token(creds: HTTPAuthorizationCredentials = Depends(security)):
    token = creds.credentials
    try:
        jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
    except jwt.InvalidTokenError:
        logger.error("Invalid JWT token")
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

@app.on_event("startup")
async def on_startup():
    await ensure_collection()
    asyncio.create_task(periodic_scrape_loop())
    logger.info("Startup complete: periodic scraper running")

@app.post("/add_urls/", dependencies=[Depends(verify_token)])
async def add_urls(req: UrlsRequest):
    added = []
    for u in req.urls:
        s = str(u)
        if s not in TARGET_URLS:
            TARGET_URLS.add(s)
            added.append(s)
            logger.info(f"Added URL: {s}")
    # если нужен форсированный запуск — запускаем scrape_all отдельно
    if req.force:
        logger.info("Force run triggered, starting immediate scrape_all")
        asyncio.create_task(scrape_all())
    return {"added": added, "all_urls": list(TARGET_URLS)}