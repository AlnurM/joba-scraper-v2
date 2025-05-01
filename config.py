import os

JWT_SECRET           = os.getenv("JWT_SECRET", "change-me")
MONGO_URL            = os.getenv("MONGODB_URL", "mongodb://localhost:27017")
MONGO_DB             = os.getenv("MONGODB_DB", "scraper_db")
MONGO_COLLECTION     = os.getenv("MONGODB_COLLECTION", "jobs")

# ScraperAPI token
SCRAPERAPI_KEY       = os.getenv("SCRAPERAPI_KEY", "")

# Browserless + Playwright
BROWSERLESS_TOKEN    = os.getenv("BROWSERLESS_TOKEN", "")
ANTHROPIC_API_KEY    = os.getenv("ANTHROPIC_API_KEY", "")
WS_ENDPOINT          = (
    f"wss://browserless-production-d8e3.up.railway.app/chromium/playwright"
    f"?token={BROWSERLESS_TOKEN}"
)

RETRY_COUNT          = 5
TARGET_BASE          = os.getenv("TARGET_BASE", "")

# HTTP service port
PORT                 = int(os.getenv("PORT", 8000))