import asyncio
import uvicorn
from config import PORT
from api.app import app

async def run_fastapi():
    if asyncio.get_event_loop().is_running():
        config = uvicorn.Config(app, host="0.0.0.0", port=PORT, log_level="info")
        server = uvicorn.Server(config)
        await server.serve()
    else:
        uvicorn.run(app, host="0.0.0.0", port=PORT)

if __name__ == "__main__":
    asyncio.run(run_fastapi())