import itertools
import logging
from typing import List, Optional

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
import uvicorn


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

app = FastAPI(title="Proxy Manager", version="0.1.0")


class ProxyLease(BaseModel):
    proxy_url: str
    lease_seconds: int


class ProxyManager:
    def __init__(self):
        self.proxies: List[str] = [
            "http://proxy1:pass1@proxy1.example.com:8080",
            "http://proxy2:pass2@proxy2.example.com:8080",
            "http://proxy3:pass3@proxy3.example.com:8080",
            "http://proxy4:pass4@proxy4.example.com:8080",
        ]
        self.proxy_cycle = itertools.cycle(self.proxies)

    def get_next_proxy(self) -> str:
        return next(self.proxy_cycle)


proxy_manager = ProxyManager()


@app.get("/lease", response_model=ProxyLease)
async def lease_proxy(
    book: Optional[str] = Query(None, description="Sportsbook name"),
    region: Optional[str] = Query(None, description="Geographic region"),
):
    try:
        proxy_url = proxy_manager.get_next_proxy()
        logger.info(f"Leasing proxy for book={book}, region={region}: {proxy_url}")

        return ProxyLease(proxy_url=proxy_url, lease_seconds=300)
    except Exception as e:
        logger.error(f"Failed to lease proxy: {e}")
        raise HTTPException(status_code=500, detail="Failed to lease proxy")


@app.get("/health")
async def health_check():
    return {"status": "ok", "available_proxies": len(proxy_manager.proxies)}


if __name__ == "__main__":
    uvicorn.run("proxy_manager.main:app", host="0.0.0.0", port=8099, reload=False)
