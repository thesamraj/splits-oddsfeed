import itertools
import json
import logging
import os
from datetime import datetime, timedelta
from typing import List, Optional, Dict

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
import uvicorn


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

app = FastAPI(title="Proxy Manager", version="0.1.0")


class ProxyResponse(BaseModel):
    proxy: str


class ReleaseRequest(BaseModel):
    proxy: str
    book: Optional[str] = None
    region: Optional[str] = None
    reason: Optional[str] = None


class ProxyManager:
    def __init__(self):
        # Load proxies from gitignored file
        self.proxies: List[str] = self._load_proxies()
        self.proxy_cycle = itertools.cycle(self.proxies)
        self.banned_proxies: Dict[str, datetime] = {}
        self.ban_duration = timedelta(minutes=5)

    def _load_proxies(self) -> List[str]:
        """Load proxies from proxies.json file."""
        proxies_file = "proxies.json"
        if os.path.exists(proxies_file):
            try:
                with open(proxies_file, "r") as f:
                    data = json.load(f)
                    if isinstance(data, list):
                        logger.info(f"Loaded {len(data)} proxies from {proxies_file}")
                        return data
                    else:
                        logger.warning(
                            f"Invalid format in {proxies_file}, using defaults"
                        )
            except Exception as e:
                logger.error(f"Error loading {proxies_file}: {e}, using defaults")

        # Default proxies if file not found or invalid
        defaults = [
            "http://proxy1:pass1@proxy1.example.com:8080",
            "http://proxy2:pass2@proxy2.example.com:8080",
        ]
        logger.warning(
            f"Using default proxies. Create {proxies_file} for custom proxies."
        )
        return defaults

    def _is_proxy_banned(self, proxy: str) -> bool:
        """Check if proxy is currently banned."""
        if proxy in self.banned_proxies:
            ban_expires = self.banned_proxies[proxy]
            if datetime.now() > ban_expires:
                # Ban expired, remove from banned list
                del self.banned_proxies[proxy]
                return False
            return True
        return False

    def get_next_proxy(self) -> Optional[str]:
        """Get next available (non-banned) proxy."""
        available_proxies = [p for p in self.proxies if not self._is_proxy_banned(p)]
        if not available_proxies:
            logger.warning("No available proxies (all banned)")
            return None

        # Find next proxy in cycle that's not banned
        for _ in range(len(self.proxies) * 2):  # Avoid infinite loop
            proxy = next(self.proxy_cycle)
            if not self._is_proxy_banned(proxy):
                return proxy

        # Fallback: return first available
        return available_proxies[0] if available_proxies else None

    def ban_proxy(self, proxy: str, reason: str = "unknown"):
        """Ban a proxy for the configured duration."""
        ban_until = datetime.now() + self.ban_duration
        self.banned_proxies[proxy] = ban_until
        logger.info(
            f"Banned proxy {proxy[:20]}... for {self.ban_duration} (reason: {reason})"
        )


proxy_manager = ProxyManager()


@app.get("/lease", response_model=ProxyResponse)
async def lease_proxy(
    book: Optional[str] = Query(None, description="Sportsbook name"),
    region: Optional[str] = Query(None, description="Geographic region"),
):
    try:
        proxy_url = proxy_manager.get_next_proxy()
        if not proxy_url:
            raise HTTPException(status_code=503, detail="No available proxies")

        logger.info(
            f"Leasing proxy for book={book}, region={region}: {proxy_url[:20]}..."
        )
        return ProxyResponse(proxy=proxy_url)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to lease proxy: {e}")
        raise HTTPException(status_code=500, detail="Failed to lease proxy")


@app.post("/release")
async def release_proxy(request: ReleaseRequest):
    try:
        reason = request.reason or "unknown"
        proxy_manager.ban_proxy(request.proxy, reason)
        logger.info(
            f"Released proxy for book={request.book}, region={request.region}, reason={reason}"
        )
        return {"ok": True}
    except Exception as e:
        logger.error(f"Failed to release proxy: {e}")
        raise HTTPException(status_code=500, detail="Failed to release proxy")


@app.get("/health")
async def health_check():
    return {"status": "ok", "available_proxies": len(proxy_manager.proxies)}


if __name__ == "__main__":
    uvicorn.run("proxy_manager.main:app", host="0.0.0.0", port=8099, reload=False)
