import os
from urllib.parse import urlparse

try:
    from curl_cffi import requests as cffi_requests
except ImportError:
    import requests as cffi_requests


def build_curl_cffi_session():
    """Build a curl_cffi session with proxy support"""
    session = cffi_requests.Session()

    # Add proxy if configured
    proxy_url = os.getenv("PROXY_URL", "")
    if proxy_url:
        parsed = urlparse(proxy_url)
        if parsed.scheme and parsed.hostname:
            proxies = {"http": proxy_url, "https": proxy_url}
            session.proxies = proxies

    # Set common headers
    session.headers.update(
        {
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://pa.betrivers.com/",
            "Origin": "https://pa.betrivers.com",
            "User-Agent": "Mozilla/5.0 (compatible; KambiBrowser/1.0)",
        }
    )

    return session
