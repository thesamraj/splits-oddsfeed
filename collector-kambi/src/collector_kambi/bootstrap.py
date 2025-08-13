import json
import os
from pathlib import Path
from typing import Dict, Optional, Any


def load_bootstrap() -> Optional[Dict[str, Any]]:
    """Load Playwright-captured cookies and headers for bot evasion."""
    path = Path(os.getenv("KAMBI_BOOTSTRAP_PATH", "artifacts/kambi_bootstrap.json"))
    if not path.exists():
        return None

    try:
        data = json.loads(path.read_text())
        cookies = data.get("cookies", [])
        seed_headers = data.get("seed_headers") or {}

        # Convert Playwright cookies to curl_cffi format
        cookie_jar = {}
        for cookie in cookies:
            cookie_jar[cookie["name"]] = cookie["value"]

        # Filter headers for common ones that work well with curl_cffi
        filtered_headers = {}
        for k, v in seed_headers.items():
            k_lower = k.lower()
            if k_lower in [
                "accept",
                "accept-language",
                "referer",
                "origin",
                "cache-control",
                "pragma",
                "dnt",
            ]:
                filtered_headers[k] = v

        return {"cookies": cookie_jar, "headers": filtered_headers, "raw_data": data}
    except Exception:
        return None
