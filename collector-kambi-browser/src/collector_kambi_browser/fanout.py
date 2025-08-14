import os
import time
from typing import Iterable
from prometheus_client import Counter
from .bootstrap import build_curl_cffi_session
from .publisher import publish_envelope

k_fanout = Counter("kambi_browser_fanouts_total", "fanouts performed", ["type"])

BASE = os.getenv(
    "KAMBI_BASE_URL", "https://eu.offering-api.kambicdn.com/offering/v2018"
)
BRAND = os.getenv("KAMBI_BRAND", "betrivers")


def rest_fanout(event_ids: Iterable[str]):
    s = build_curl_cffi_session()
    for eid in set(event_ids):
        url = f"{BASE}/{BRAND}/event/{eid}.json"
        try:
            r = s.get(url, timeout=8)
            data = r.json()
            publish_envelope(source="kambi", url=url, status=r.status_code, data=data)
            k_fanout.labels("rest").inc()
            time.sleep(0.2)
        except Exception:
            continue
