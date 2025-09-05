import json
import urllib.request
import time

tokens = ["rsi2uspa", "rsi2usil", "rsi2usco", "rsi2usnj", "rsi2usmi"]
results = []

for tok in tokens:
    url = f"https://e1-api.kambi.com/offering/v2018/{tok}/event/live/open.json"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=10) as r:
            body = r.read()
            if r.status == 200 and len(body) > 500:
                results.append(
                    {"token": tok, "status": 200, "bytes": len(body), "working": True}
                )
    except:
        pass
    time.sleep(1)

with open("/tmp/public_scout_results.json", "w") as f:
    json.dump(results, f, indent=2)
print(f"Scout found {len(results)} working tokens")
