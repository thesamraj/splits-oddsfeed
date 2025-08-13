import os
import json
import time
import asyncio
import httpx

proxies_raw = os.environ.get("RAW_PROXIES", "")
proxies = [
    p.strip()
    for p in proxies_raw.splitlines()
    if p.strip() and not p.strip().startswith("#")
]

BASE = os.environ.get("KAMBI_BASE_URL", "https://e0-api.kambi.com/offering/v2018")
brand = os.environ.get("KAMBI_BRAND", "betrivers")
locale = os.environ.get("KAMBI_LOCALE", "en_US")
sport = os.environ.get("KAMBI_SPORT", "american_football")
league = os.environ.get("KAMBI_LEAGUE", "nfl")
kambi_url = f"{BASE}/{brand}/listView/{sport}/{league}/all/matches.json?lang={locale}&market=US&client_id=2&channel_id=1&ncid=1000&useCombined=true"

headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.8",
    "Referer": "https://www.betrivers.com/",
    "Cache-Control": "no-cache",
}

results = []


async def probe(px):
    tout = httpx.Timeout(10, connect=6)
    proxy = {"http://": px, "https://": px}
    ok = False
    ip = None
    status = None
    err = None
    t0 = time.time()
    try:
        async with httpx.AsyncClient(
            timeout=tout, headers=headers, proxies=proxy, follow_redirects=True
        ) as c:
            r1 = await c.get("https://ifconfig.io/ip")
            ip = (
                r1.text.strip()
                if r1.status_code == 200
                else f"ifconfig:{r1.status_code}"
            )
            r2 = await c.get(kambi_url)
            status = r2.status_code
            ok = status == 200 and r2.headers.get("content-type", "").startswith(
                "application/json"
            )
    except Exception as e:
        err = str(e)[:200]
    dt = round((time.time() - t0) * 1000)
    results.append(
        {"proxy": px, "ip": ip, "status": status, "ok": ok, "ms": dt, "err": err}
    )
    print(f"[{px}] -> ip={ip} status={status} ok={ok} ms={dt} err={err}")


async def main():
    print(f"Testing {len(proxies)} proxies against Kambi API...")
    print(f"Target URL: {kambi_url}")
    tasks = [probe(p) for p in proxies]
    if not tasks:
        print("No proxies provided. Edit RAW_PROXIES in the script.")
        return
    await asyncio.gather(*tasks)
    good = [r for r in results if r["ok"]]
    with open("artifacts/kambi_proxy_probe.json", "w") as f:
        json.dump(results, f, indent=2)
    if good:
        with open("proxy-manager/proxies.json", "w") as f:
            json.dump(
                {"proxies": [{"url": g["proxy"], "region": "us"} for g in good]},
                f,
                indent=2,
            )
        print(f"Saved {len(good)} working proxies to proxy-manager/proxies.json")
    else:
        print("No working proxies found. Check providers/regions/creds.")
    print("Wrote detailed results to artifacts/kambi_proxy_probe.json")


if __name__ == "__main__":
    asyncio.run(main())
