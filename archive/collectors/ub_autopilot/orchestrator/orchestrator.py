#!/usr/bin/env python3
import os
import time
import redis
import subprocess
import json

r = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"))
WIN = int(os.getenv("WINDOW_SEC", "1800"))  # 30 minutes
TARGET_O1 = int(os.getenv("TARGET_O1", "50"))
TARGET_O2 = int(os.getenv("TARGET_O2", "1"))


def api_count():
    try:
        import urllib.request
        import json as j

        with urllib.request.urlopen(
            "http://127.0.0.1:8080/odds?brand=unibet&minutes=15&last=true", timeout=5
        ) as f:
            return j.loads(f.read().decode()).get("count", 0)
    except:
        return 0


def db_odds():
    try:
        cmd = [
            "docker",
            "exec",
            "-i",
            "splits-oddsfeed-store-1",
            "bash",
            "-lc",
            "psql -U ${POSTGRES_USER:-postgres} -d ${POSTGRES_DB:-splits} -Atc \"select count(*) from odds where book='unibet' and ts>=now()-interval '15 min';\"",
        ]
        out = (
            subprocess.check_output(cmd, stderr=subprocess.DEVNULL, timeout=5)
            .decode()
            .strip()
        )
        return int(out or "0")
    except:
        return 0


start = time.time()
mode = "http"
flips = 0
print(
    f"[ORCHESTRATOR] Starting 30-min acceptance test. Targets: O1≥{TARGET_O1}, O2≥{TARGET_O2}"
)

while time.time() - start < WIN:
    o1 = db_odds()
    o2 = api_count()
    print(f"[ORCHESTRATOR] {int(time.time()-start)}s: O1={o1} O2={o2} mode={mode}")

    if o1 >= TARGET_O1 and o2 >= TARGET_O2:
        result = {"o1": o1, "o2": o2, "mode": mode, "success": True}
        open("/tmp/UB_GO", "w").write(json.dumps(result))
        print(f"[ORCHESTRATOR] SUCCESS! Targets met: {result}")
        break

    # simple fallback flip if no growth after ~2 minutes
    if time.time() - start > 120 and o1 < 10 and flips < 1:
        print("[ORCHESTRATOR] Switching to stealth mode due to low HTTP performance")
        try:
            subprocess.run(
                [
                    "docker",
                    "compose",
                    "-f",
                    "docker-compose.yml",
                    "-f",
                    "docker-compose.override.ub-sandbox.yml",
                    "up",
                    "-d",
                    "ub-cdp-collector",
                ],
                check=False,
            )
        except:
            pass
        mode = "stealth"
        flips += 1

    time.sleep(20)

# write summary
final_o1 = db_odds()
final_o2 = api_count()
summary = {
    "o1": final_o1,
    "o2": final_o2,
    "mode": mode,
    "elapsed": int(time.time() - start),
}
open("/tmp/UB_SUMMARY", "w").write(json.dumps(summary))
print(f"[ORCHESTRATOR] Final summary: {summary}")
