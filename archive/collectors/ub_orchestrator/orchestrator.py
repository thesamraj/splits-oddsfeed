#!/usr/bin/env python3
import time
import json
import urllib.request
import subprocess as sp

WIN = 1500


def api_count():
    try:
        with urllib.request.urlopen(
            "http://127.0.0.1:8080/odds?brand=unibet&minutes=15&last=true"
        ) as f:
            return json.loads(f.read().decode()).get("count", 0)
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
        result = sp.check_output(cmd, stderr=sp.DEVNULL).decode().strip()
        return int(result or "0")
    except:
        return 0


print(f"[ORCHESTRATOR] Starting {WIN}s window, targets: odds≥50, api≥1", flush=True)
start = time.time()
mode = "http"
flipped = False

while time.time() - start < WIN:
    o1 = db_odds()
    o2 = api_count()
    print(
        json.dumps(
            {
                "t": int(time.time() - start),
                "mode": mode,
                "odds15m": o1,
                "api_count": o2,
            }
        ),
        flush=True,
    )

    if o1 >= 50 and o2 >= 1:
        with open("/tmp/UB_GO", "w") as f:
            f.write(json.dumps({"o1": o1, "o2": o2, "mode": mode}))
        print("[ORCHESTRATOR] SUCCESS! Targets met", flush=True)
        break

    if (time.time() - start > 240) and (o1 == 0) and not flipped:
        print("[ORCHESTRATOR] Switching to stealth mode", flush=True)
        try:
            sp.check_call(
                [
                    "docker",
                    "compose",
                    "-f",
                    "docker-compose.yml",
                    "-f",
                    "docker-compose.override.ub.yml",
                    "up",
                    "-d",
                    "collector-ub-stealth",
                ]
            )
        except:
            pass
        mode = "stealth"
        flipped = True

    time.sleep(20)

with open("/tmp/UB_SUMMARY", "w") as f:
    f.write(
        json.dumps({"final_odds15m": db_odds(), "final_api": api_count(), "mode": mode})
    )
print("[ORCHESTRATOR] Complete", flush=True)
