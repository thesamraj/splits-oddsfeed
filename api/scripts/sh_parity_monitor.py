#!/usr/bin/env python3
"""
24-hour BR⇄SH Parity Monitor
Checks every 5 minutes that BetRivers and SugarHouse return identical counts
"""
import json
import time
import os
from datetime import datetime, timedelta
import requests
import subprocess

API_BASE = "http://localhost:8080"
LOG_DIR = "SH_PARITY_LOG"
CHECK_INTERVAL = 300  # 5 minutes
WINDOW_MINUTES = 15
TOLERANCE = 0.02  # 2% difference allowed

os.makedirs(LOG_DIR, exist_ok=True)
consecutive_failures = 0


def get_brand_counts(brand, minutes=WINDOW_MINUTES):
    """Get event and odds counts for a brand"""
    try:
        resp = requests.get(f"{API_BASE}/odds?brand={brand}&minutes={minutes}")
        data = resp.json()
        return {
            "count": data.get("count", 0),
            "status": data.get("status", "error"),
            "has_prices": any(e.get("odds", []) for e in data.get("events", [])),
        }
    except Exception as e:
        return {"count": 0, "status": "error", "error": str(e)}


def check_parity():
    """Check if BR and SH have identical counts"""
    global consecutive_failures

    timestamp = datetime.utcnow().isoformat()
    br_data = get_brand_counts("betrivers")
    sh_data = get_brand_counts("sugarhouse")

    # Calculate difference
    if br_data["count"] > 0:
        diff_pct = abs(br_data["count"] - sh_data["count"]) / br_data["count"]
    else:
        diff_pct = 0 if sh_data["count"] == 0 else 1.0

    parity_pass = diff_pct <= TOLERANCE

    result = {
        "timestamp": timestamp,
        "betrivers": br_data,
        "sugarhouse": sh_data,
        "diff_pct": round(diff_pct * 100, 2),
        "parity": "PASS" if parity_pass else "FAIL",
    }

    # Log result
    log_file = os.path.join(
        LOG_DIR, f"parity_{datetime.utcnow().strftime('%Y%m%d')}.jsonl"
    )
    with open(log_file, "a") as f:
        f.write(json.dumps(result) + "\n")

    print(
        f"[{timestamp}] BR={br_data['count']} SH={sh_data['count']} Diff={result['diff_pct']}% {result['parity']}"
    )

    # Handle failures
    if not parity_pass:
        consecutive_failures += 1
        if consecutive_failures >= 2:
            print(
                f"WARNING: {consecutive_failures} consecutive parity failures! Restarting API..."
            )
            subprocess.run(["docker", "compose", "restart", "api"], capture_output=True)
            time.sleep(10)
            # Re-check after restart
            br_data = get_brand_counts("betrivers")
            sh_data = get_brand_counts("sugarhouse")
            print(f"After restart: BR={br_data['count']} SH={sh_data['count']}")

            # Capture diagnostics
            diag_file = os.path.join(
                LOG_DIR,
                f"diagnostics_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json",
            )
            with open(diag_file, "w") as f:
                json.dump(
                    {
                        "timestamp": timestamp,
                        "consecutive_failures": consecutive_failures,
                        "br_sample": requests.get(
                            f"{API_BASE}/odds?brand=betrivers&minutes=5&limit=3"
                        ).json(),
                        "sh_sample": requests.get(
                            f"{API_BASE}/odds?brand=sugarhouse&minutes=5&limit=3"
                        ).json(),
                    },
                    f,
                    indent=2,
                )
    else:
        consecutive_failures = 0

    return result


def main():
    """Run 24-hour monitoring"""
    start_time = datetime.utcnow()
    end_time = start_time + timedelta(hours=24)

    print("Starting 24h BR⇄SH parity monitor")
    print(f"Start: {start_time.isoformat()}")
    print(f"End: {end_time.isoformat()}")

    while datetime.utcnow() < end_time:
        try:
            check_parity()
        except Exception as e:
            print(f"ERROR: {e}")

        # Wait for next check
        time.sleep(CHECK_INTERVAL)

    print("24-hour monitoring complete")

    # Generate summary
    summary_file = os.path.join(LOG_DIR, "SUMMARY.md")
    with open(summary_file, "w") as f:
        f.write("# BR⇄SH 24-Hour Parity Monitor Summary\n\n")
        f.write(f"Start: {start_time.isoformat()}\n")
        f.write(f"End: {datetime.utcnow().isoformat()}\n\n")

        # Analyze logs
        total_checks = 0
        pass_count = 0
        for log_file in os.listdir(LOG_DIR):
            if log_file.endswith(".jsonl"):
                with open(os.path.join(LOG_DIR, log_file)) as lf:
                    for line in lf:
                        entry = json.loads(line)
                        total_checks += 1
                        if entry["parity"] == "PASS":
                            pass_count += 1

        f.write(f"Total checks: {total_checks}\n")
        f.write(f"Passed: {pass_count}\n")
        f.write(f"Failed: {total_checks - pass_count}\n")
        f.write(f"Success rate: {100 * pass_count / total_checks:.1f}%\n")


if __name__ == "__main__":
    main()
