#!/usr/bin/env python3
"""
Auto-healing verification script for odds pipeline
Runs on DO droplet, checks health, and fixes issues
"""

import os
import subprocess
import time
import json
from datetime import datetime, timedelta
import psycopg2
import redis

# Configuration
REQUIRED_COLLECTORS = ["circa", "superbook", "betonline", "bookmaker", "betway", "wynnbet"]
MIN_ROWS_PER_10MIN = 10
BOVADA_MIN_ROWS = 5
REALNESS_THRESHOLD = 0.50

# Connection strings
DATABASE_URL = os.environ.get("DATABASE_URL")
REDIS_URL = os.environ.get("REDIS_URL")

def run_cmd(cmd):
    """Run shell command and return output"""
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=30)
        return result.stdout.strip(), result.returncode == 0
    except Exception as e:
        return str(e), False

def check_container_health(book):
    """Check if collector container is running"""
    cmd = f"docker ps --filter name=splits-oddsfeed-{book}-1 --format '{{{{.Status}}}}'"
    status, ok = run_cmd(cmd)
    return "Up" in status if ok else False

def check_metrics(book, port):
    """Check collector metrics endpoint"""
    cmd = f"curl -s http://localhost:{port}/metrics | grep collector_up | grep -v '#' | tail -1"
    output, ok = run_cmd(cmd)
    if ok and output:
        try:
            # Extract value from: collector_up{book="circa"} 1.0
            value = float(output.split()[-1])
            return value == 1.0
        except:
            pass
    return False

def check_database_writes(book, minutes=10):
    """Check database writes for a collector"""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()

        time_ago = datetime.utcnow() - timedelta(minutes=minutes)
        cur.execute("""
            SELECT COUNT(*) FROM odds
            WHERE book = %s AND ts > %s
        """, (book, time_ago))

        count = cur.fetchone()[0]
        cur.close()
        conn.close()
        return count
    except Exception as e:
        print(f"DB Error for {book}: {e}")
        return 0

def restart_collector(book):
    """Restart a collector container"""
    print(f"  🔄 Restarting {book}...")
    cmd = f"cd /root/splits-oddsfeed && docker-compose -f docker-compose.do.yml restart {book}"
    output, ok = run_cmd(cmd)
    if ok:
        print(f"  ✅ {book} restarted")
        time.sleep(10)  # Wait for startup
        return True
    else:
        print(f"  ❌ Failed to restart {book}: {output}")
        return False

def fix_normalizer():
    """Fix normalizer if not processing"""
    print("  🔧 Fixing normalizer...")

    # Copy fixed normalizer code
    cmd = "docker cp /root/splits-oddsfeed/normalizer/src/normalizer/main.py splits-oddsfeed-normalizer-1:/app/src/normalizer/main.py"
    run_cmd(cmd)

    # Restart normalizer
    cmd = "docker restart splits-oddsfeed-normalizer-1"
    output, ok = run_cmd(cmd)

    if ok:
        print("  ✅ Normalizer fixed and restarted")
        time.sleep(15)
        return True
    return False

def check_bovada():
    """Check Bovada status and quarantine metrics"""
    rows = check_database_writes("bovada", 10)

    # Check quarantine metrics
    cmd = "docker logs splits-oddsfeed-bovada-1 2>&1 | tail -50 | grep -E 'REALNESS_OK|Quarantined' | tail -5"
    output, _ = run_cmd(cmd)

    realness_ok = "REALNESS_OK=1" in output if output else False
    quarantined = "Quarantined" in output if output else False

    return {
        "rows": rows,
        "realness_ok": realness_ok,
        "quarantined": quarantined,
        "status": "PASS" if rows >= BOVADA_MIN_ROWS and realness_ok else "FAIL"
    }

def main():
    print("\n" + "="*60)
    print(f"🔍 ODDS PIPELINE VERIFICATION - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)

    results = {}
    all_pass = True

    # Port mapping
    ports = {
        "circa": 19199,
        "superbook": 19102,
        "betonline": 19198,
        "bookmaker": 19104,
        "betway": 19105,
        "wynnbet": 19106
    }

    print("\n📊 Checking 6 Required Collectors:")
    print("-" * 40)

    for book in REQUIRED_COLLECTORS:
        port = ports.get(book)

        # Check container
        container_up = check_container_health(book)

        # Check metrics
        metrics_ok = check_metrics(book, port) if port else False

        # Check database writes
        db_rows = check_database_writes(book)

        # Determine status
        status = "PASS" if container_up and db_rows >= MIN_ROWS_PER_10MIN else "FAIL"

        results[book] = {
            "container": container_up,
            "metrics": metrics_ok,
            "rows": db_rows,
            "status": status
        }

        # Print status
        icon = "✅" if status == "PASS" else "❌"
        print(f"{icon} {book:12} Container={'UP' if container_up else 'DOWN':4} "
              f"Metrics={'OK' if metrics_ok else 'NO':3} Rows={db_rows:3} {status}")

        # Auto-heal if failed
        if status == "FAIL":
            all_pass = False
            if not container_up or db_rows == 0:
                if restart_collector(book):
                    # Re-check after restart
                    time.sleep(10)
                    new_rows = check_database_writes(book, 1)
                    if new_rows > 0:
                        print(f"  ✅ {book} recovered! New rows: {new_rows}")
                        results[book]["status"] = "RECOVERED"

    # Check Bovada
    print("\n🎰 Checking Bovada:")
    print("-" * 40)
    bovada = check_bovada()
    icon = "✅" if bovada["status"] == "PASS" else "⚠️"
    print(f"{icon} bovada       Rows={bovada['rows']:3} "
          f"Realness={'OK' if bovada['realness_ok'] else 'LOW'} "
          f"Quarantine={'YES' if bovada['quarantined'] else 'NO'} "
          f"{bovada['status']}")

    # Check normalizer
    print("\n🔄 Checking Normalizer:")
    print("-" * 40)

    # Check if normalizer is processing
    cmd = "docker logs splits-oddsfeed-normalizer-1 2>&1 | tail -100 | grep -c 'E2E_TIMER: batch_size=0'"
    output, ok = run_cmd(cmd)
    batch_empty = int(output) > 10 if ok and output.isdigit() else False

    if batch_empty:
        print("⚠️  Normalizer batch_size=0, attempting fix...")
        if fix_normalizer():
            print("✅ Normalizer fixed")
    else:
        print("✅ Normalizer processing batches")

    # Summary
    print("\n" + "="*60)
    if all_pass:
        print("🎉 SUCCESS: All 6 collectors operational!")
    else:
        failed = [b for b, r in results.items() if r["status"] == "FAIL"]
        recovered = [b for b, r in results.items() if r["status"] == "RECOVERED"]

        if recovered:
            print(f"🔧 RECOVERED: {', '.join(recovered)}")
        if failed:
            print(f"❌ FAILED: {', '.join(failed)}")
            print("\nRecommended actions:")
            for book in failed:
                print(f"  - Check {book} logs: docker logs splits-oddsfeed-{book}-1")
                print(f"  - Verify Redis connectivity for {book}")

    print("="*60 + "\n")

    # Write JSON report
    report = {
        "timestamp": datetime.now().isoformat(),
        "collectors": results,
        "bovada": bovada,
        "all_pass": all_pass
    }

    with open("/root/verification_report.json", "w") as f:
        json.dump(report, f, indent=2)

    return 0 if all_pass else 1

if __name__ == "__main__":
    exit(main())