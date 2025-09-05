#!/usr/bin/env python3
"""
Deployment Monitor - Watches Bovada go live and enables progressive rollout
"""

import os
import sys
import time
import requests
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
import statistics

# Configuration
METRICS_URL = os.getenv("METRICS_URL", "http://localhost:8000/metrics")
RENDER_API_KEY = os.getenv("RENDER_API_KEY")
RENDER_OWNER_ID = os.getenv("RENDER_OWNER_ID")
SERVICE_BETRIVERS = os.getenv("SERVICE_BETRIVERS")
DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"

# Monitoring configuration
POLL_INTERVAL = 60  # seconds
BOVADA_GATE_DURATION = 30 * 60  # 30 minutes
BETRIVERS_GATE_DURATION = 60 * 60  # 60 minutes
REALNESS_THRESHOLD = 0.9


class MetricsCollector:
    def __init__(self):
        self.bovada_realness = []
        self.bovada_odds_15m = []
        self.bovada_ticks = []
        self.betrivers_realness = []
        self.betrivers_odds_15m = []
        self.betrivers_ticks = []

    def fetch_metrics(self) -> str:
        """Fetch metrics from proxy"""
        try:
            response = requests.get(METRICS_URL, timeout=10)
            response.raise_for_status()
            return response.text
        except Exception as e:
            print(f"⚠️  Failed to fetch metrics: {e}")
            return ""

    def parse_metric(
        self, metrics_text: str, metric_name: str, book: str
    ) -> Optional[float]:
        """Extract metric value for a book"""
        for line in metrics_text.split("\n"):
            if (
                metric_name in line
                and f'book="{book}"' in line
                and not line.startswith("#")
            ):
                parts = line.split()
                if len(parts) >= 2:
                    try:
                        return float(parts[-1])
                    except:
                        pass
        return None

    def collect_bovada(self, metrics_text: str):
        """Collect Bovada metrics"""
        realness = self.parse_metric(metrics_text, "realness_score", "bovada")
        odds = self.parse_metric(metrics_text, "odds_15m", "bovada")
        ticks = self.parse_metric(metrics_text, "ticks_total", "bovada")

        if realness is not None:
            self.bovada_realness.append(realness)
        if odds is not None:
            self.bovada_odds_15m.append(odds)
        if ticks is not None:
            self.bovada_ticks.append(ticks)

        return realness, odds, ticks

    def collect_betrivers(self, metrics_text: str):
        """Collect BetRivers metrics"""
        realness = self.parse_metric(metrics_text, "realness_score", "betrivers")
        odds = self.parse_metric(metrics_text, "odds_15m", "betrivers")
        ticks = self.parse_metric(metrics_text, "ticks_total", "betrivers")

        if realness is not None:
            self.betrivers_realness.append(realness)
        if odds is not None:
            self.betrivers_odds_15m.append(odds)
        if ticks is not None:
            self.betrivers_ticks.append(ticks)

        return realness, odds, ticks

    def check_bovada_gate(self) -> Tuple[bool, Dict]:
        """Check if Bovada passes stability gate"""
        results = {
            "realness_median": None,
            "odds_latest": None,
            "ticks_increasing": False,
            "samples": len(self.bovada_realness),
        }

        # Need at least 5 samples
        if len(self.bovada_realness) < 5:
            return False, results

        # Calculate median realness
        results["realness_median"] = statistics.median(
            self.bovada_realness[-30:]
        )  # Last 30 samples

        # Get latest odds
        if self.bovada_odds_15m:
            results["odds_latest"] = self.bovada_odds_15m[-1]

        # Check ticks increasing
        if len(self.bovada_ticks) >= 2:
            results["ticks_increasing"] = self.bovada_ticks[-1] > self.bovada_ticks[0]
            results["ticks_start"] = self.bovada_ticks[0]
            results["ticks_end"] = self.bovada_ticks[-1]

        # Gate passes if all criteria met
        passes = (
            results["realness_median"] is not None
            and results["realness_median"] >= REALNESS_THRESHOLD
            and results["odds_latest"] is not None
            and results["odds_latest"] > 0
            and results["ticks_increasing"]
        )

        return passes, results

    def check_betrivers_gate(self) -> Tuple[bool, Dict]:
        """Check if BetRivers passes stability gate"""
        results = {
            "realness_median": None,
            "odds_latest": None,
            "ticks_increasing": False,
            "samples": len(self.betrivers_realness),
        }

        if len(self.betrivers_realness) < 5:
            return False, results

        results["realness_median"] = statistics.median(self.betrivers_realness[-30:])

        if self.betrivers_odds_15m:
            results["odds_latest"] = self.betrivers_odds_15m[-1]

        if len(self.betrivers_ticks) >= 2:
            results["ticks_increasing"] = (
                self.betrivers_ticks[-1] > self.betrivers_ticks[0]
            )
            results["ticks_start"] = self.betrivers_ticks[0]
            results["ticks_end"] = self.betrivers_ticks[-1]

        passes = (
            results["realness_median"] is not None
            and results["realness_median"] >= REALNESS_THRESHOLD
            and results["odds_latest"] is not None
            and results["odds_latest"] > 0
            and results["ticks_increasing"]
        )

        return passes, results


class RenderAPI:
    def __init__(self, api_key: str, owner_id: str):
        self.api_key = api_key
        self.owner_id = owner_id
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }

    def enable_service(self, service_id: str, book: str) -> bool:
        """Enable and deploy a service"""
        if DRY_RUN:
            print(f"🔸 DRY RUN: Would enable {book} (service_id: {service_id})")
            return True

        try:
            # Update service to autoDeploy: true
            url = f"https://api.render.com/v1/services/{service_id}"
            data = {"autoDeploy": True}

            response = requests.patch(url, json=data, headers=self.headers)
            if response.status_code == 200:
                print(f"✅ Enabled autoDeploy for {book}")

                # Trigger deploy
                deploy_url = f"https://api.render.com/v1/services/{service_id}/deploys"
                deploy_response = requests.post(deploy_url, headers=self.headers)
                if deploy_response.status_code in [200, 201]:
                    deploy_data = deploy_response.json()
                    print(
                        f"✅ Deployment triggered for {book}: {deploy_data.get('id', 'unknown')}"
                    )
                    return True
                else:
                    print(f"⚠️  Deploy request failed: {deploy_response.status_code}")
                    return False
            else:
                print(f"❌ Failed to enable {book}: {response.status_code}")
                if response.text:
                    print(f"   Response: {response.text[:200]}")
                return False
        except Exception as e:
            print(f"❌ Error enabling {book}: {e}")
            return False

    def get_logs(self, service_id: str, lines: int = 200) -> List[str]:
        """Fetch recent logs from a service"""
        try:
            url = f"https://api.render.com/v1/services/{service_id}/logs"
            params = {"tail": lines}
            response = requests.get(url, params=params, headers=self.headers)
            if response.status_code == 200:
                return response.text.split("\n")
            return []
        except Exception as e:
            print(f"Failed to fetch logs: {e}")
            return []


def diagnose_failure(
    collector: MetricsCollector, render_api: Optional[RenderAPI] = None
):
    """Diagnose why Bovada isn't passing gates"""
    print("\n=== DIAGNOSTIC REPORT ===")

    # Check metrics availability
    print("\n📊 Metrics Summary:")
    print(f"  Realness samples: {len(collector.bovada_realness)}")
    if collector.bovada_realness:
        print(
            f"  Realness range: {min(collector.bovada_realness):.3f} - {max(collector.bovada_realness):.3f}"
        )
        print(f"  Realness median: {statistics.median(collector.bovada_realness):.3f}")

    print(f"  Odds samples: {len(collector.bovada_odds_15m)}")
    if collector.bovada_odds_15m:
        print(f"  Odds latest: {collector.bovada_odds_15m[-1]}")

    print(f"  Ticks samples: {len(collector.bovada_ticks)}")
    if len(collector.bovada_ticks) >= 2:
        print(
            f"  Ticks growth: {collector.bovada_ticks[-1] - collector.bovada_ticks[0]}"
        )

    # Fetch logs if API available
    if render_api and os.getenv("SERVICE_BOVADA"):
        print("\n📜 Recent Errors from Logs:")
        logs = render_api.get_logs(os.getenv("SERVICE_BOVADA"))
        errors = [l for l in logs if "ERROR" in l or "429" in l or "403" in l]

        # Count error types
        error_counts = {}
        for error in errors[-50:]:  # Last 50 errors
            if "429" in error:
                error_counts["Rate Limited (429)"] = (
                    error_counts.get("Rate Limited (429)", 0) + 1
                )
            elif "403" in error:
                error_counts["Forbidden (403)"] = (
                    error_counts.get("Forbidden (403)", 0) + 1
                )
            elif "timeout" in error.lower():
                error_counts["Timeout"] = error_counts.get("Timeout", 0) + 1
            elif "realness" in error.lower():
                error_counts["Realness Failed"] = (
                    error_counts.get("Realness Failed", 0) + 1
                )
            else:
                error_counts["Other"] = error_counts.get("Other", 0) + 1

        for error_type, count in sorted(
            error_counts.items(), key=lambda x: x[1], reverse=True
        )[:5]:
            print(f"  {error_type}: {count} occurrences")

    # Suggest fixes
    print("\n🔧 Suggested Fixes:")

    if (
        collector.bovada_realness
        and statistics.median(collector.bovada_realness) < REALNESS_THRESHOLD
    ):
        print("  ⚠️  Realness score too low - possible causes:")
        print(
            "     - Getting test/demo data → Check Bovada URL in collectors/bovada_real/"
        )
        print(
            "     - Schema changed → Update normalizer in normalizer/src/normalizer/bovada_mapper.py"
        )

    if not collector.bovada_odds_15m or collector.bovada_odds_15m[-1] == 0:
        print("  ⚠️  No odds data - possible causes:")
        print("     - 429 rate limits → Reduce COLLECTION_INTERVAL in render.yaml")
        print("     - 403 blocked → Add PROXY_URL in Render environment")
        print("     - Network issues → Check Render service logs")

    if not collector.bovada_ticks or len(collector.bovada_ticks) < 2:
        print("  ⚠️  No tick growth - possible causes:")
        print("     - Collector not running → Check Render service status")
        print("     - Redis connection issue → Verify REDIS_URL in render.yaml")


def main():
    print("=== BOVADA DEPLOYMENT MONITOR ===")
    print(f"Time: {datetime.now(timezone.utc).isoformat()}")
    print(f"Mode: {'DRY RUN' if DRY_RUN else 'LIVE'}")
    print(f"Metrics URL: {METRICS_URL}")
    print("")

    collector = MetricsCollector()
    render_api = None

    if RENDER_API_KEY and RENDER_OWNER_ID:
        render_api = RenderAPI(RENDER_API_KEY, RENDER_OWNER_ID)
        print("✅ Render API configured")
    else:
        print("⚠️  Render API not configured (dry run mode)")

    # Phase 1: Monitor Bovada for 30 minutes
    print("\n📍 PHASE 1: Monitoring Bovada")
    print(f"Duration: {BOVADA_GATE_DURATION // 60} minutes")
    print(f"Threshold: realness >= {REALNESS_THRESHOLD}")
    print("")

    bovada_start = time.time()
    bovada_stable = False

    while time.time() - bovada_start < BOVADA_GATE_DURATION:
        elapsed = int(time.time() - bovada_start)
        remaining = BOVADA_GATE_DURATION - elapsed

        # Fetch metrics
        metrics = collector.fetch_metrics()
        if metrics:
            realness, odds, ticks = collector.collect_bovada(metrics)
            realness_str = f"{realness:.3f}" if realness is not None else "N/A"
            odds_str = str(odds) if odds is not None else "N/A"
            ticks_str = str(ticks) if ticks is not None else "N/A"
            print(
                f"[{elapsed//60:02d}:{elapsed%60:02d}] Bovada: realness={realness_str}, "
                f"odds_15m={odds_str}, ticks={ticks_str}"
            )
        else:
            print(f"[{elapsed//60:02d}:{elapsed%60:02d}] No metrics available")

        # Check gate
        passes, results = collector.check_bovada_gate()
        if passes and elapsed >= 300:  # Need at least 5 minutes of data
            print("\n✅ BOVADA GATE PASSED")
            print(f"   Median realness: {results['realness_median']:.3f}")
            print(f"   Latest odds: {results['odds_latest']}")
            print(
                f"   Ticks increased: {results['ticks_start']} → {results['ticks_end']}"
            )
            bovada_stable = True
            break

        # Sleep until next poll
        time.sleep(min(POLL_INTERVAL, remaining))

    if not bovada_stable:
        print(f"\n❌ BOVADA GATE FAILED after {BOVADA_GATE_DURATION // 60} minutes")
        passes, results = collector.check_bovada_gate()
        print(f"   Median realness: {results.get('realness_median', 'N/A')}")
        print(f"   Latest odds: {results.get('odds_latest', 'N/A')}")
        print(f"   Ticks increasing: {results.get('ticks_increasing', False)}")

        diagnose_failure(collector, render_api)
        sys.exit(1)

    # Phase 2: Enable BetRivers if configured
    if not SERVICE_BETRIVERS:
        print("\n⚠️  SERVICE_BETRIVERS not set, skipping progressive rollout")
        print(
            "To enable: set SERVICE_BETRIVERS environment variable to Render service ID"
        )
        sys.exit(0)

    print("\n📍 PHASE 2: Enabling BetRivers")

    if render_api:
        if render_api.enable_service(SERVICE_BETRIVERS, "betrivers"):
            print("✅ BetRivers deployment initiated")
        else:
            print("❌ Failed to enable BetRivers")
            sys.exit(1)
    else:
        print("🔸 DRY RUN: Would enable BetRivers")

    # Phase 3: Monitor BetRivers for 60 minutes
    print("\n📍 PHASE 3: Monitoring BetRivers")
    print(f"Duration: {BETRIVERS_GATE_DURATION // 60} minutes")
    print("")

    betrivers_start = time.time()
    betrivers_stable = False

    # Wait 2 minutes for deployment to start
    print("Waiting 2 minutes for deployment...")
    time.sleep(120)

    while time.time() - betrivers_start < BETRIVERS_GATE_DURATION:
        elapsed = int(time.time() - betrivers_start)
        remaining = BETRIVERS_GATE_DURATION - elapsed

        # Fetch metrics
        metrics = collector.fetch_metrics()
        if metrics:
            realness, odds, ticks = collector.collect_betrivers(metrics)
            print(
                f"[{elapsed//60:02d}:{elapsed%60:02d}] BetRivers: realness={realness:.3f if realness else 'N/A'}, "
                f"odds_15m={odds if odds else 'N/A'}, ticks={ticks if ticks else 'N/A'}"
            )

        # Check gate
        passes, results = collector.check_betrivers_gate()
        if passes and elapsed >= 300:
            print("\n✅ BETRIVERS GATE PASSED")
            print(f"   Median realness: {results['realness_median']:.3f}")
            print(f"   Latest odds: {results['odds_latest']}")
            print(
                f"   Ticks increased: {results['ticks_start']} → {results['ticks_end']}"
            )
            betrivers_stable = True
            break

        time.sleep(min(POLL_INTERVAL, remaining))

    if not betrivers_stable:
        print(
            f"\n⚠️  BETRIVERS GATE PENDING after {BETRIVERS_GATE_DURATION // 60} minutes"
        )
        passes, results = collector.check_betrivers_gate()
        print(f"   Median realness: {results.get('realness_median', 'N/A')}")
        print(f"   Latest odds: {results.get('odds_latest', 'N/A')}")
        print(f"   Ticks increasing: {results.get('ticks_increasing', False)}")

    # Final summary
    print("\n=== DEPLOYMENT SUMMARY ===")
    print(f"Bovada: {'✅ STABLE' if bovada_stable else '❌ FAILED'}")
    print(
        f"BetRivers: {'✅ STABLE' if betrivers_stable else '⚠️ PENDING' if SERVICE_BETRIVERS else '⏭️ SKIPPED'}"
    )

    if betrivers_stable:
        print("\n🚀 Ready to enable next Kambi brand!")
        print("Next books in queue: Barstool, Caesars, SugarHouse, Unibet")
        print("Set SERVICE_BARSTOOL, SERVICE_CAESARS, etc. to continue rollout")


if __name__ == "__main__":
    main()
