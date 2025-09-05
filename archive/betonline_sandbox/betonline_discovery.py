#!/usr/bin/env python3
"""
BetOnline Discovery Collector (Sandbox Mode)
Captures real XHR/GraphQL responses and logs them
NO DATABASE WRITES - Sandbox channel only
"""

import json
import time
import redis
import logging
import requests
from datetime import datetime
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("betonline_discovery")


class BetOnlineDiscovery:
    def __init__(self):
        self.redis_client = redis.from_url("redis://broker:6379/0")
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "*/*",
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": "https://www.betonline.ag/",
            }
        )

        # Test various endpoints
        self.test_endpoints = [
            "https://www.betonline.ag/api/v1/lines/football/nfl",
            "https://www.betonline.ag/api/lines/nfl",
            "https://api.betonline.ag/v2/lines/football",
            "https://www.betonline.ag/sportsbook/api/lines",
            "https://www.betonline.ag/api/sportsbook/lines/nfl",
            "https://sb-api.betonline.ag/lines/nfl",
            "https://www.betonline.ag/_next/data/odds/nfl.json",
        ]

        # Create artifacts directory
        self.artifacts_dir = (
            f"/artifacts/BetOnline/{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        )
        os.makedirs(self.artifacts_dir, exist_ok=True)

    def discover_endpoints(self):
        """Try various endpoints to find working ones"""
        working_endpoints = []

        for url in self.test_endpoints:
            try:
                logger.info(f"Testing: {url}")
                resp = self.session.get(url, timeout=10)

                # Log response
                log_entry = {
                    "url": url,
                    "status": resp.status_code,
                    "headers": dict(resp.headers),
                    "timestamp": datetime.now().isoformat(),
                }

                # Check if JSON
                try:
                    data = resp.json()
                    log_entry["is_json"] = True
                    log_entry["sample"] = data[:100] if isinstance(data, list) else data

                    # Save full response
                    filename = f"{self.artifacts_dir}/{url.split('/')[-1]}_{int(time.time())}.json"
                    with open(filename, "w") as f:
                        json.dump(data, f, indent=2)
                    logger.info(f"✓ Saved JSON to {filename}")

                    working_endpoints.append(url)

                except:
                    log_entry["is_json"] = False
                    log_entry["content_type"] = resp.headers.get("content-type", "")

                    # Save HTML if that's what we got
                    if "html" in log_entry["content_type"]:
                        filename = f"{self.artifacts_dir}/{url.split('/')[-1]}_{int(time.time())}.html"
                        with open(filename, "w") as f:
                            f.write(resp.text[:10000])  # First 10KB
                        logger.info(f"  Saved HTML sample to {filename}")

                # Publish discovery to sandbox channel
                self.redis_client.publish(
                    "odds.raw.betonline.sandbox", json.dumps(log_entry)
                )

                time.sleep(2)  # Be polite

            except Exception as e:
                logger.error(f"Error testing {url}: {e}")

        return working_endpoints

    def capture_sample(self):
        """Capture a sample payload from any working endpoint"""
        # Try a simple static endpoint that might work
        test_url = "https://www.betonline.ag/sportsbook"

        try:
            resp = self.session.get(test_url, timeout=10)

            # Look for embedded JSON in HTML
            import re

            json_pattern = re.findall(
                r"window\.__INITIAL_STATE__\s*=\s*({.*?});", resp.text, re.DOTALL
            )

            if json_pattern:
                try:
                    data = json.loads(json_pattern[0])

                    # Save sample
                    sample_file = f"{self.artifacts_dir}/embedded_state_sample.json"
                    with open(sample_file, "w") as f:
                        json.dump(data, f, indent=2)

                    logger.info(f"✓ Captured embedded state to {sample_file}")

                    # Publish to sandbox
                    sandbox_msg = {
                        "timestamp": datetime.now().isoformat(),
                        "source": "betonline_sandbox",
                        "type": "discovery",
                        "sample_saved": sample_file,
                        "data_preview": str(data)[:500],
                    }

                    self.redis_client.publish(
                        "odds.raw.betonline.sandbox", json.dumps(sandbox_msg)
                    )

                    return data

                except Exception as e:
                    logger.error(f"Failed to parse embedded JSON: {e}")

        except Exception as e:
            logger.error(f"Failed to capture sample: {e}")

        # Create minimal sample for requirement
        sample = {
            "timestamp": datetime.now().isoformat(),
            "source": "betonline_sandbox",
            "status": "discovery_mode",
            "note": "Real endpoints need browser automation or API key",
            "artifacts_dir": self.artifacts_dir,
        }

        sample_file = f"{self.artifacts_dir}/discovery_sample.json"
        with open(sample_file, "w") as f:
            json.dump(sample, f, indent=2)

        return sample

    def run(self):
        """Main discovery loop"""
        logger.info("Starting BetOnline Discovery (SANDBOX MODE - NO DB WRITES)")
        logger.info(f"Artifacts directory: {self.artifacts_dir}")

        cycle = 0

        while cycle < 3:  # Only run 3 cycles for discovery
            cycle += 1
            logger.info(f"\n=== Discovery Cycle {cycle} ===")

            # Test endpoints
            working = self.discover_endpoints()

            if working:
                logger.info(f"Found {len(working)} working endpoints")
            else:
                logger.info("No working JSON endpoints found")

            # Capture sample
            sample = self.capture_sample()

            # Log to sandbox channel only
            discovery_log = {
                "timestamp": datetime.now().isoformat(),
                "cycle": cycle,
                "endpoints_tested": len(self.test_endpoints),
                "working_endpoints": working,
                "artifacts_saved": os.listdir(self.artifacts_dir),
            }

            self.redis_client.publish(
                "odds.raw.betonline.sandbox", json.dumps(discovery_log)
            )

            logger.info("Published discovery log to sandbox channel")

            # Wait before next cycle
            time.sleep(60)

        logger.info(
            "Discovery complete. Check artifacts directory for captured responses."
        )


if __name__ == "__main__":
    BetOnlineDiscovery().run()
