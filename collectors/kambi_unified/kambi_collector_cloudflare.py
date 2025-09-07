#!/usr/bin/env python3
"""
Kambi Unified Collector with Cloudflare bypass
Uses cloudscraper to bypass Cloudflare protection
"""

import os
import sys
import time
import json
import random
import redis
from datetime import datetime
from typing import Dict, List, Any, Optional
import logging

try:
    import cloudscraper
except ImportError:
    print("Installing cloudscraper...")
    os.system("pip install cloudscraper")
    import cloudscraper

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Configuration
KAMBI_BRAND = os.getenv("KAMBI_BRAND", "betrivers").lower()
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
COLLECTION_INTERVAL = int(os.getenv("COLLECTION_INTERVAL", "60"))

# Brand configurations
BRAND_CONFIG = {
    "betrivers": {
        "base_url": "https://eu-offering.kambicdn.org/offering/v2018/rsi2us",
        "endpoints": [
            "listView/american_football.json",
            "listView/american_football/nfl.json",
            "listView/basketball.json",
            "listView/basketball/nba.json",
        ],
    },
    "barstool": {
        "base_url": "https://eu-offering.kambicdn.org/offering/v2018/barstoolsports",
        "endpoints": [
            "listView/american_football.json",
            "listView/american_football/nfl.json",
            "listView/basketball.json",
            "listView/basketball/nba.json",
        ],
    },
    "caesars": {
        "base_url": "https://eu-offering.kambicdn.org/offering/v2018/caesarspa",
        "endpoints": [
            "listView/american_football.json",
            "listView/american_football/nfl.json",
            "listView/basketball.json",
            "listView/basketball/nba.json",
        ],
    },
    "sugarhouse": {
        "base_url": "https://eu-offering.kambicdn.org/offering/v2018/shpa",
        "endpoints": [
            "listView/american_football.json",
            "listView/american_football/nfl.json",
            "listView/basketball.json",
            "listView/basketball/nba.json",
        ],
    },
    "unibet": {
        "base_url": "https://eu-offering.kambicdn.org/offering/v2018/ubuspa",
        "endpoints": [
            "listView/american_football.json",
            "listView/american_football/nfl.json",
            "listView/basketball.json",
            "listView/basketball/nba.json",
        ],
    },
}

class KambiCloudflareCollector:
    def __init__(self):
        self.brand = KAMBI_BRAND
        self.config = BRAND_CONFIG.get(self.brand)
        if not self.config:
            logger.error(f"Unknown brand: {self.brand}")
            sys.exit(1)

        # Use cloudscraper instead of regular requests
        self.scraper = cloudscraper.create_scraper(
            browser={
                'browser': 'chrome',
                'platform': 'windows',
                'mobile': False
            }
        )
        
        self.redis_client = redis.from_url(REDIS_URL)
        logger.info(f"Initialized {self.brand} collector with cloudscraper")

    def fetch_odds(self, endpoint: str) -> Optional[Dict]:
        """Fetch odds using cloudscraper"""
        url = f"{self.config['base_url']}/{endpoint}"
        
        try:
            logger.info(f"Fetching {url}")
            response = self.scraper.get(url, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                logger.info(f"✓ Got data from {endpoint}")
                return data
            else:
                logger.warning(f"Status {response.status_code} from {endpoint}")
                return None
                
        except Exception as e:
            logger.error(f"Error fetching {endpoint}: {e}")
            return None

    def process_events(self, data: Dict) -> List[Dict]:
        """Process raw Kambi data into normalized events"""
        events = []
        
        if "events" in data:
            for event in data.get("events", []):
                processed = {
                    "id": event.get("event", {}).get("id"),
                    "name": event.get("event", {}).get("name"),
                    "sport": event.get("event", {}).get("sport"),
                    "start_time": event.get("event", {}).get("start"),
                    "markets": []
                }
                
                # Extract markets
                for betOffer in event.get("betOffers", []):
                    market = {
                        "id": betOffer.get("id"),
                        "name": betOffer.get("criterion", {}).get("label"),
                        "outcomes": []
                    }
                    
                    for outcome in betOffer.get("outcomes", []):
                        market["outcomes"].append({
                            "id": outcome.get("id"),
                            "name": outcome.get("label"),
                            "odds": outcome.get("odds"),
                            "line": outcome.get("line"),
                        })
                    
                    processed["markets"].append(market)
                
                events.append(processed)
        
        return events

    def publish_events(self, events: List[Dict]):
        """Publish events to Redis"""
        if not events:
            return
            
        channel = f"odds.raw.{self.brand}"
        message = {
            "timestamp": datetime.utcnow().isoformat(),
            "source": self.brand,
            "realness_score": 0.95,
            "is_warmup": False,
            "events": events
        }
        
        self.redis_client.publish(channel, json.dumps(message))
        logger.info(f"Published {len(events)} events to {channel}")

    def run(self):
        """Main collection loop"""
        logger.info(f"Starting {self.brand} collector with cloudscraper")
        
        while True:
            try:
                all_events = []
                
                for endpoint in self.config["endpoints"]:
                    data = self.fetch_odds(endpoint)
                    if data:
                        events = self.process_events(data)
                        all_events.extend(events)
                    
                    # Small delay between requests
                    time.sleep(random.uniform(2, 5))
                
                if all_events:
                    # Deduplicate events by ID
                    unique_events = {e["id"]: e for e in all_events if e.get("id")}
                    self.publish_events(list(unique_events.values()))
                    logger.info(f"Collected {len(unique_events)} unique events")
                else:
                    logger.warning("No events collected this cycle")
                
            except Exception as e:
                logger.error(f"Error in collection loop: {e}")
            
            logger.info(f"Sleeping {COLLECTION_INTERVAL}s...")
            time.sleep(COLLECTION_INTERVAL)

if __name__ == "__main__":
    collector = KambiCloudflareCollector()
    collector.run()