#!/usr/bin/env python3
"""Adapter for Superbook sportsbook messages"""

import time
from typing import Dict, Any, Optional
from .base_adapter import BaseAdapter
import logging

logger = logging.getLogger(__name__)


class SuperbookAdapter(BaseAdapter):
    """Convert Superbook messages to canonical format"""
    
    def __init__(self):
        super().__init__("superbook")
    
    def to_canonical(self, raw_message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Convert Superbook raw message to canonical format"""
        try:
            # Handle both single event and events array
            events = raw_message.get('events', [raw_message])
            
            canonical_events = []
            for event in events:
                canonical = {
                    "book": self.book,
                    "sport": event.get("sport", "football"),
                    "league": event.get("league", "NFL"),
                    "event_id": event.get("event_id", f"superbook_{int(time.time()*1000)}"),
                    "home": self.normalize_team_name(event.get("home", event.get("home_team", ""))),
                    "away": self.normalize_team_name(event.get("away", event.get("away_team", ""))),
                    "commence_time": self.ensure_iso_timestamp(event.get("commence_time", event.get("ts"))),
                    "markets": [],
                    "ts": int(time.time() * 1000)
                }
                
                # Handle moneyline from price_home/price_away
                if "price_home" in event or "price_away" in event:
                    market = {
                        "key": "moneyline",
                        "outcomes": []
                    }
                    if "price_home" in event:
                        market["outcomes"].append({
                            "name": "home",
                            "price": event["price_home"]
                        })
                    if "price_away" in event:
                        market["outcomes"].append({
                            "name": "away",
                            "price": event["price_away"]
                        })
                    if market["outcomes"]:
                        canonical["markets"].append(market)
                
                # Handle markets array if present
                if "markets" in event:
                    for mkt in event["markets"]:
                        market_obj = {
                            "key": mkt.get("key", mkt.get("type", "unknown")),
                            "outcomes": []
                        }
                        for outcome in mkt.get("outcomes", []):
                            out = {
                                "name": outcome.get("name"),
                                "price": outcome.get("price")
                            }
                            if "point" in outcome:
                                out["point"] = outcome["point"]
                            market_obj["outcomes"].append(out)
                        
                        if market_obj["outcomes"]:
                            canonical["markets"].append(market_obj)
                
                # Only add if valid
                if self.validate_canonical(canonical):
                    canonical_events.append(canonical)
                else:
                    logger.warning(f"Superbook event failed validation: {event.get('event_id', 'unknown')}")
            
            # Return single event or wrapped events
            if len(canonical_events) == 1:
                return canonical_events[0]
            elif canonical_events:
                return {"events": canonical_events}
            else:
                return None
                
        except Exception as e:
            logger.error(f"Superbook adapter error: {e}")
            return None