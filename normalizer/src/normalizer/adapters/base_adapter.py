#!/usr/bin/env python3
"""Base adapter for normalizing book-specific messages to canonical format"""

import time
from datetime import datetime
from typing import Dict, List, Optional, Any
import logging

logger = logging.getLogger(__name__)


class BaseAdapter:
    """Base class for book-specific adapters"""
    
    def __init__(self, book_name: str):
        self.book = book_name
        
    def to_canonical(self, raw_message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Convert raw message to canonical format
        
        Canonical format:
        {
            "book": "book_name",
            "sport": "football",
            "league": "NFL",
            "event_id": "unique_id",
            "home": "Home Team",
            "away": "Away Team",
            "commence_time": "2024-01-01T00:00:00Z",
            "markets": [
                {
                    "key": "moneyline",
                    "outcomes": [
                        {"name": "home", "price": -110},
                        {"name": "away", "price": -110}
                    ]
                },
                {
                    "key": "spread",
                    "outcomes": [
                        {"name": "home", "price": -110, "point": -3.5},
                        {"name": "away", "price": -110, "point": 3.5}
                    ]
                },
                {
                    "key": "total",
                    "outcomes": [
                        {"name": "over", "price": -110, "point": 45.5},
                        {"name": "under", "price": -110, "point": 45.5}
                    ]
                }
            ],
            "ts": 1234567890000
        }
        """
        raise NotImplementedError("Subclasses must implement to_canonical")
    
    def normalize_team_name(self, name: str) -> str:
        """Normalize team name (trim, collapse whitespace)"""
        if not name:
            return ""
        return " ".join(name.strip().split())
    
    def ensure_iso_timestamp(self, ts: Any) -> Optional[str]:
        """Convert various timestamp formats to ISO8601 UTC"""
        if not ts:
            return None
            
        try:
            # If already ISO format
            if isinstance(ts, str) and 'T' in ts:
                return ts
            
            # If epoch timestamp
            if isinstance(ts, (int, float)):
                if ts > 10**10:  # milliseconds
                    ts = ts / 1000
                return datetime.utcfromtimestamp(ts).isoformat() + 'Z'
            
            # If datetime object
            if hasattr(ts, 'isoformat'):
                return ts.isoformat() + 'Z'
                
            return None
        except Exception as e:
            logger.warning(f"Failed to convert timestamp {ts}: {e}")
            return None
    
    def validate_canonical(self, msg: Dict[str, Any]) -> bool:
        """Validate that message has required fields"""
        required = ['book', 'event_id', 'markets']
        for field in required:
            if field not in msg or not msg[field]:
                logger.warning(f"Missing required field: {field}")
                return False
        
        # Must have at least one market with outcomes
        if not msg.get('markets'):
            logger.warning("No markets in message")
            return False
            
        for market in msg['markets']:
            if not market.get('outcomes'):
                logger.warning(f"Market {market.get('key')} has no outcomes")
                return False
                
        return True