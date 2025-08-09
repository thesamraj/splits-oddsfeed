from enum import Enum
from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel


class Sport(str, Enum):
    NFL = "nfl"
    NBA = "nba"
    MLB = "mlb"
    NHL = "nhl"
    NCAAF = "ncaaf"
    NCAAB = "ncaab"
    SOCCER = "soccer"
    TENNIS = "tennis"
    MMA = "mma"
    BOXING = "boxing"


class MarketType(str, Enum):
    MONEYLINE = "moneyline"
    SPREAD = "spread"
    TOTAL = "total"
    PROPS = "props"
    FUTURES = "futures"


class Outcome(BaseModel):
    id: str
    name: str
    price: float
    line: Optional[float] = None
    last_seen_ts: datetime
    source_ts: datetime


class Market(BaseModel):
    id: str
    type: MarketType
    outcomes: List[Outcome]


class Event(BaseModel):
    id: str
    sport: Sport
    league: str
    home: str
    away: str
    start_ts: datetime
    markets: List[Market]
