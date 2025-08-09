from datetime import datetime
from typing import Dict, Any, Optional
from pydantic import BaseModel


class RawOddsMessage(BaseModel):
    book: str
    sport: Optional[str] = None
    payload: Dict[str, Any]
    received_ts: datetime