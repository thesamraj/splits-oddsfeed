import re


def extract_odds_from_html(html):
    """Extract odds data from FanDuel HTML"""
    events = []

    # Pattern 1: American odds format
    american_odds = re.findall(r"([+-]\d{3,4})", html)

    # Pattern 2: Team names (common patterns)
    team_pattern = re.compile(r"([A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,2})")
    teams = team_pattern.findall(html)

    # Pattern 3: Look for JSON-like structures
    json_pattern = re.compile(r'\{[^}]*"(?:odds|price|spread|total|moneyline)"[^}]*\}')
    json_matches = json_pattern.findall(html)

    # Pattern 4: Data attributes
    data_pattern = re.compile(r'data-(?:event|odds|price|team)[^=]*=["\'](.*?)["\']')
    data_matches = data_pattern.findall(html)

    return {
        "american_odds": american_odds[:100],  # Limit to first 100
        "teams": list(set(teams))[:50],
        "json_structures": json_matches[:20],
        "data_attributes": data_matches[:20],
    }


def parse_fanduel_json(data):
    """Parse FanDuel JSON response"""
    events = []

    def walk(obj, path=""):
        if isinstance(obj, dict):
            # Look for event-like structures
            if any(k in obj for k in ["eventId", "event_id", "id"]):
                event = {
                    "id": obj.get("eventId") or obj.get("event_id") or obj.get("id"),
                    "home": obj.get("homeTeam") or obj.get("home"),
                    "away": obj.get("awayTeam") or obj.get("away"),
                    "markets": [],
                }

                # Extract markets
                for market_key in ["markets", "odds", "prices"]:
                    if market_key in obj:
                        market_data = obj[market_key]
                        if isinstance(market_data, list):
                            for market in market_data:
                                if isinstance(market, dict):
                                    event["markets"].append(
                                        {
                                            "type": market.get("type")
                                            or market.get("marketType"),
                                            "odds": market.get("odds")
                                            or market.get("prices"),
                                            "line": market.get("line")
                                            or market.get("spread"),
                                        }
                                    )

                if event["markets"]:
                    events.append(event)

            # Recurse
            for k, v in obj.items():
                walk(v, f"{path}.{k}")
        elif isinstance(obj, list):
            for i, item in enumerate(obj):
                walk(item, f"{path}[{i}]")

    walk(data)
    return events


def normalize_odds(value):
    """Normalize odds to American format"""
    if isinstance(value, str):
        # Remove special characters
        value = value.replace("−", "-").replace("+", "")
        try:
            return int(value)
        except:
            return None
    elif isinstance(value, (int, float)):
        return int(value)
    return None
