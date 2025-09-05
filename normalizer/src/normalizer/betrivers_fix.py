#!/usr/bin/env python3
"""
BetRivers Normalizer Fix
Converts old schema (price_home/price_away) to new schema (outcome_name/outcome_price)
"""


def convert_to_new_schema(rows):
    """
    Convert rows from old schema to new schema format
    Old: price_home, price_away, price_over, price_under
    New: outcome_name, outcome_price, outcome_point
    """
    converted_rows = []

    for row in rows:
        event_id = row.get("event_id")
        book = row.get("book", "betrivers")
        brand = row.get("brand", "betrivers")
        market = row.get("market", "h2h")
        line = row.get("line")
        total = row.get("total")
        ts = row.get("ts")
        home_team = row.get("home_team", "")
        away_team = row.get("away_team", "")

        # Convert H2H/Moneyline markets
        if market == "h2h":
            if row.get("price_home") is not None:
                converted_rows.append(
                    {
                        "event_id": event_id,
                        "book": book,
                        "brand": brand,
                        "market": "moneyline",
                        "outcome_name": home_team or "Home",
                        "outcome_price": row["price_home"],
                        "outcome_point": None,
                        "ts": ts,
                        "_source_ts_ms": row.get("_source_ts_ms"),
                    }
                )

            if row.get("price_away") is not None:
                converted_rows.append(
                    {
                        "event_id": event_id,
                        "book": book,
                        "brand": brand,
                        "market": "moneyline",
                        "outcome_name": away_team or "Away",
                        "outcome_price": row["price_away"],
                        "outcome_point": None,
                        "ts": ts,
                        "_source_ts_ms": row.get("_source_ts_ms"),
                    }
                )

        # Convert Spread markets
        elif market == "spreads" or market == "spread":
            if row.get("price_home") is not None and line is not None:
                converted_rows.append(
                    {
                        "event_id": event_id,
                        "book": book,
                        "brand": brand,
                        "market": "spread",
                        "outcome_name": (
                            f"{home_team or 'Home'} {line:+.1f}"
                            if home_team
                            else f"Home {line:+.1f}"
                        ),
                        "outcome_price": row["price_home"],
                        "outcome_point": line,
                        "ts": ts,
                        "_source_ts_ms": row.get("_source_ts_ms"),
                    }
                )

            if row.get("price_away") is not None and line is not None:
                converted_rows.append(
                    {
                        "event_id": event_id,
                        "book": book,
                        "brand": brand,
                        "market": "spread",
                        "outcome_name": (
                            f"{away_team or 'Away'} {-line:+.1f}"
                            if away_team
                            else f"Away {-line:+.1f}"
                        ),
                        "outcome_price": row["price_away"],
                        "outcome_point": -line,
                        "ts": ts,
                        "_source_ts_ms": row.get("_source_ts_ms"),
                    }
                )

        # Convert Totals markets
        elif market == "totals" or market == "total":
            if row.get("price_over") is not None and total is not None:
                converted_rows.append(
                    {
                        "event_id": event_id,
                        "book": book,
                        "brand": brand,
                        "market": "total",
                        "outcome_name": f"Over {total}",
                        "outcome_price": row["price_over"],
                        "outcome_point": total,
                        "ts": ts,
                        "_source_ts_ms": row.get("_source_ts_ms"),
                    }
                )

            if row.get("price_under") is not None and total is not None:
                converted_rows.append(
                    {
                        "event_id": event_id,
                        "book": book,
                        "brand": brand,
                        "market": "total",
                        "outcome_name": f"Under {total}",
                        "outcome_price": row["price_under"],
                        "outcome_point": total,
                        "ts": ts,
                        "_source_ts_ms": row.get("_source_ts_ms"),
                    }
                )

        # If no specific prices but has metadata, keep as placeholder
        elif not any(
            [row.get(f"price_{side}") for side in ["home", "away", "over", "under"]]
        ):
            converted_rows.append(
                {
                    "event_id": event_id,
                    "book": book,
                    "brand": brand,
                    "market": market or "unknown",
                    "outcome_name": None,
                    "outcome_price": None,
                    "outcome_point": None,
                    "ts": ts,
                    "_source_ts_ms": row.get("_source_ts_ms"),
                }
            )

    return converted_rows


def decimal_to_american(decimal_odds):
    """Convert decimal odds to American odds"""
    if not decimal_odds or decimal_odds <= 1.0:
        return None

    if decimal_odds >= 2.0:
        return int(round((decimal_odds - 1.0) * 100))
    else:
        return int(round(-100 / (decimal_odds - 1.0)))
