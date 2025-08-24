#!/usr/bin/env python3

import sys

# Add the normalizer source to path
sys.path.insert(0, "./normalizer/src")

from normalizer.kambi_mapper import (
    _iter_betoffers_anywhere,
    _market_name,
    _side_from_label,
    _to_american_from_decimal,
)

# Sample envelope based on the real data we saw
sample_envelope = {
    "event_id": "1023693113",
    "url": "https://pa.betrivers.com/",
    "payload": {
        "betOffers": [
            {
                "id": 2518973088,
                "criterion": {
                    "id": 1001159490,
                    "label": "Total Points",
                    "englishLabel": "Total Points - Including Overtime",
                },
                "betOfferType": {"id": 6, "name": "Over/Under"},
                "eventId": 1023693113,
                "outcomes": [
                    {
                        "id": 3739508166,
                        "label": "Under",
                        "odds": 1860,
                        "line": 46500,
                        "type": "OT_UNDER",
                        "oddsAmerican": "-117",
                    },
                    {
                        "id": 3739508168,
                        "label": "Over",
                        "odds": 1940,
                        "line": 46500,
                        "type": "OT_OVER",
                        "oddsAmerican": "-107",
                    },
                ],
            }
        ]
    },
}


def main():
    print("=== Debugging Kambi Mapper Step by Step ===")

    payload = sample_envelope.get("payload") or sample_envelope.get("data") or {}
    print(f"Payload type: {type(payload)}")

    print("\n=== Testing _iter_betoffers_anywhere ===")
    bet_offers = list(_iter_betoffers_anywhere(payload))
    print(f"Found {len(bet_offers)} bet offers")

    for i, bo in enumerate(bet_offers):
        print(f"\nBet Offer {i+1}:")
        print(f"  ID: {bo.get('id')}")

        crit = bo.get("criterion") or {}
        label = crit.get("label", "")
        print(f"  Criterion label: '{label}'")

        # Test market name mapping
        mkt = _market_name(label)
        print(f"  Market name result: {mkt}")

        if not mkt:
            print("  SKIPPED: No market match")
            continue

        # Test line/total extraction
        line = None
        total = None
        if "line" in bo and isinstance(bo["line"], (int, float)):
            line = (
                int(bo["line"])
                if isinstance(bo["line"], int)
                else int(round(float(bo["line"])))
            )
            print(f"  Line: {line}")

        if mkt == "totals":
            total = line
            line = None
            print(f"  Converted to total: {total}")

        # Test outcomes
        outs = bo.get("outcomes") or []
        print(f"  Found {len(outs)} outcomes")

        for j, oc in enumerate(outs):
            print(f"    Outcome {j+1}:")
            label = oc.get("label", "")
            print(f"      Label: '{label}'")

            side = _side_from_label(label, j)
            print(f"      Side: {side}")

            # Test odds conversion
            american = None
            odds_obj = oc.get("odds")
            print(f"      Raw odds: {odds_obj}")

            if isinstance(odds_obj, dict):
                print("      Odds is dict - checking for american/decimal")
                if "american" in odds_obj and odds_obj["american"] not in (None, 0):
                    american = int(odds_obj["american"])
                    print(f"      Found american: {american}")
                elif "decimal" in odds_obj and odds_obj["decimal"] not in (None, 0):
                    print(f"      Found decimal: {odds_obj['decimal']}")
                    if isinstance(odds_obj["decimal"], int):
                        # Kambi scaled
                        dec = odds_obj["decimal"] / 1000.0
                        american = _to_american_from_decimal(dec)
                        print(
                            f"      Converted scaled decimal {odds_obj['decimal']} -> {dec} -> {american}"
                        )
                    else:
                        american = _to_american_from_decimal(float(odds_obj["decimal"]))
                        print(
                            f"      Converted decimal {odds_obj['decimal']} -> {american}"
                        )
            elif isinstance(odds_obj, (int, float)):
                # Direct odds value, assume it's Kambi scaled
                print(f"      Direct odds value: {odds_obj}")
                dec = odds_obj / 1000.0
                american = _to_american_from_decimal(dec)
                print(
                    f"      Converted direct scaled {odds_obj} -> {dec} -> {american}"
                )

            print(f"      Final american odds: {american}")

            if american is None:
                print("      SKIPPED: No valid odds")
                continue


if __name__ == "__main__":
    main()
