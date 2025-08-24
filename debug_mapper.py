#!/usr/bin/env python3

import sys

# Add the normalizer source to path
sys.path.insert(0, "./normalizer/src")

from normalizer.kambi_mapper import normalize_kambi_envelope, extract_event_metadata

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
            },
            {
                "id": 2519734593,
                "criterion": {
                    "id": 1001159928,
                    "label": "Point Spread",
                    "englishLabel": "Point Spread - Including Overtime",
                },
                "betOfferType": {"id": 1, "name": "Handicap"},
                "eventId": 1023717346,
                "outcomes": [
                    {
                        "id": 3742343327,
                        "label": "NY Jets",
                        "odds": 1910,
                        "line": 3000,
                        "participant": "NY Jets",
                        "type": "OT_ONE",
                        "oddsAmerican": "-110",
                    },
                    {
                        "id": 3742343330,
                        "label": "PIT Steelers",
                        "odds": 1880,
                        "line": -3000,
                        "participant": "PIT Steelers",
                        "type": "OT_TWO",
                        "oddsAmerican": "-114",
                    },
                ],
            },
            {
                "id": 2546842989,
                "criterion": {
                    "id": 1001159860,
                    "label": "Moneyline",
                    "englishLabel": "Moneyline",
                },
                "betOfferType": {"id": 2, "name": "Match"},
                "eventId": 1022037100,
                "outcomes": [
                    {
                        "id": 3845835503,
                        "label": "MIA Marlins",
                        "odds": 1700,
                        "participant": "MIA Marlins",
                        "type": "OT_ONE",
                        "oddsAmerican": "-143",
                    },
                    {
                        "id": 3845835507,
                        "label": "STL Cardinals",
                        "odds": 2200,
                        "participant": "STL Cardinals",
                        "type": "OT_TWO",
                        "oddsAmerican": "120",
                    },
                ],
            },
        ]
    },
}


def main():
    print("=== Testing Kambi Mapper ===")
    print(f"Sample envelope event_id: {sample_envelope.get('event_id')}")
    print(f"Sample envelope URL: {sample_envelope.get('url')}")
    print(f"Number of betOffers: {len(sample_envelope['payload']['betOffers'])}")

    print("\n=== Testing normalize_kambi_envelope ===")
    try:
        rows = normalize_kambi_envelope(sample_envelope)
        print(f"Returned {len(rows)} rows")

        for i, row in enumerate(rows):
            print(f"\nRow {i+1}:")
            for key, value in row.items():
                if not key.startswith("_"):  # Skip internal fields
                    print(f"  {key}: {value}")

    except Exception as e:
        print(f"Error in normalize_kambi_envelope: {e}")
        import traceback

        traceback.print_exc()

    print("\n=== Testing extract_event_metadata ===")
    try:
        metadata = extract_event_metadata(sample_envelope)
        print("Event metadata:")
        for key, value in metadata.items():
            print(f"  {key}: {value}")
    except Exception as e:
        print(f"Error in extract_event_metadata: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()
