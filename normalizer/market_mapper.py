#!/usr/bin/env python3
"""
Market Standardization Layer
Maps various market names from different sportsbooks to standard format
"""


class MarketMapper:
    """Standardizes market names across all sportsbooks"""

    # Core market mappings
    MARKET_MAPPINGS = {
        # Moneyline variations
        "moneyline": "h2h",
        "ml": "h2h",
        "money_line": "h2h",
        "money line": "h2h",
        "head_to_head": "h2h",
        "head to head": "h2h",
        "match_winner": "h2h",
        "match winner": "h2h",
        "winner": "h2h",
        "1x2": "h2h",
        "home/away": "h2h",
        # Spread variations
        "spread": "spread",
        "spreads": "spread",
        "handicap": "spread",
        "point_spread": "spread",
        "point spread": "spread",
        "asian_handicap": "spread",
        "asian handicap": "spread",
        "line": "spread",
        "points": "spread",
        "run_line": "spread",
        "run line": "spread",
        "puck_line": "spread",
        "puck line": "spread",
        # Total variations
        "total": "total",
        "totals": "total",
        "over_under": "total",
        "over/under": "total",
        "o/u": "total",
        "ou": "total",
        "game_total": "total",
        "game total": "total",
        "total_points": "total",
        "total points": "total",
        "total_goals": "total",
        "total goals": "total",
        "total_runs": "total",
        "total runs": "total",
        # Props
        "player_props": "props",
        "player props": "props",
        "prop": "props",
        "proposition": "props",
        "specials": "props",
        # Futures
        "futures": "futures",
        "outrights": "futures",
        "championship": "futures",
        "winner": "futures",
        # Parlays
        "parlay": "parlay",
        "parlays": "parlay",
        "combo": "parlay",
        "accumulator": "parlay",
        "acca": "parlay",
    }

    # Book-specific mappings
    BOOK_SPECIFIC = {
        "draftkings": {
            "game_lines": "h2h",
            "game_spreads": "spread",
            "game_totals": "total",
            "alternate_spreads": "spread",
            "alternate_totals": "total",
        },
        "fanduel": {
            "match_result": "h2h",
            "point_spread": "spread",
            "total_points": "total",
            "alt_spread": "spread",
            "alt_total": "total",
        },
        "bovada": {
            "game_lines": "h2h",
            "point_spread": "spread",
            "total": "total",
            "team_totals": "team_total",
        },
        "betmgm": {
            "money_line": "h2h",
            "spread": "spread",
            "game_total": "total",
            "team_total": "team_total",
        },
        "pinnacle": {
            "m": "h2h",  # Moneyline
            "s": "spread",  # Spread
            "t": "total",  # Total
            "tt": "team_total",  # Team total
        },
    }

    @classmethod
    def standardize(cls, market_name, book=None):
        """
        Standardize a market name to consistent format

        Args:
            market_name: Raw market name from sportsbook
            book: Optional book name for book-specific mappings

        Returns:
            Standardized market name
        """
        if not market_name:
            return "unknown"

        # Clean the market name
        clean_name = str(market_name).lower().strip()
        clean_name = clean_name.replace("-", "_").replace(" ", "_")

        # Check book-specific mappings first
        if book and book.lower() in cls.BOOK_SPECIFIC:
            book_mappings = cls.BOOK_SPECIFIC[book.lower()]
            if clean_name in book_mappings:
                return book_mappings[clean_name]

        # Check general mappings
        if clean_name in cls.MARKET_MAPPINGS:
            return cls.MARKET_MAPPINGS[clean_name]

        # Check without underscores
        no_underscore = clean_name.replace("_", "")
        if no_underscore in cls.MARKET_MAPPINGS:
            return cls.MARKET_MAPPINGS[no_underscore]

        # Check with spaces
        with_spaces = clean_name.replace("_", " ")
        if with_spaces in cls.MARKET_MAPPINGS:
            return cls.MARKET_MAPPINGS[with_spaces]

        # Default to original if no mapping found
        return market_name

    @classmethod
    def is_standard_market(cls, market_name):
        """Check if a market name is already in standard format"""
        standard_markets = [
            "h2h",
            "spread",
            "total",
            "props",
            "futures",
            "parlay",
            "team_total",
        ]
        return market_name in standard_markets

    @classmethod
    def get_display_name(cls, standard_market):
        """Get user-friendly display name for standard market"""
        display_names = {
            "h2h": "Moneyline",
            "spread": "Spread",
            "total": "Total",
            "props": "Player Props",
            "futures": "Futures",
            "parlay": "Parlay",
            "team_total": "Team Total",
        }
        return display_names.get(standard_market, standard_market.title())


# Testing
if __name__ == "__main__":
    test_cases = [
        ("moneyline", None, "h2h"),
        ("Money Line", None, "h2h"),
        ("spread", None, "spread"),
        ("Point Spread", None, "spread"),
        ("over/under", None, "total"),
        ("game_lines", "draftkings", "h2h"),
        ("match_result", "fanduel", "h2h"),
        ("m", "pinnacle", "h2h"),
        ("unknown_market", None, "unknown_market"),
    ]

    print("Market Mapper Tests:")
    print("-" * 50)

    for market, book, expected in test_cases:
        result = MarketMapper.standardize(market, book)
        status = "✓" if result == expected else "✗"
        print(
            f"{status} {market:20} ({book or 'generic':10}) -> {result:15} (expected: {expected})"
        )
