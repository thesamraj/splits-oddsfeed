#!/usr/bin/env python3
"""
Odds Converter Utility
Handles conversion between American, Decimal, and Fractional odds formats
"""


def convert_american_to_decimal(american_odds):
    """
    Convert American odds to decimal format

    American odds examples:
    -110 (bet $110 to win $100) -> 1.909
    +150 (bet $100 to win $150) -> 2.500

    Args:
        american_odds: Integer or float American odds value

    Returns:
        Decimal odds rounded to 3 decimal places
    """
    try:
        american_odds = float(american_odds)

        if american_odds > 0:
            # Positive American odds (underdog)
            return round((american_odds / 100) + 1, 3)
        else:
            # Negative American odds (favorite)
            return round((100 / abs(american_odds)) + 1, 3)
    except (ValueError, TypeError, ZeroDivisionError):
        return None


def convert_decimal_to_american(decimal_odds):
    """
    Convert decimal odds to American format

    Args:
        decimal_odds: Decimal odds value (e.g., 1.909)

    Returns:
        American odds as integer
    """
    try:
        decimal_odds = float(decimal_odds)

        if decimal_odds < 1.01:
            return None

        if decimal_odds >= 2.0:
            # Underdog (positive American odds)
            return int((decimal_odds - 1) * 100)
        else:
            # Favorite (negative American odds)
            return int(-100 / (decimal_odds - 1))
    except (ValueError, TypeError, ZeroDivisionError):
        return None


def validate_price(price, odds_format="decimal"):
    """
    Validate odds are within reasonable bounds

    Args:
        price: The odds value to validate
        odds_format: 'decimal', 'american', or 'fractional'

    Returns:
        Boolean indicating if price is valid
    """
    try:
        price = float(price)

        if odds_format == "decimal":
            # Decimal odds should be between 1.01 and 100
            return 1.01 <= price <= 100
        elif odds_format == "american":
            # American odds typically between -10000 and +10000
            return -10000 <= price <= 10000 and price != 0
        elif odds_format == "fractional":
            # Fractional stored as decimal (e.g., 1/2 = 0.5)
            return 0.01 <= price <= 100
        else:
            return False
    except (ValueError, TypeError):
        return False


def detect_odds_format(price):
    """
    Detect the format of odds based on the value

    Args:
        price: The odds value to analyze

    Returns:
        String: 'american', 'decimal', or 'unknown'
    """
    try:
        price = float(price)

        # American odds characteristics:
        # - Never between 0 and 1
        # - Negative values less than -100 or positive values greater than 100
        # - Often round numbers
        if price < -100 or price > 100:
            if price % 5 == 0:  # Often divisible by 5
                return "american"
            elif price < 0:  # Negative is likely American
                return "american"

        # Decimal odds characteristics:
        # - Always positive
        # - Usually between 1.01 and 20
        if 1.01 <= price <= 20:
            return "decimal"

        # Edge cases
        if price < 0:
            return "american"
        elif 0 < price < 1:
            return "fractional"
        elif 20 < price <= 100:
            return "american"  # Likely American underdog

        return "unknown"

    except (ValueError, TypeError):
        return "unknown"


def normalize_odds(price, source_book=None):
    """
    Intelligently normalize odds to decimal format

    Args:
        price: The raw odds value
        source_book: Optional book name for book-specific logic

    Returns:
        Normalized decimal odds or None if invalid
    """
    if price is None:
        return None

    # Detect format
    format_detected = detect_odds_format(price)

    # Book-specific overrides
    if source_book in ["bovada", "draftkings", "fanduel", "betmgm", "pointsbet"]:
        # These books typically use American odds
        format_detected = "american"
    elif source_book in ["pinnacle", "bet365"]:
        # These may use decimal
        if format_detected == "unknown" and price > 1:
            format_detected = "decimal"

    # Convert based on detected format
    if format_detected == "american":
        decimal_odds = convert_american_to_decimal(price)
    elif format_detected == "decimal":
        decimal_odds = float(price)
    else:
        # Unknown format, try to make sense of it
        if price < 0:
            # Negative, assume American
            decimal_odds = convert_american_to_decimal(price)
        elif 1.01 <= price <= 100:
            # In decimal range, use as-is
            decimal_odds = float(price)
        else:
            return None

    # Validate the result
    if decimal_odds and validate_price(decimal_odds, "decimal"):
        return round(decimal_odds, 3)

    return None


# Testing
if __name__ == "__main__":
    test_cases = [
        (-110, "American favorite"),
        (+150, "American underdog"),
        (1.909, "Decimal"),
        (-250, "Heavy favorite"),
        (+500, "Big underdog"),
        (2.5, "Decimal even"),
        (-10000, "Extreme favorite"),
        (0.5, "Fractional"),
    ]

    print("Odds Converter Tests:")
    print("-" * 50)

    for price, description in test_cases:
        detected = detect_odds_format(price)
        normalized = normalize_odds(price)
        print(
            f"{description:20} | Input: {price:7} | Detected: {detected:8} | Output: {normalized}"
        )
