# BoltOdds → Canonical Schema Mapping

## Overview
This document maps BoltOdds message fields to our canonical odds feed schema.

## Canonical Schema Fields
```
event_id    - Unique identifier for the game/match
teams       - Home and away team names
league      - Sport/league identifier  
market      - Market type (ML, SPREAD, TOTAL)
side        - Which side of the market (home/away for ML/spread, over/under for totals)
price       - Odds value (American format)
line        - Line value for spreads/totals
timestamp   - When the odds were captured
book        - Sportsbook identifier
```

## Message Type: `initial_state`
Full snapshot of all outcomes for a game/book combination.

### Field Mappings
| BoltOdds Field | Canonical Field | Notes |
|----------------|-----------------|-------|
| `data.universal_game_id` | `event_id` | Consistent across all books |
| `data.home_team` | `teams.home` | Direct mapping |
| `data.away_team` | `teams.away` | Direct mapping |
| `data.sport` | `league` | Sport/league identifier |
| `data.sportsbook` | `book` | Sportsbook name |
| `timestamp` | `timestamp` | ISO timestamp |
| `data.outcomes[*]` | Various | Parse outcome_name for market type |

### Market Detection Logic
```python
if outcome['outcome_name'] == 'Moneyline':
    market = 'ML'
    side = 'home' if outcome['outcome_target'] == home_team else 'away'
elif outcome['outcome_name'] == 'Spread':
    market = 'SPREAD'
    side = 'home' if outcome['outcome_target'] == home_team else 'away'
    line = outcome['outcome_line']
elif outcome['outcome_over_under'] in ['Over', 'Under']:
    market = 'TOTAL'
    side = outcome['outcome_over_under'].lower()
    line = float(outcome['outcome_line'])
```

### Examples

#### Example 1: Moneyline
```json
{
  "timestamp": "2025-09-08T10:52:34.472825+00:00",
  "action": "initial_state",
  "data": {
    "sport": "NFL",
    "sportsbook": "draftkings",
    "universal_game_id": "80cfa8281e36",
    "home_team": "Chicago Bears",
    "away_team": "Minnesota Vikings",
    "outcomes": {
      "Minnesota Vikings Moneyline": {
        "odds": "-122",
        "outcome_name": "Moneyline",
        "outcome_target": "Minnesota Vikings"
      }
    }
  }
}
```
**Normalized**: event_id=80cfa8281e36, league=NFL, market=ML, side=away, price=-122, book=draftkings

#### Example 2: Spread
```json
{
  "timestamp": "2025-09-08T10:52:34.472825+00:00",
  "action": "initial_state",
  "data": {
    "sport": "NFL",
    "sportsbook": "draftkings",
    "universal_game_id": "80cfa8281e36",
    "home_team": "Chicago Bears",
    "away_team": "Minnesota Vikings",
    "outcomes": {
      "Minnesota Vikings -1.5 Spread": {
        "odds": "-112",
        "outcome_name": "Spread",
        "outcome_line": -1.5,
        "outcome_target": "Minnesota Vikings"
      }
    }
  }
}
```
**Normalized**: event_id=80cfa8281e36, league=NFL, market=SPREAD, side=away, price=-112, line=-1.5, book=draftkings

#### Example 3: Total
```json
{
  "timestamp": "2025-09-08T10:52:35.123456+00:00",
  "action": "initial_state",
  "data": {
    "sport": "NFL",
    "sportsbook": "betmgm",
    "universal_game_id": "80cfa8281e36",
    "home_team": "Chicago Bears",
    "away_team": "Minnesota Vikings",
    "outcomes": {
      "Game Total Over 44.5": {
        "odds": "-110",
        "outcome_name": "Game Total",
        "outcome_line": "44.5",
        "outcome_over_under": "Over",
        "outcome_target": null
      }
    }
  }
}
```
**Normalized**: event_id=80cfa8281e36, league=NFL, market=TOTAL, side=over, price=-110, line=44.5, book=betmgm

## Message Type: `line_update`
Incremental update for specific outcomes.

### Field Mappings
Same as `initial_state` but typically contains only changed outcomes.

### Examples

#### Example 1: Line Movement
```json
{
  "timestamp": "2025-09-08T10:53:45.678901+00:00",
  "action": "line_update",
  "data": {
    "sport": "NBA",
    "sportsbook": "fanduel",
    "universal_game_id": "abc123def456",
    "home_team": "Los Angeles Lakers",
    "away_team": "Boston Celtics",
    "outcomes": {
      "Boston Celtics Moneyline": {
        "odds": "-145",
        "outcome_name": "Moneyline",
        "outcome_target": "Boston Celtics"
      }
    }
  }
}
```
**Normalized**: event_id=abc123def456, league=NBA, market=ML, side=away, price=-145, book=fanduel

#### Example 2: Spread Update
```json
{
  "timestamp": "2025-09-08T10:54:12.345678+00:00",
  "action": "line_update",
  "data": {
    "sport": "NBA",
    "sportsbook": "fanduel",
    "universal_game_id": "abc123def456",
    "home_team": "Los Angeles Lakers",
    "away_team": "Boston Celtics",
    "outcomes": {
      "Los Angeles Lakers 3.5 Spread": {
        "odds": "-105",
        "outcome_name": "Spread",
        "outcome_line": 3.5,
        "outcome_target": "Los Angeles Lakers"
      }
    }
  }
}
```
**Normalized**: event_id=abc123def456, league=NBA, market=SPREAD, side=home, price=-105, line=3.5, book=fanduel

#### Example 3: Total Update
```json
{
  "timestamp": "2025-09-08T10:55:23.456789+00:00",
  "action": "line_update",
  "data": {
    "sport": "NHL",
    "sportsbook": "caesars",
    "universal_game_id": "xyz789ghi012",
    "home_team": "New York Rangers",
    "away_team": "Toronto Maple Leafs",
    "outcomes": {
      "Total Under 6.5": {
        "odds": "+105",
        "outcome_name": "Total",
        "outcome_line": "6.5",
        "outcome_over_under": "Under"
      }
    }
  }
}
```
**Normalized**: event_id=xyz789ghi012, league=NHL, market=TOTAL, side=under, price=+105, line=6.5, book=caesars

## Message Type: `game_update`
Updates to game metadata (time, status, etc).

### Field Mappings
| BoltOdds Field | Canonical Field | Notes |
|----------------|-----------------|-------|
| `data.universal_game_id` | `event_id` | Game identifier |
| `data.info.when` | `game_time` | Parse to timestamp |
| `data.sport` | `league` | Sport/league |

### Examples

#### Example 1: Game Time Update
```json
{
  "timestamp": "2025-09-08T10:56:00.000000+00:00",
  "action": "game_update",
  "data": {
    "sport": "NFL",
    "sportsbook": "draftkings",
    "universal_game_id": "80cfa8281e36",
    "home_team": "Chicago Bears",
    "away_team": "Minnesota Vikings",
    "info": {
      "when": "2025-09-08, 08:20 PM",
      "game_id": "32225523"
    }
  }
}
```
**Normalized**: event_id=80cfa8281e36, league=NFL, game_time="2025-09-08 20:20:00"

#### Example 2: Status Change
```json
{
  "timestamp": "2025-09-08T11:00:00.000000+00:00",
  "action": "game_update",
  "data": {
    "sport": "NBA",
    "sportsbook": "betmgm",
    "universal_game_id": "nba456xyz",
    "home_team": "Miami Heat",
    "away_team": "Philadelphia 76ers",
    "status": "in_progress"
  }
}
```
**Normalized**: event_id=nba456xyz, league=NBA, status=in_progress

#### Example 3: Venue Update
```json
{
  "timestamp": "2025-09-08T11:15:00.000000+00:00",
  "action": "game_update",
  "data": {
    "sport": "MLB",
    "sportsbook": "pointsbet",
    "universal_game_id": "mlb789abc",
    "home_team": "New York Yankees",
    "away_team": "Boston Red Sox",
    "venue": "Yankee Stadium"
  }
}
```
**Normalized**: event_id=mlb789abc, league=MLB, venue="Yankee Stadium"

## Schema Gaps & Considerations

### Fields We Can Map
✅ Event ID (universal_game_id)
✅ Teams (home_team, away_team)
✅ League/Sport
✅ Market type (via outcome_name parsing)
✅ Price/Odds
✅ Line values
✅ Timestamp
✅ Sportsbook

### Fields Missing/Unclear
❌ **Game start time** - Only human-readable format in `info.when`
❌ **Live/pregame status** - Not explicitly provided
❌ **Market period** - No quarter/half/period info for non-game totals
❌ **Bet limits** - No max bet information
❌ **Market suspended** - No availability status
❌ **Player props** - Complex parsing needed for player-specific markets

### Special Handling Required
1. **American odds format** - Need to handle +/- prefix
2. **Outcome naming** - Inconsistent across books (e.g., "Total" vs "Game Total")
3. **Player props** - Outcome_target becomes player name instead of team
4. **Alternative lines** - Multiple spreads/totals for same game
5. **Line format** - Sometimes string ("44.5"), sometimes float (44.5), sometimes int (-1.5)