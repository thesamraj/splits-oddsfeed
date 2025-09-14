# BoltOdds Schema Map (Analyzed)

Total frames analyzed: 10969

## Action: `initial_state`

**Count:** 10960 (99.9%)

**Sports:** Brazil Serie A, Bundesliga, CSL, Challenger Biella, Challenger Guangzhou, Challenger Rennes, Challenger Szczecin, Challenger Targu Mures, Challenger Winston Salem, Colombian Primera A, EPL, LP Argentina, La Liga, Liga MX, Ligue 1, MLB, MLS, NCAAF, NFL, Primeira Liga, Serie A, WNBA, WTA Guadalajara, WTA Sao Paulo
**Books:** betmgm, draftkings, espnbet, neobet, sportsinteraction, thescore
**Markets:** Moneyline, Spread, Total

**Fields:**
- `action`: required
- `data`: required
- `timestamp`: required

**Example:**
```json
{
  "timestamp": "2025-09-08T20:32:52.889016+00:00",
  "action": "initial_state",
  "data": {
    "sport": "NFL",
    "sportsbook": "draftkings",
    "game": "Chicago Bears vs Minnesota Vikings, 2025-09-08, 08",
    "universal_game_id": "80cfa8281e36",
    "home_team": "Chicago Bears",
    "away_team": "Minnesota Vikings",
    "info": {
      "game_id": "32225523",
      "when": "2025-09-08, 08:15 PM",
      "link": "https://sportsbook.draftkings.com/event/min-vikings-%40-chi-bears/32225523",
      "game": "Chicago Bears vs Minnesota Vikings, 2025-09-08, 08",
      "orig_teams": "CHI Bears vs MIN Vikings",
      "universal_id": "80cfa8281e36"
    },
    "outcomes": {
      "Minnesota Vikings Moneyline": {
        "odds": "-118",
        "link": "https://sportsbook.draftkings.com/event/min-vikings-%40-chi-bears/32225523?outcomes=0ML79742904_3",
        "outcome_name": "Moneyline",
        "outcome_line": null,
        "outcome_over_under": null,
        "outcome_target": "Minnesota Vikings"
      },
      "...": "truncated"
    }
  }
}
```

## Action: `socket_connected`

**Count:** 9 (0.1%)


**Fields:**
- `action`: required
