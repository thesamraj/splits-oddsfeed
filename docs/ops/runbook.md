# Runbook (Production)
Live books:
- BetRivers (BR) + SugarHouse alias
- FanDuel
- DraftKings
- PointsBet (host collector)
- ESPN BET (Barstool alias, host collector)
- Bovada (real collector, 657k+ ticks backfilled)

## Key Points
- PB/ESPN BET host collectors publish to redis://localhost:6379
- Aliases: sugarhouse → betrivers, barstool ↔ espnbet
- Smoke: `scripts/smoke_all.sh` (API + DB + ticks)
- Backup: `scripts/backup_now.sh`
- Freeze tags: br-*, fd-*, dk-*, pb-*, espnbet-*
