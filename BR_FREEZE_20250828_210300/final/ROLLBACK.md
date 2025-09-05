# ROLLBACK COMMANDS

If issues arise, use these commands to restore previous state:

## Database Rollback
```bash
# Remove odds_ticks data added during freeze
docker compose exec -T store psql -U odds -d oddsfeed -c "DELETE FROM odds_ticks WHERE created_at >= '2025-08-28 21:00:00';"
```

## Git Rollback
```bash
# Remove freeze tag
git tag -d br-freeze-20250828_210300

# Reset to pre-freeze state if needed
git reset --hard cdb3eb5  # pre-BR-only snapshot
```

## Service Rollback
```bash
# No service changes were made - all used existing infrastructure
# Services remain in br-only configuration as intended
```

## Configuration Restore
```bash
# Restore original docker-compose if needed
cp docker-compose.override.br-only.yml.bak docker-compose.override.br-only.yml
docker compose down && docker compose up -d
```
