# Archive Directory

This directory contains **deprecated and sandbox configurations** that are not used in production.

## Purpose
- Reference implementations from development/testing
- Historical configurations for debugging
- Sandbox/mock services that must NOT be used in production

## Important Notes
- **CI excludes this directory** from security and quality checks
- **Do NOT symlink** these files into production compose configurations
- **Do NOT import** code from this directory in production services
- Use `docker-compose.prod.yml` for all production deployments

## Contents
- `docker-compose.*.yml.disabled` - Deprecated compose overrides with sandbox references
- Legacy collector configurations
- Test/mock service definitions

## Migration
If you need to resurrect any configuration:
1. Copy (don't move) the file out of archive
2. Remove all sandbox/test/mock references
3. Update to use realness gates and fail-soft patterns
4. Test thoroughly before production use

## Production Alternative
Use `docker-compose.prod.yml` which includes:
- Only real collectors with realness validation
- Neon-only database (no local Postgres)
- Standardized metrics (9090) and health (9091) ports
- Staged rollout: Bovada → Kambi → FanDuel → DraftKings → BetMGM → Pinnacle
