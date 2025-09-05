# Rollback steps:
1) Disable plus collector:
   docker compose -f docker-compose.yml -f docker-compose.override.br-plus.yml down collector-br-http-plus

2) Revert patches:
   git reset --hard HEAD~1

3) Restart services if needed:
   docker compose up -d api normalizer dashboard
