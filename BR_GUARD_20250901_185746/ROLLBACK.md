# Rollback Steps:
1) Restore previous collector:
   git reset --hard HEAD~1
   docker compose -f docker-compose.yml -f docker-compose.override.br-plus.yml up -d --build collector-br-http-plus

2) Remove retention function if undesired:
   docker exec splits-oddsfeed-store-1 psql -U odds -d oddsfeed -c "DROP FUNCTION IF EXISTS prune_old_data();"
