#!/bin/bash

# Unified Sportsbook Dashboard
# Real-time monitoring of all collectors

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color
BOLD='\033[1m'

# Function to get container status
get_container_status() {
    local name=$1
    if docker ps | grep -q "$name"; then
        echo -e "${GREEN}✓ UP${NC}"
    else
        echo -e "${RED}✗ DOWN${NC}"
    fi
}

# Function to get Redis channel subscribers
get_redis_subs() {
    local channel=$1
    subs=$(docker exec splits-oddsfeed-broker-1 redis-cli --no-auth-warning PUBSUB NUMSUB "$channel" 2>/dev/null | tail -1)
    if [ -z "$subs" ] || [ "$subs" = "0" ]; then
        echo -e "${RED}0${NC}"
    else
        echo -e "${GREEN}$subs${NC}"
    fi
}

# Function to get recent logs
get_recent_activity() {
    local container=$1
    local count=$(docker logs "$container" 2>&1 --tail 100 | grep -c "Published\|Stored\|Published.*events" 2>/dev/null || echo "0")
    if [ "$count" -gt 0 ]; then
        echo -e "${GREEN}Active ($count msgs)${NC}"
    else
        echo -e "${YELLOW}Idle${NC}"
    fi
}

# Main dashboard loop
while true; do
    clear

    echo -e "${BOLD}${CYAN}════════════════════════════════════════════════════════════════════${NC}"
    echo -e "${BOLD}${CYAN}           UNIFIED SPORTSBOOK ODDS COLLECTION DASHBOARD              ${NC}"
    echo -e "${BOLD}${CYAN}════════════════════════════════════════════════════════════════════${NC}"
    echo ""
    echo -e "Time: $(date '+%Y-%m-%d %H:%M:%S')"
    echo ""

    # Header
    printf "${BOLD}%-15s %-12s %-12s %-20s %-15s${NC}\n" "SPORTSBOOK" "COLLECTOR" "REDIS SUBS" "RECENT ACTIVITY" "SPECIAL NOTES"
    echo "────────────────────────────────────────────────────────────────────────────────"

    # DraftKings
    dk_status=$(get_container_status "draftkings")
    dk_ws_status=$(get_container_status "dk-websocket")
    dk_subs=$(get_redis_subs "odds.raw.draftkings")
    dk_activity=$(get_recent_activity "dk-websocket")
    printf "%-15s %-12s %-12s %-20s " "DraftKings" "$dk_status" "$dk_subs" "$dk_activity"
    if docker ps | grep -q "dk-websocket"; then
        echo -e "${BLUE}WebSocket Active${NC}"
    else
        echo ""
    fi

    # BetRivers
    br_status=$(get_container_status "betrivers\|br-prematch\|kambi")
    br_subs=$(get_redis_subs "odds.raw.kambi")
    br_activity=$(get_recent_activity "collector-br-prematch-a")
    printf "%-15s %-12s %-12s %-20s " "BetRivers" "$br_status" "$br_subs" "$br_activity"
    echo -e "${BLUE}Enhanced Mapper${NC}"

    # Barstool
    bs_status=$(get_container_status "barstool-collector")
    bs_subs=$(get_redis_subs "odds.raw.barstool")
    bs_activity=$(get_recent_activity "barstool-collector")
    printf "%-15s %-12s %-12s %-20s " "Barstool" "$bs_status" "$bs_subs" "$bs_activity"
    echo -e "${CYAN}ESPN API${NC}"

    # PointsBet
    pb_status=$(get_container_status "pointsbet")
    pb_subs=$(get_redis_subs "odds.raw.pointsbet")
    pb_activity=$(get_recent_activity "splits-oddsfeed-collector-pointsbet-1")
    printf "%-15s %-12s %-12s %-20s " "PointsBet" "$pb_status" "$pb_subs" "$pb_activity"
    echo -e "${GREEN}7s latency${NC}"

    # FanDuel
    fd_status=$(get_container_status "fanduel")
    fd_subs=$(get_redis_subs "odds.raw.fanduel")
    fd_activity=$(get_recent_activity "splits-oddsfeed-collector-fanduel-1")
    printf "%-15s %-12s %-12s %-20s " "FanDuel" "$fd_status" "$fd_subs" "$fd_activity"
    echo -e "${RED}Cloudflare Block${NC}"

    echo "────────────────────────────────────────────────────────────────────────────────"

    # New Books
    echo -e "${BOLD}NEW SPORTSBOOKS:${NC}"

    # Stake
    stake_status=$(get_container_status "stake-collector")
    stake_subs=$(get_redis_subs "odds.raw.stake")
    stake_activity=$(get_recent_activity "stake-collector")
    printf "%-15s %-12s %-12s %-20s " "Stake.com" "$stake_status" "$stake_subs" "$stake_activity"
    echo -e "${CYAN}Crypto-friendly${NC}"

    # Betano
    betano_status=$(get_container_status "betano-collector")
    betano_subs=$(get_redis_subs "odds.raw.betano")
    betano_activity=$(get_recent_activity "betano-collector")
    printf "%-15s %-12s %-12s %-20s " "Betano" "$betano_status" "$betano_subs" "$betano_activity"
    echo -e "${CYAN}European${NC}"

    # BetOnline
    betonline_status=$(get_container_status "betonline-collector")
    betonline_subs=$(get_redis_subs "odds.raw.betonline")
    betonline_activity=$(get_recent_activity "betonline-collector")
    printf "%-15s %-12s %-12s %-20s " "BetOnline" "$betonline_status" "$betonline_subs" "$betonline_activity"
    echo -e "${CYAN}Offshore${NC}"

    # Bovada
    bovada_status=$(get_container_status "bovada-collector")
    bovada_subs=$(get_redis_subs "odds.raw.bovada")
    bovada_activity=$(get_recent_activity "bovada-collector")
    printf "%-15s %-12s %-12s %-20s " "Bovada" "$bovada_status" "$bovada_subs" "$bovada_activity"
    echo -e "${CYAN}Major Offshore${NC}"

    echo ""
    echo "════════════════════════════════════════════════════════════════════"

    # System Statistics
    echo -e "${BOLD}SYSTEM STATISTICS:${NC}"

    # Count total containers
    total_collectors=$(docker ps | grep -c "collector\|websocket" || echo "0")
    echo -e "Total Collectors Running: ${GREEN}$total_collectors${NC}"

    # Redis status
    redis_status=$(docker ps | grep -q "broker" && echo -e "${GREEN}✓ Connected${NC}" || echo -e "${RED}✗ Disconnected${NC}")
    echo -e "Redis Broker: $redis_status"

    # Normalizer status
    norm_status=$(docker ps | grep -q "normalizer" && echo -e "${GREEN}✓ Processing${NC}" || echo -e "${RED}✗ Stopped${NC}")
    echo -e "Normalizer: $norm_status"

    # Database status
    db_status=$(docker ps | grep -q "store" && echo -e "${GREEN}✓ Online${NC}" || echo -e "${RED}✗ Offline${NC}")
    echo -e "Database: $db_status"

    echo ""
    echo "════════════════════════════════════════════════════════════════════"
    echo -e "${BOLD}Controls:${NC} Press Ctrl+C to exit | Dashboard refreshes every 10 seconds"

    sleep 10
done
