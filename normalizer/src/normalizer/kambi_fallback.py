import json
import logging
from typing import Dict, List, Any
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

def kambi_fallback_extract_betrivers(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Fallback extractor for BetRivers when primary Kambi mapper returns 0 rows.
    Robust parser for liveEvents data with safe defaults.
    """
    rows = []
    
    try:
        # Robust JSON handling
        if isinstance(payload, str):
            try:
                data = json.loads(payload)
            except json.JSONDecodeError:
                logger.warning("FALLBACK_PARSE_ERR: Invalid JSON payload")
                return []
        elif isinstance(payload, dict):
            data = payload
        else:
            logger.warning("FALLBACK_PARSE_ERR: Unexpected payload type")
            return []
        
        # Find liveEvents (case-insensitive search)
        live_events = None
        for key in ['liveEvents', 'liveevents', 'events', 'Events']:
            if key in data and isinstance(data[key], list):
                live_events = data[key]
                break
        
        if not live_events:
            logger.info("FALLBACK_SCAN liveEvents=0 candidates=0 outcomes=0")
            return []
        
        markets_found = 0
        outcomes_found = 0
        
        for event_node in live_events:
            if not isinstance(event_node, dict):
                continue
                
            # Extract event_id
            event_id = None
            for id_path in [
                ["event", "id"],
                ["id"],
                ["eventId"],
                ["event_id"]
            ]:
                try:
                    val = event_node
                    for key in id_path:
                        val = val[key]
                    if val:
                        event_id = str(val)
                        break
                except (KeyError, TypeError):
                    continue
            
            if not event_id:
                continue
            
            # Scan for markets/betOffers
            market_containers = []
            for container_key in ['markets', 'betOffers', 'betoffers', 'offers']:
                container = event_node.get(container_key)
                if isinstance(container, list):
                    market_containers.extend(container)
            
            markets_found += len(market_containers)
            
            for market in market_containers:
                if not isinstance(market, dict):
                    continue
                
                # Determine market type
                market_name = (market.get('name', '') or market.get('criterionLabel', '')).lower()
                if 'moneyline' in market_name or 'winner' in market_name or 'match' in market_name:
                    market_type = 'h2h'
                elif 'spread' in market_name or 'handicap' in market_name:
                    market_type = 'spread'  
                elif 'total' in market_name or 'over/under' in market_name:
                    market_type = 'total'
                else:
                    market_type = 'h2h'  # default
                
                # Extract outcomes
                outcomes = market.get('outcomes', [])
                if not isinstance(outcomes, list):
                    continue
                
                outcomes_found += len(outcomes)
                
                for i, outcome in enumerate(outcomes):
                    if not isinstance(outcome, dict):
                        continue
                    
                    # Extract price
                    price_american = None
                    for price_key in ['oddsAmerican', 'americanOdds', 'american', 'oddsUS']:
                        val = outcome.get(price_key)
                        if val is not None:
                            try:
                                price_american = int(str(val).replace('+', ''))
                                break
                            except (ValueError, TypeError):
                                continue
                    
                    # Convert from decimal if american not found
                    if price_american is None:
                        for decimal_key in ['odds', 'decimal', 'oddsDecimal']:
                            val = outcome.get(decimal_key)
                            if val is not None:
                                try:
                                    decimal = float(val)
                                    if decimal >= 2.0:
                                        price_american = int((decimal - 1.0) * 100)
                                    elif decimal > 1.0:
                                        price_american = int(-100 / (decimal - 1.0))
                                    break
                                except (ValueError, TypeError):
                                    continue
                    
                    if price_american is None:
                        continue
                    
                    # Determine side
                    outcome_name = (outcome.get('name', '') or outcome.get('label', '')).lower()
                    if 'home' in outcome_name or (market_type == 'h2h' and i == 0):
                        side = 'home'
                    elif 'away' in outcome_name or (market_type == 'h2h' and i == 1):
                        side = 'away'
                    elif 'over' in outcome_name:
                        side = 'over'
                    elif 'under' in outcome_name:
                        side = 'under'
                    else:
                        side = 'home' if i == 0 else 'away'  # fallback
                    
                    # Create row
                    row = {
                        'book': 'betrivers',
                        'event_id': event_id,
                        'market': market_type,
                        'line': outcome.get('line') or market.get('line'),
                        'total': None,
                        'price_home': price_american if side == 'home' else None,
                        'price_away': price_american if side == 'away' else None,
                        'price_over': price_american if side == 'over' else None,
                        'price_under': price_american if side == 'under' else None,
                    }
                    
                    if market_type == 'total' and row['line']:
                        row['total'] = row['line']
                        row['line'] = None
                    
                    rows.append(row)
                    
                    logger.info("FALLBACK_EMIT event_id=%s market=%s side=%s price=%s", 
                              event_id, market_type, side, price_american)
        
        logger.info("FALLBACK_SCAN liveEvents=%d candidates=%d outcomes=%d", 
                   len(live_events), markets_found, outcomes_found)
                   
    except Exception as e:
        logger.warning("FALLBACK_PARSE_ERR: %s", str(e))
        return []
    
    return rows
