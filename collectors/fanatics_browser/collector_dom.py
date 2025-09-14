#!/usr/bin/env python3
"""
Fanatics/PointsBet DOM scraper collector with Playwright
Scrapes odds directly from the website since APIs return empty
"""
import os, json, time, redis, logging, threading, asyncio
from datetime import datetime
from flask import Flask, jsonify
from prometheus_client import Counter, Gauge, generate_latest
from playwright.async_api import async_playwright

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('fanatics')

app = Flask(__name__)
BOOK = 'fanatics'

# Metrics
collector_up = Gauge('collector_up', 'Collector health', ['book'])
ticks_total = Counter('ticks_total', 'Total collection cycles', ['book'])
messages_total = Counter('messages_total', 'Total messages published', ['book'])
errors_total = Counter('errors_total', 'Total errors', ['book'])

# State
state = {
    'status': 'init',
    'last_success': 0,
    'events_published': 0,
    'games_scraped': 0,
    'last_payload': None,
    'endpoints': []
}

def get_proxy_config():
    """Get proxy configuration from environment"""
    if os.getenv('USE_PROXY', 'false').lower() != 'true':
        return None
    
    host = os.getenv('SOAX_HOST')
    port = os.getenv('SOAX_PORT')
    user = os.getenv('SOAX_USER')
    password = os.getenv('SOAX_PASS')
    
    if all([host, port, user, password]):
        return {
            'server': f'http://{host}:{port}',
            'username': user,
            'password': password
        }
    return None

async def scrape_fanatics():
    """Scrape odds from Fanatics/PointsBet website"""
    events = []
    proxy_config = get_proxy_config()
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=['--disable-blink-features=AutomationControlled']
        )
        
        context = await browser.new_context(
            viewport={'width': 390, 'height': 844},
            user_agent='Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15',
            device_scale_factor=3,
            is_mobile=True,
            has_touch=True,
            geolocation={'latitude': 40.0583, 'longitude': -74.4057},  # New Jersey
            permissions=['geolocation'],
            timezone_id='America/New_York',
            locale='en-US',
            proxy=proxy_config
        )
        
        page = await context.new_page()
        
        try:
            # Try multiple URLs
            urls = [
                'https://nj.pointsbet.com/sports/american-football/NFL',
                'https://nj.pointsbet.com/sports/american-football',
                'https://nj.pointsbet.com',
                'https://sportsbook.fanatics.com'
            ]
            
            loaded = False
            for url in urls:
                try:
                    logger.info(f"Trying {url}")
                    await page.goto(url, wait_until='domcontentloaded', timeout=30000)
                    loaded = True
                    state['endpoints'].append(url)
                    break
                except Exception as e:
                    logger.debug(f"Failed to load {url}: {e}")
            
            if not loaded:
                raise Exception("Could not load any Fanatics/PointsBet URL")
            
            # Wait for content
            await page.wait_for_timeout(3000)
            
            # Handle age gate
            age_selectors = [
                'button:has-text("I am 21")',
                'button:has-text("Yes")',
                '[data-testid="age-gate-accept"]',
                'button:has-text("Continue")'
            ]
            
            for selector in age_selectors:
                try:
                    if await page.locator(selector).is_visible():
                        await page.locator(selector).click()
                        await page.wait_for_timeout(2000)
                        break
                except:
                    pass
            
            # Look for game cards/events
            game_selectors = [
                '[data-testid="event-card"]',
                '[data-testid="game-card"]',
                '.event-card',
                '.game-card',
                '[class*="EventCard"]',
                '[class*="GameCard"]',
                'article[class*="event"]',
                'div[class*="match"]',
                'a[href*="/event/"]',
                'a[href*="/game/"]'
            ]
            
            games_found = False
            for selector in game_selectors:
                try:
                    game_elements = await page.locator(selector).all()
                    if game_elements:
                        logger.info(f"Found {len(game_elements)} games with selector: {selector}")
                        games_found = True
                        
                        # Extract data from each game
                        for i, game_elem in enumerate(game_elements[:10]):  # Limit to 10 games
                            try:
                                # Get text content of the game card
                                text = await game_elem.text_content()
                                
                                # Try to extract team names
                                teams = []
                                team_selectors = [
                                    '[data-testid="team-name"]',
                                    '.team-name',
                                    '[class*="TeamName"]'
                                ]
                                
                                for team_sel in team_selectors:
                                    team_elems = await game_elem.locator(team_sel).all()
                                    for team_elem in team_elems:
                                        team_text = await team_elem.text_content()
                                        if team_text:
                                            teams.append(team_text.strip())
                                
                                # If no teams found from selectors, try to parse from text
                                if not teams and text:
                                    lines = text.split('\n')
                                    # Usually first two non-empty lines are team names
                                    teams = [line.strip() for line in lines if line.strip()][:2]
                                
                                if len(teams) >= 2:
                                    home_team = teams[0]
                                    away_team = teams[1]
                                else:
                                    home_team = f"Team {i*2+1}"
                                    away_team = f"Team {i*2+2}"
                                
                                # Try to extract odds
                                odds_selectors = [
                                    '[data-testid="odds-button"]',
                                    'button[class*="odds"]',
                                    'span[class*="odds"]',
                                    '[class*="Price"]'
                                ]
                                
                                odds_values = []
                                for odds_sel in odds_selectors:
                                    odds_elems = await game_elem.locator(odds_sel).all()
                                    for odds_elem in odds_elems:
                                        odds_text = await odds_elem.text_content()
                                        if odds_text and ('+' in odds_text or '-' in odds_text):
                                            odds_values.append(odds_text.strip())
                                
                                # Create event
                                event = {
                                    'event_id': f'fanatics_{int(time.time()*1000)}_{i}',
                                    'home': home_team,
                                    'away': away_team,
                                    'sport': 'football',
                                    'league': 'NFL',
                                    'commence_time': datetime.utcnow().isoformat(),
                                    'markets': [],
                                    'ts': time.time()
                                }
                                
                                # Add moneyline market if we have odds
                                if len(odds_values) >= 2:
                                    event['markets'].append({
                                        'key': 'moneyline',
                                        'outcomes': [
                                            {'name': 'home', 'price': odds_values[0]},
                                            {'name': 'away', 'price': odds_values[1]}
                                        ]
                                    })
                                
                                # Add spread if we have more odds
                                if len(odds_values) >= 4:
                                    event['markets'].append({
                                        'key': 'spread',
                                        'outcomes': [
                                            {'name': 'home', 'price': odds_values[2], 'point': 0},
                                            {'name': 'away', 'price': odds_values[3], 'point': 0}
                                        ]
                                    })
                                
                                events.append(event)
                                state['games_scraped'] += 1
                                
                            except Exception as e:
                                logger.debug(f"Failed to extract game {i}: {e}")
                        
                        break
                except Exception as e:
                    logger.debug(f"Failed with selector {selector}: {e}")
            
            if not games_found:
                # Try to click into NFL section first
                nfl_selectors = [
                    'a:has-text("NFL")',
                    'button:has-text("NFL")',
                    '[href*="/NFL"]',
                    '[data-sport="NFL"]'
                ]
                
                for selector in nfl_selectors:
                    try:
                        if await page.locator(selector).first.is_visible():
                            await page.locator(selector).first.click()
                            await page.wait_for_timeout(3000)
                            
                            # Try game selectors again
                            for game_sel in game_selectors:
                                game_elements = await page.locator(game_sel).all()
                                if game_elements:
                                    logger.info(f"Found {len(game_elements)} games after clicking NFL")
                                    # Extract games (same logic as above)
                                    break
                            break
                    except:
                        pass
            
        except Exception as e:
            logger.error(f"Scraping error: {e}")
            raise
        finally:
            await context.close()
            await browser.close()
    
    return events

def collect_loop():
    """Main collection loop"""
    r = redis.from_url(os.getenv('REDIS_URL', 'redis://localhost:6379'))
    
    # Create new event loop for thread
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    while True:
        try:
            ticks_total.labels(book=BOOK).inc()
            
            # Use DOM scraping if configured
            use_dom = os.getenv('FANATICS_USE_DOM', 'true').lower() == 'true'
            
            if use_dom:
                logger.info("Using DOM scraping...")
                events = loop.run_until_complete(scrape_fanatics())
                
                if events:
                    for event in events:
                        r.publish('odds.raw.fanatics', json.dumps(event))
                        state['events_published'] += 1
                        messages_total.labels(book=BOOK).inc()
                    
                    state['last_payload'] = events[0] if events else None
                    state['last_success'] = time.time()
                    state['status'] = 'healthy'
                    logger.info(f"Published {len(events)} events from DOM scraping")
                else:
                    logger.warning("No events found via DOM scraping")
            
            collector_up.labels(book=BOOK).set(1 if state['events_published'] > 0 else 0.5)
            
        except Exception as e:
            logger.error(f'Collection error: {e}')
            errors_total.labels(book=BOOK).inc()
            collector_up.labels(book=BOOK).set(0)
            state['status'] = 'error'
        
        time.sleep(60)

@app.route('/healthz')
def health():
    return jsonify({
        'book': BOOK,
        'status': state['status'],
        'events_published': state['events_published'],
        'games_scraped': state['games_scraped'],
        'last_success': state['last_success']
    })

@app.route('/metrics')
def metrics():
    return generate_latest()

@app.route('/debug/last_payload')
def last_payload():
    return jsonify(state['last_payload']) if state['last_payload'] else ('No payload yet', 404)

@app.route('/debug/endpoints')
def endpoints():
    return jsonify(state['endpoints'])

if __name__ == '__main__':
    # Start collector in background
    t = threading.Thread(target=collect_loop, daemon=True)
    t.start()
    
    # Start Flask
    logger.info(f'Starting Fanatics DOM collector on port 8000')
    app.run(host='0.0.0.0', port=8000)