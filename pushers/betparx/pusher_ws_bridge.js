const { chromium } = require('playwright');
const Redis = require('ioredis');
const express = require('express');
const pako = require('pako');

const BRAND = process.env.BRAND || 'betparx';
const HOME_URL = process.env.HOME_URL || 'https://pa.betparx.com/?page=sportsbook#live';
const REDIS_URL = process.env.REDIS_URL || 'redis://broker:6379/0';
const PORT = parseInt(process.env.PORT || '9118', 10);

const redis = new Redis(REDIS_URL, { lazyConnect: false });
const app = express();

let counters = {
  messages_received: 0,
  ws_json_published: 0,
  last_ok_ts: null,
  last_error: null,
  ws_urls: new Set()
};

function publishEnvelope(envelope) {
  try {
    const msg = JSON.stringify(envelope);
    return redis.publish('odds.raw.kambi', msg);
  } catch (e) {
    console.error('publish error', e);
    counters.last_error = String(e);
  }
}

function tryParseJSON(payloadData) {
  if (!payloadData) return null;

  // Method 1: Direct JSON
  try {
    if (payloadData.startsWith('{') || payloadData.startsWith('[')) {
      return JSON.parse(payloadData);
    }
  } catch (e) { /* Not direct JSON */ }

  // Method 2: Base64 JSON
  try {
    const decoded = Buffer.from(payloadData, 'base64').toString('utf8');
    if (decoded.startsWith('{') || decoded.startsWith('[')) {
      return JSON.parse(decoded);
    }
  } catch (e) { /* Not base64 JSON */ }

  // Method 3: Base64 + Deflate JSON
  try {
    const decodedBytes = Buffer.from(payloadData, 'base64');
    const inflated = pako.inflate(decodedBytes, { to: 'string' });
    if (inflated.startsWith('{') || inflated.startsWith('[')) {
      return JSON.parse(inflated);
    }
  } catch (e) { /* Not deflate JSON */ }

  return null;
}

function isOddsLikeJSON(obj) {
  if (!obj || typeof obj !== 'object') return false;

  const oddsKeys = ['events', 'event', 'markets', 'outcomes', 'competitors', 'live'];
  const hasOddsKey = oddsKeys.some(key => key in obj);

  if (hasOddsKey) return true;

  // Check if it's an array of event-like objects
  if (Array.isArray(obj) && obj.length > 0) {
    const firstItem = obj[0];
    if (typeof firstItem === 'object' && firstItem.id) {
      const hasEventFields = ['market', 'selection', 'competitor', 'outcome'].some(field =>
        JSON.stringify(firstItem).toLowerCase().includes(field)
      );
      if (hasEventFields) return true;
    }
  }

  return false;
}

(async () => {
  const browser = await chromium.launch({
    headless: true,
    args: ['--no-sandbox', '--disable-setuid-sandbox']
  });

  const context = await browser.newContext({
    viewport: { width: 1280, height: 860 },
    userAgent: 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    locale: 'en-US',
    timezoneId: 'America/New_York'
  });

  await context.addInitScript(() => {
    Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
    window.chrome = window.chrome || { runtime: {} };
  });

  const page = await context.newPage();
  const cdp = await context.newCDPSession(page);
  await cdp.send('Network.enable');
  await cdp.send('Target.setAutoAttach', {
    autoAttach: true,
    waitForDebuggerOnStart: false,
    flatten: true
  });

  // WebSocket URL capture
  cdp.on('Network.webSocketCreated', p => {
    if (p && p.url) counters.ws_urls.add(p.url);
  });

  // MEGA-WS-BRIDGE: Parse WebSocket frames for JSON data
  cdp.on('Network.webSocketFrameReceived', p => {
    const payload = p?.response?.payloadData || '';
    if (!payload) return;

    counters.messages_received++;

    // Always publish observability frame (existing behavior)
    publishEnvelope({
      brand_hint: BRAND,
      transport: 'ws-obsv',
      page_url: page.url(),
      frame_len: payload.length,
      frame_sample: payload.slice(0, 512),
      ts: new Date().toISOString()
    });

    // NEW: Try to parse as JSON and publish as HTTP-equivalent envelope
    const parsedJSON = tryParseJSON(payload);
    if (parsedJSON && isOddsLikeJSON(parsedJSON)) {
      const httpEquivalentEnvelope = {
        brand_hint: BRAND,
        transport: 'ws-json',
        source: 'kambi-ws',
        url: `wss://offering.kambi/ws?brand=${BRAND}`,
        payload: JSON.stringify(parsedJSON),
        content_type: 'application/json',
        ts: new Date().toISOString()
      };

      publishEnvelope(httpEquivalentEnvelope);
      counters.ws_json_published++;
      counters.last_ok_ts = Date.now();

      console.log(`WS→JSON: Published ${JSON.stringify(parsedJSON).length}b odds payload`);
    }
  });

  await page.goto(HOME_URL, { waitUntil: 'domcontentloaded', timeout: 60000 });
  console.log(`BetParx WS Bridge loaded: ${HOME_URL}`);

  // Interaction to trigger more WebSocket activity
  setInterval(() => {
    page.evaluate(() => {
      window.scrollBy(0, 200);
      const buttons = document.querySelectorAll('button, .clickable, [onclick], a');
      if (buttons.length > 0) {
        const randomButton = buttons[Math.floor(Math.random() * Math.min(5, buttons.length))];
        if (randomButton && randomButton.offsetParent) {
          randomButton.click();
        }
      }
    }).catch(() => {});
  }, 8000);

})().catch(e => {
  console.error('pusher_ws_bridge fatal', e);
  counters.last_error = String(e);
  process.exit(1);
});

// Healthz endpoint
app.get('/healthz', (_req, res) => {
  res.json({
    status: 'active',
    brand: BRAND,
    messages_received: counters.messages_received,
    ws_json_published: counters.ws_json_published,
    last_ok_ts: counters.last_ok_ts,
    last_error: counters.last_error,
    ws_urls: Array.from(counters.ws_urls).slice(0, 5)
  });
});

app.listen(PORT, () => console.log(`BetParx WS Bridge healthz on :${PORT}`));
