const { chromium } = require('playwright');
const Redis = require('ioredis');
const express = require('express');
const pako = require('pako');

const BRAND = process.env.BRAND || 'betparx';
const HOME_URL = process.env.HOME_URL || 'https://pa.betparx.com/?page=sportsbook#live';
const REDIS_URL = process.env.REDIS_URL || 'redis://broker:6379/0';
const PORT = parseInt(process.env.PORT || '9127', 10);
const PROFILE_DIR = `/tmp/playwright-${BRAND}`;

const redis = new Redis(REDIS_URL, { lazyConnect: false });
const app = express();

let counters = {
  frames_seen: 0,
  candidate_json: 0,
  published: 0,
  bytes_seen: 0,
  last_json_ts: null,
  ws_connections: 0
};

let frameBuffer = new Map(); // Per-WS URL rolling buffer for multi-frame reassembly

function publishEnvelope(envelope) {
  try {
    const msg = JSON.stringify(envelope);
    redis.publish('odds.raw.kambi', msg);
    counters.published++;
    counters.last_json_ts = Date.now();

    const keys = envelope.data ? Object.keys(envelope.data).slice(0, 5).join(',') : 'none';
    console.log(`BRIDGE_PUBLISH brand=${BRAND} size=${msg.length}b keys=${keys}`);

  } catch (e) {
    console.error('Redis publish error:', e);
  }
}

function extractJSON(payload) {
  if (!payload) return null;

  // Method 1: Plain JSON
  try {
    if (payload.startsWith('{') || payload.startsWith('[')) {
      return JSON.parse(payload);
    }
  } catch (e) { /* continue */ }

  // Method 2: Socket-like envelopes (strip common prefixes)
  const prefixes = ['42', '4', '0', '2[', '2{'];
  for (const prefix of prefixes) {
    if (payload.startsWith(prefix)) {
      try {
        const cleaned = payload.substring(prefix.length);
        if (cleaned.startsWith('{') || cleaned.startsWith('[')) {
          return JSON.parse(cleaned);
        }
      } catch (e) { /* continue */ }
    }
  }

  // Method 3: Nested JSON (find first balanced JSON)
  const openBrace = payload.indexOf('{');
  const openBracket = payload.indexOf('[');
  const start = (openBrace === -1) ? openBracket :
                (openBracket === -1) ? openBrace :
                Math.min(openBrace, openBracket);

  if (start !== -1) {
    try {
      let depth = 0;
      let inString = false;
      let escaped = false;
      const startChar = payload[start];
      const endChar = startChar === '{' ? '}' : ']';

      for (let i = start; i < payload.length; i++) {
        const char = payload[i];

        if (escaped) {
          escaped = false;
          continue;
        }

        if (char === '\\') {
          escaped = true;
          continue;
        }

        if (char === '"') {
          inString = !inString;
          continue;
        }

        if (!inString) {
          if (char === startChar) depth++;
          else if (char === endChar) depth--;

          if (depth === 0 && i > start) {
            const jsonStr = payload.substring(start, i + 1);
            return JSON.parse(jsonStr);
          }
        }
      }
    } catch (e) { /* continue */ }
  }

  // Method 4: Base64/deflate
  try {
    // Check if looks like base64
    if (/^[A-Za-z0-9+/=]+$/.test(payload) && payload.length > 20) {
      const decoded = Buffer.from(payload, 'base64');

      // Try direct decode
      const decodedStr = decoded.toString('utf8');
      if (decodedStr.startsWith('{') || decodedStr.startsWith('[')) {
        return JSON.parse(decodedStr);
      }

      // Try deflate
      const inflated = pako.inflate(decoded, { to: 'string' });
      if (inflated.startsWith('{') || inflated.startsWith('[')) {
        return JSON.parse(inflated);
      }
    }
  } catch (e) { /* continue */ }

  return null;
}

function isOddsLike(obj) {
  if (!obj || typeof obj !== 'object') return false;

  const oddsKeys = ['event', 'events', 'outcomes', 'markets', 'home', 'away', 'league', 'start'];
  const objStr = JSON.stringify(obj).toLowerCase();

  return oddsKeys.some(key => objStr.includes(key));
}

function processFrame(wsUrl, payload) {
  counters.frames_seen++;
  counters.bytes_seen += payload.length;

  // Try direct extraction
  let jsonData = extractJSON(payload);

  if (!jsonData) {
    // Multi-frame reassembly
    if (!frameBuffer.has(wsUrl)) {
      frameBuffer.set(wsUrl, { buffer: '', lastUpdate: Date.now() });
    }

    const bufferData = frameBuffer.get(wsUrl);
    bufferData.buffer += payload;
    bufferData.lastUpdate = Date.now();

    // Try parsing accumulated buffer
    if (bufferData.buffer.length > 100) {
      jsonData = extractJSON(bufferData.buffer);
      if (jsonData || bufferData.buffer.length > 10000) {
        // Reset buffer on success or overflow
        bufferData.buffer = '';
      }
    }
  }

  if (jsonData && isOddsLike(jsonData)) {
    counters.candidate_json++;

    const envelope = {
      brand_hint: BRAND,
      transport: 'ws',
      page_url: global.currentPageUrl || HOME_URL,
      ws_url: wsUrl,
      data: jsonData,
      ts: new Date().toISOString()
    };

    publishEnvelope(envelope);
  }
}

(async () => {
  console.log(`Starting WS JSON Bridge for ${BRAND}...`);

  const browser = await chromium.launch({
    headless: true,
    args: ['--no-sandbox', '--disable-setuid-sandbox']
  });

  const context = await browser.newContext({
    viewport: { width: 1280, height: 720 },
    userAgent: 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
  });

  // Anti-automation hardening
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

  // WebSocket connection tracking
  cdp.on('Network.webSocketCreated', (event) => {
    counters.ws_connections++;
    console.log(`WebSocket created: ${event.url}`);
  });

  cdp.on('Network.webSocketClosed', (event) => {
    console.log(`WebSocket closed: ${event.timestamp}`);
  });

  // Frame processing
  cdp.on('Network.webSocketFrameReceived', (event) => {
    try {
      const payload = event.response?.payloadData || '';
      if (payload) {
        processFrame(event.requestId, payload);
      }
    } catch (error) {
      console.error('Frame processing error:', error);
    }
  });

  // Navigate and maintain session
  console.log(`Loading ${HOME_URL}...`);
  await page.goto(HOME_URL, { waitUntil: 'domcontentloaded', timeout: 60000 });
  global.currentPageUrl = page.url();

  // Keep page active
  setInterval(async () => {
    try {
      await page.evaluate(() => {
        window.scrollBy(0, 100);
        // Click random interactive elements
        const clickables = document.querySelectorAll('button, a, [role="button"]');
        if (clickables.length > 0) {
          const random = clickables[Math.floor(Math.random() * Math.min(3, clickables.length))];
          if (random && random.offsetParent) {
            random.click();
          }
        }
      });
    } catch (e) {
      // Ignore interaction errors
    }
  }, 15000);

  // Clean frame buffers periodically
  setInterval(() => {
    const now = Date.now();
    for (const [url, data] of frameBuffer.entries()) {
      if (now - data.lastUpdate > 30000) { // 30s timeout
        frameBuffer.delete(url);
      }
    }
  }, 60000);

  console.log('WS JSON Bridge active, monitoring WebSocket frames...');

})().catch(e => {
  console.error('Bridge fatal error:', e);
  process.exit(1);
});

// Healthz endpoint
app.get('/healthz', (_req, res) => {
  res.json({
    status: counters.frames_seen > 0 ? 'active' : 'initializing',
    frames_seen: counters.frames_seen,
    candidate_json: counters.candidate_json,
    published: counters.published,
    bytes_seen: counters.bytes_seen,
    last_json_ts: counters.last_json_ts,
    ws_connections: counters.ws_connections
  });
});

app.listen(PORT, () => console.log(`WS Bridge healthz on :${PORT}`));
