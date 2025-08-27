const { chromium } = require('playwright');
const Redis = require('ioredis');
const express = require('express');

const BRAND = process.env.BRAND || 'betparx';
const HOME_URL = process.env.HOME_URL || 'https://pa.betparx.com/?page=sportsbook#live';
const REDIS_URL = process.env.REDIS_URL || 'redis://broker:6379/0';
const FETCH_INTERVAL_MS = parseInt(process.env.FETCH_INTERVAL_MS || '20000', 10); // 20s
const TOKEN_HINT = process.env.TOKEN_HINT || 'bp2uspa'; // fallback token
const PORT = parseInt(process.env.PORT || '9118', 10);

const redis = new Redis(REDIS_URL, { lazyConnect: false });
const app = express();
let counters = { messages_received: 0, last_msg_ts: null, ws_urls: new Set(), last_fetch_status: null, last_fetch_err: null };

function publishEnvelope(envelope) {
  try {
    const msg = JSON.stringify(envelope);
    return redis.publish('odds.raw.kambi', msg);
  } catch (e) {
    console.error('publish error', e);
  }
}

(async () => {
  const browser = await chromium.launch({ headless: true, args: ['--no-sandbox','--disable-setuid-sandbox'] });
  const context = await browser.newContext({
    viewport: { width: 1280, height: 860 },
    userAgent: 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    locale: 'en-US', timezoneId: 'America/New_York'
  });
  // Basic stealth
  await context.addInitScript(() => { Object.defineProperty(navigator,'webdriver',{get:()=>undefined}); window.chrome = window.chrome || { runtime:{} }; });

  const page = await context.newPage();
  const cdp = await context.newCDPSession(page);
  await cdp.send('Network.enable');
  await cdp.send('Target.setAutoAttach', { autoAttach: true, waitForDebuggerOnStart: false, flatten: true });

  // WS capture for observability (non-blocking)
  cdp.on('Network.webSocketCreated', p => { counters.ws_urls.add(p.url); });
  cdp.on('Network.webSocketFrameReceived', p => {
    const payload = p.response?.payloadData || '';
    const sample = payload.slice(0, 512);

    // Try to extract Kambi-like data from WebSocket frames
    let processedData = null;
    try {
      if (payload.length > 10) {
        // Try parsing as JSON
        const parsed = JSON.parse(payload);
        if (parsed && typeof parsed === 'object') {
          processedData = parsed;
        }
      }
    } catch (e) {
      // Not JSON, treat as raw frame
    }

    // Publish in normalizer-compatible format
    publishEnvelope({
      brand_hint: BRAND,
      transport: 'websocket',
      url: `https://offering-api.kambicdn.com/offering/v2018/bp2uspa/websocket`, // Synthetic URL for brand mapping
      payload: processedData || payload, // Use parsed data if available, otherwise raw
      body: payload, // Also include raw body for fallback
      page_url: page.url(),
      ts: new Date().toISOString(),
      // Legacy fields for observability
      frame_len: payload.length,
      frame_sample: sample
    });
    counters.messages_received++; counters.last_msg_ts = Date.now();
  });

  // Navigate
  console.log(`Navigating to ${HOME_URL}`);
  await page.goto(HOME_URL, { waitUntil: 'domcontentloaded', timeout: 60000 });
  console.log('Navigation complete, waiting 10s for page setup');
  await page.waitForTimeout(10000);

  // Helper to pick token: try from observed WS urls (if any) else TOKEN_HINT
  async function pickToken() {
    const log = await cdp.send('Network.getResponseBodyForInterception', {}).catch(()=>null); // noop to avoid unused warning
    // We keep it simple: map BRAND→token; WS token extraction is site-specific; fallback to TOKEN_HINT
    const brandToToken = { betparx: 'bp2uspa', unibet: 'ub2uspa', betrivers:'rsi2uspa', sugarhouse:'sg2uspa' };
    return brandToToken[BRAND] || TOKEN_HINT;
  }

  // Periodic browser-side fetch that returns full JSON body
  async function doBrowserFetch() {
    try {
      const token = await pickToken();
      console.log(`Attempting browser fetch with token=${token}`);
      const res = await page.evaluate(async (tok) => {
        try {
          const url = `https://offering-api.kambicdn.com/offering/v2018/${tok}/event/live/open.json`;
          console.log(`Browser fetch: ${url}`);
          const r = await fetch(url, { credentials: 'include' });
          const text = await r.text();
          console.log(`Browser fetch result: status=${r.status}, body length=${text.length}`);
          return { status: r.status, url, text };
        } catch(e) {
          console.log(`Browser fetch error: ${e}`);
          return { status: 'ERR', url: null, text: String(e) };
        }
      }, token);
      counters.last_fetch_status = res.status;
      console.log(`Fetch result: status=${res.status}, text length=${res.text?.length || 0}`);
      if (res.status === 200 && res.text) {
        publishEnvelope({
          brand_hint: BRAND,
          transport: 'browser-fetch',
          url: res.url,                       // normalizer brand mapping uses this
          body: res.text,                     // full JSON text body (HTTP-equivalent)
          page_url: page.url(),
          ts: new Date().toISOString()
        });
        counters.messages_received++; counters.last_msg_ts = Date.now();
        console.log(`Published successful fetch result`);
      } else if (res.status !== 200) {
        counters.last_fetch_err = `status=${res.status}`;
      }
    } catch (e) {
      counters.last_fetch_err = String(e);
      console.log(`doBrowserFetch error: ${e}`);
    }
  }

  setInterval(() => { page.evaluate(() => window.scrollBy(0, 180)).catch(()=>{}); }, 8000);
  setInterval(doBrowserFetch, FETCH_INTERVAL_MS);
})().catch(e => { console.error('pusher fatal', e); process.exit(1); });

// Healthz
app.get('/healthz', (_req, res) => {
  res.json({
    status: 'active', brand: BRAND,
    messages_received: counters.messages_received,
    last_msg_ts: counters.last_msg_ts,
    ws_urls: Array.from(counters.ws_urls).slice(0,5),
    last_fetch_status: counters.last_fetch_status,
    last_fetch_err: counters.last_fetch_err
  });
});
app.listen(PORT, () => console.log(`pusher healthz on :${PORT}`));
