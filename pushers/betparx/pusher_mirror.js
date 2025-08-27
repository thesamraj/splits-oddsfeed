const { chromium } = require('playwright');
const Redis = require('ioredis');
const express = require('express');

const BRAND = process.env.BRAND || 'betparx';
const HOME_URL = process.env.HOME_URL || 'https://pa.betparx.com/?page=sportsbook#live';
const REDIS_URL = process.env.REDIS_URL || 'redis://broker:6379/0';
const PORT = parseInt(process.env.PORT || '9118', 10);

const redis = new Redis(REDIS_URL, { lazyConnect: false });
const app = express();

let counters = {
  messages_received: 0,
  last_msg_ts: null,
  ws_urls: new Set(),
  mirrored_http: 0,
  last_mirror_url: null,
  last_mirror_status: null,
  last_mirror_err: null
};

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
  await context.addInitScript(() => { Object.defineProperty(navigator,'webdriver',{get:()=>undefined}); window.chrome = window.chrome || { runtime:{} }; });

  const page = await context.newPage();
  const cdp = await context.newCDPSession(page);
  await cdp.send('Network.enable');
  await cdp.send('Target.setAutoAttach', { autoAttach: true, waitForDebuggerOnStart: false, flatten: true });

  // WS capture for observability
  cdp.on('Network.webSocketCreated', p => { if (p && p.url) counters.ws_urls.add(p.url); });
  cdp.on('Network.webSocketFrameReceived', p => {
    const payload = p?.response?.payloadData || '';
    if (!payload) return;
    publishEnvelope({
      brand_hint: BRAND, transport: 'ws',
      page_url: page.url(), frame_len: payload.length,
      frame_sample: payload.slice(0, 512), ts: new Date().toISOString()
    });
    counters.messages_received++; counters.last_msg_ts = Date.now();
  });

  // HTTP MIRROR: capture offering-api JSON and publish full body
  cdp.on('Network.responseReceived', async (ev) => {
    try {
      const url = ev?.response?.url || '';
      const status = ev?.response?.status || 0;
      const mime = (ev?.response?.mimeType || '').toLowerCase();

      // Log all kambi-related requests for debugging
      if (url.includes('kambi') && status === 200) {
        console.log(`HTTP detected: ${url} (${status}) ${mime}`);
      }

      // Capture from any kambi domain with JSON content
      if (!url.includes('kambi') || status !== 200 || !mime.includes('json')) return;

      const bodyResp = await cdp.send('Network.getResponseBody', { requestId: ev.requestId }).catch(() => null);
      if (!bodyResp || !bodyResp.body) return;

      // Publish in a format the normalizer HTTP path can consume:
      // include both 'body' and 'payload' to be safe; include 'url'
      const envelope = {
        brand_hint: BRAND,
        transport: 'mirror-http',
        source: 'offering-api',
        url,                    // critical for brand extraction & routing
        body: bodyResp.body,    // full JSON string
        payload: bodyResp.body, // duplicate for compatibility
        content_type: mime,
        ts: new Date().toISOString()
      };
      await publishEnvelope(envelope);
      counters.mirrored_http++;
      counters.messages_received++;
      counters.last_msg_ts = Date.now();
      counters.last_mirror_url = url;
      counters.last_mirror_status = status;
    } catch (e) {
      counters.last_mirror_err = String(e);
    }
  });

  await page.goto(HOME_URL, { waitUntil: 'domcontentloaded', timeout: 60000 });

  // More aggressive interaction to trigger API calls
  setInterval(() => {
    page.evaluate(() => {
      window.scrollBy(0, 200);
      // Try clicking on sports/live elements if they exist
      const buttons = document.querySelectorAll('button, .clickable, [onclick], a');
      if (buttons.length > 0) {
        const randomButton = buttons[Math.floor(Math.random() * Math.min(5, buttons.length))];
        if (randomButton && randomButton.offsetParent) { // Only click visible elements
          randomButton.click();
        }
      }
    }).catch(()=>{});
  }, 8000);
})().catch(e => { console.error('pusher_mirror fatal', e); process.exit(1); });

// Healthz
app.get('/healthz', (_req, res) => {
  res.json({
    status: 'active', brand: BRAND,
    messages_received: counters.messages_received,
    mirrored_http: counters.mirrored_http,
    last_msg_ts: counters.last_msg_ts,
    last_mirror_url: counters.last_mirror_url,
    last_mirror_status: counters.last_mirror_status,
    last_mirror_err: counters.last_mirror_err,
    ws_urls: Array.from(counters.ws_urls).slice(0,5)
  });
});
app.listen(PORT, () => console.log(`betparx mirror healthz on :${PORT}`));
