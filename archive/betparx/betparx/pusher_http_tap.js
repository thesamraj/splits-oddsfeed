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
  published_count: 0,
  last_ok_ts: null,
  last_err: null,
  urls_seen: new Set()
};

function publishEnvelope(envelope) {
  try {
    const msg = JSON.stringify(envelope);
    redis.publish('odds.raw.kambi', msg);
    counters.published_count++;
    counters.last_ok_ts = Date.now();
    console.log(`Published HTTP envelope from ${envelope.url}`);
  } catch (e) {
    console.error('publish error', e);
    counters.last_err = String(e);
  }
}

function isOfferingApiUrl(url) {
  // Broader detection: kambi domains and API-like patterns
  return (url.includes('kambicdn.com') || url.includes('kambi.com')) &&
         (url.includes('offering') || url.includes('api') || url.includes('v2018'));
}

function isValidOfferingJSON(data) {
  if (!data || typeof data !== 'object') return false;

  // Check for typical Kambi offering API response keys
  const offeringKeys = ['events', 'event', 'markets', 'outcomes', 'live', 'betOffers', 'competitions'];
  return offeringKeys.some(key => key in data);
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

  // Anti-automation hardening
  await context.addInitScript(() => {
    Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
    window.chrome = window.chrome || { runtime: {} };
  });

  const page = await context.newPage();
  const cdp = await context.newCDPSession(page);
  await cdp.send('Network.enable');

  // Track offering-api requests
  const pendingRequests = new Map();

  cdp.on('Network.requestWillBeSent', (event) => {
    const url = event.request?.url;
    if (url && isOfferingApiUrl(url)) {
      pendingRequests.set(event.requestId, {
        url,
        timestamp: Date.now()
      });
      counters.urls_seen.add(url);
      console.log(`Tracking offering request: ${url}`);
    }
  });

  cdp.on('Network.responseReceived', async (event) => {
    const requestId = event.requestId;
    const response = event.response;

    if (!pendingRequests.has(requestId)) return;
    if (!response || response.status !== 200) return;

    const requestInfo = pendingRequests.get(requestId);
    pendingRequests.delete(requestId);

    try {
      // Get response body
      const bodyResponse = await cdp.send('Network.getResponseBody', { requestId });
      if (!bodyResponse || !bodyResponse.body) return;

      let bodyText = bodyResponse.body;

      // Handle base64 encoded responses
      if (bodyResponse.base64Encoded) {
        bodyText = Buffer.from(bodyResponse.body, 'base64').toString('utf8');
      }

      // Validate JSON and content
      let jsonData;
      try {
        jsonData = JSON.parse(bodyText);
      } catch (e) {
        console.log(`Non-JSON response from ${requestInfo.url}`);
        return;
      }

      if (!isValidOfferingJSON(jsonData)) {
        console.log(`JSON does not contain offering data: ${requestInfo.url}`);
        return;
      }

      // Publish as HTTP transport envelope (compatible with existing normalizer)
      const envelope = {
        brand_hint: BRAND,
        transport: 'http',
        source: 'kambi-http-tap',
        page_url: page.url(),
        url: requestInfo.url,
        content_type: 'application/json',
        payload: bodyText,
        ts: new Date().toISOString()
      };

      publishEnvelope(envelope);
      counters.messages_received++;

    } catch (error) {
      console.error(`Error processing response from ${requestInfo.url}:`, error);
      counters.last_err = String(error);
    }
  });

  // Navigate to BetParx
  console.log(`Loading BetParx: ${HOME_URL}`);
  await page.goto(HOME_URL, { waitUntil: 'domcontentloaded', timeout: 60000 });

  // Keep page active with periodic interaction
  setInterval(() => {
    page.evaluate(() => {
      // Scroll and click to trigger potential API calls
      window.scrollBy(0, 200);
      const buttons = document.querySelectorAll('button, .clickable, [data-testid], a[href*="live"]');
      if (buttons.length > 0) {
        const randomButton = buttons[Math.floor(Math.random() * Math.min(3, buttons.length))];
        if (randomButton && randomButton.offsetParent) {
          randomButton.click();
        }
      }
    }).catch(() => {});
  }, 10000);

  console.log('BetParx HTTP tap active, monitoring offering-api requests...');

})().catch(e => {
  console.error('pusher_http_tap fatal', e);
  counters.last_err = String(e);
  process.exit(1);
});

// Healthz endpoint
app.get('/healthz', (_req, res) => {
  res.json({
    status: 'active',
    brand: BRAND,
    messages_received: counters.messages_received,
    published_count: counters.published_count,
    last_ok_ts: counters.last_ok_ts,
    last_err: counters.last_err,
    urls_seen: Array.from(counters.urls_seen).slice(0, 10)
  });
});

app.listen(PORT, () => console.log(`BetParx HTTP tap healthz on :${PORT}`));
