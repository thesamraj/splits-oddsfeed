const { chromium } = require('playwright');
const fs = require('fs');
const path = require('path');
const redis = require('redis');

const BRAND = 'betparx';
const URL = 'https://pa.betparx.com/?page=sportsbook#live';
const REDIS_URL = process.env.REDIS_URL || 'redis://localhost:6379/0';
const CHANNEL = 'odds.raw.kambi';
const DURATION_SEC = parseInt(process.env.DURATION_SEC || '900', 10); // 15 minutes
const TZ = 'America/New_York';
const LOCALE = 'en-US';

// Counter for healthz
let messagesSent = 0;
let lastMessageTs = null;
let wsConnections = 0;
let wsFrames = 0;

// Redis client
const redisClient = redis.createClient({ url: REDIS_URL });

// HTTP healthz server
const http = require('http');
const healthzServer = http.createServer((req, res) => {
  if (req.url === '/healthz') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      status: 'active',
      brand: BRAND,
      messages_sent: messagesSent,
      last_message_ts: lastMessageTs,
      ws_connections: wsConnections,
      ws_frames: wsFrames,
      uptime_sec: Math.floor(process.uptime())
    }));
  } else {
    res.writeHead(404);
    res.end();
  }
});

function L(s) {
  const line = `[${new Date().toISOString()}] ${s}`;
  console.log(line);
}

async function publishMessage(type, data) {
  try {
    const message = {
      brand_hint: BRAND,
      source_type: type,
      payload: data,
      ts: Date.now() / 1000,
      collector: 'push-first-ephemeral'
    };
    await redisClient.publish(CHANNEL, JSON.stringify(message));
    messagesSent++;
    lastMessageTs = Date.now() / 1000;
    L(`Published ${type} message #${messagesSent}`);
  } catch (error) {
    L(`Redis publish error: ${error.message}`);
  }
}

(async () => {
  L(`Start EPHEMERAL PUSH-FIRST collector for ${BRAND}`);

  // Connect to Redis
  await redisClient.connect();
  L('Connected to Redis');

  // Start healthz server
  healthzServer.listen(9130, '0.0.0.0', () => {
    L('Healthz server listening on port 9130');
  });

  const profileDir = path.join('/Users/sam/Desktop/splits-oddsfeed/diag/profiles', BRAND);
  fs.mkdirSync(profileDir, { recursive: true });

  // Launch persistent context
  const context = await chromium.launchPersistentContext(profileDir, {
    headless: true,
    args: [
      '--no-sandbox', '--disable-setuid-sandbox',
      '--disable-blink-features=AutomationControlled',
      '--window-size=1280,860'
    ],
    viewport: { width: 1280, height: 860 },
    userAgent: 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    locale: LOCALE,
    timezoneId: TZ,
    permissions: ['geolocation', 'notifications'],
    geolocation: { latitude: 39.9526, longitude: -75.1652 },
    colorScheme: 'light',
    javaScriptEnabled: true
  });

  // Stealth hardening
  await context.addInitScript(() => {
    Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
    Object.defineProperty(Notification, 'permission', { get: () => 'default' });
    Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
    Object.defineProperty(navigator, 'platform', { get: () => 'MacIntel' });
    const getPlugins = () => [{ name: 'Chrome PDF Plugin' }, { name: 'Chrome PDF Viewer' }, { name: 'Native Client' }];
    Object.defineProperty(navigator, 'plugins', { get: () => getPlugins() });
    window.chrome = window.chrome || { runtime: {} };
  });

  const page = await context.newPage();

  // CDP session for WebSocket monitoring
  const client = await context.newCDPSession(page);
  await client.send('Network.enable');
  await client.send('Target.setAutoAttach', { autoAttach: true, waitForDebuggerOnStart: false, flatten: true });

  // Child target attachment
  client.on('Target.attachedToTarget', async (ev) => {
    try {
      L(`Attached to child target: ${ev?.targetInfo?.type || 'unknown'}`);
      await client.send('Target.sendMessageToTarget', {
        sessionId: ev.sessionId,
        message: JSON.stringify({ id: Date.now(), method: 'Network.enable' })
      });
    } catch (error) {
      L(`Child target error: ${error.message}`);
    }
  });

  // WebSocket event handlers
  client.on('Network.webSocketCreated', async (params) => {
    wsConnections++;
    L(`WS created #${wsConnections}: ${params.url}`);
    await publishMessage('websocket_created', {
      url: params.url,
      requestId: params.requestId
    });
  });

  client.on('Network.webSocketFrameReceived', async (params) => {
    wsFrames++;
    const size = params.response?.payloadData?.length || 0;
    L(`WS Frame #${wsFrames}: ${size} bytes`);

    // Publish frame data
    await publishMessage('websocket_frame', {
      requestId: params.requestId,
      size: size,
      data: params.response?.payloadData?.slice(0, 1000) // First 1000 chars
    });
  });

  // HTTP request monitoring
  client.on('Network.requestWillBeSent', async (params) => {
    const url = params.request?.url || '';
    if (url.includes('offering') || url.includes('kambi')) {
      L(`HTTP request: ${url}`);
      await publishMessage('http_request', {
        url: url,
        method: params.request?.method,
        headers: params.request?.headers
      });
    }
  });

  // Navigate to the page
  try {
    L(`Navigating to ${URL}`);
    await page.goto(URL, { waitUntil: 'domcontentloaded', timeout: 60000 });
    L('Page loaded successfully');

    // Wait for initial WebSocket connections
    await page.waitForTimeout(10000);

    // Gentle scrolling to trigger more activity
    const scrollInterval = setInterval(async () => {
      try {
        await page.evaluate(() => window.scrollBy(0, 200));
      } catch (error) {
        L(`Scroll error: ${error.message}`);
      }
    }, 15000);

    // Run for specified duration
    await page.waitForTimeout(DURATION_SEC * 1000);

    clearInterval(scrollInterval);

  } catch (error) {
    L(`Navigation error: ${error.message}`);
  }

  L(`Collection completed. Messages sent: ${messagesSent}, WS frames: ${wsFrames}`);

  await context.close();
  await redisClient.quit();
  healthzServer.close();

  process.exit(0);
})();
