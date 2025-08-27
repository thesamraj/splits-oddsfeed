const { chromium } = require('playwright');
const fs = require('fs');
const path = require('path');

const BRAND = process.env.BRAND || 'betparx';
const URL = process.env.HOME_URL || (BRAND === 'unibet'
  ? 'https://pa.unibet.com/?page=sportsbook#live'
  : 'https://pa.betparx.com/?page=sportsbook#live');

const DURATION_SEC = parseInt(process.env.DURATION_SEC || '120', 10);
const HEADED = process.env.HEADED === 'true';
const TZ = process.env.TZ || 'America/New_York';
const LOCALE = 'en-US';

// Output dir
const TS = new Date().toISOString().replace(/[:.]/g,'').replace('Z','Z');
const OUTDIR = process.env.OUTPUT_DIR || `/root/MEGA2_STEALTH_${BRAND}_${TS}`;
fs.mkdirSync(OUTDIR, { recursive: true });
const log = fs.createWriteStream(path.join(OUTDIR, 'cdp.log'));
function L(s){ const line = `[${new Date().toISOString()}] ${s}\n`; log.write(line); console.log(line.trim()); }

(async () => {
  L(`Start STEALTH CDP for ${BRAND} → ${URL}`);
  const profileDir = path.join('/Users/sam/Desktop/splits-oddsfeed/diag/profiles', BRAND);
  fs.mkdirSync(profileDir, { recursive: true });

  // Launch persistent context (keeps cookies/cache)
  const context = await chromium.launchPersistentContext(profileDir, {
    headless: !HEADED,
    args: [
      '--no-sandbox','--disable-setuid-sandbox',
      '--disable-blink-features=AutomationControlled',
      '--window-size=1280,860'
    ],
    viewport: { width: 1280, height: 860 },
    userAgent: 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    locale: LOCALE,
    timezoneId: TZ,
    permissions: ['geolocation','notifications'],
    geolocation: { latitude: 39.9526, longitude: -75.1652 }, // Philly-ish
    colorScheme: 'light',
    javaScriptEnabled: true
  });

  // Stealth hardening (best-effort)
  await context.addInitScript(() => {
    Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
    Object.defineProperty(Notification, 'permission', { get: () => 'default' });
    Object.defineProperty(navigator, 'languages', { get: () => ['en-US','en'] });
    Object.defineProperty(navigator, 'platform', { get: () => 'MacIntel' });
    const getPlugins = () => [{name:'Chrome PDF Plugin'},{name:'Chrome PDF Viewer'},{name:'Native Client'}];
    Object.defineProperty(navigator, 'plugins', { get: () => getPlugins() });
    // Chrome runtime shim
    window.chrome = window.chrome || { runtime: {} };
  });

  const page = await context.newPage();

  // CDP session + child-target attach
  const client = await context.newCDPSession(page);
  await client.send('Network.enable');
  await client.send('Target.setAutoAttach', { autoAttach: true, waitForDebuggerOnStart: false, flatten: true });
  client.on('Target.attachedToTarget', async (ev) => {
    try {
      L(`Attached to child target: ${ev?.targetInfo?.type || 'unknown'} - ${ev?.targetInfo?.url || ''}`);
      await client.send('Target.sendMessageToTarget', {
        sessionId: ev.sessionId,
        message: JSON.stringify({ id: Date.now(), method: 'Network.enable' })
      });
    } catch {}
  });

  // WS/HTTP tracking
  const wsUrls = new Set();
  const httpOffering = new Set();
  const tokens = new Set();
  let wsFrames = 0, hsReq = 0, hsResp = 0;

  const tokenFrom = (u) => {
    const m = u && u.match(/\/offering\/v2018\/([^\/]+)\//);
    return m ? m[1] : null;
  };

  client.on('Network.webSocketCreated', p => {
    wsUrls.add(p.url);
    const t = tokenFrom(p.url); if (t) { tokens.add(t); L(`Token (WS): ${t}`); }
    L(`WS created: ${p.url}`);
  });
  client.on('Network.webSocketWillSendHandshakeRequest', _ => { hsReq++; L('WS handshake → request'); });
  client.on('Network.webSocketHandshakeResponseReceived', p => { hsResp++; L(`WS handshake ← resp status=${p.response?.status || 'n/a'}`); });
  client.on('Network.webSocketFrameReceived', p => {
    wsFrames++;
    const size = p.response?.payloadData?.length || 0;
    L(`WS Frame #${wsFrames}: ${size} bytes`);
  });

  client.on('Network.requestWillBeSent', p => {
    const u = p.request.url || '';
    if (u.includes('offering') && u.includes('kambi')) {
      httpOffering.add(u);
      const t = tokenFrom(u); if (t) { tokens.add(t); L(`Token (HTTP): ${t}`); }
      L(`HTTP offering → ${u}`);
    }
  });
  client.on('Network.responseReceived', p => {
    const u = p.response.url || '';
    if (u.includes('offering') && u.includes('kambi')) {
      L(`HTTP offering ← ${p.response.status} for ${u}`);
    }
  });

  // Navigate + gentle scroll
  try {
    L(`Goto ${URL}`);
    await page.goto(URL, { waitUntil: 'domcontentloaded', timeout: 60000 });
    L('Page DOM loaded');
    for (let i=0;i<Math.floor(DURATION_SEC/10);i++){
      await page.evaluate(() => window.scrollBy(0, 180));
      await page.waitForTimeout(10000);
      L(`Tick ${(i+1)*10}/${DURATION_SEC}s; WS:${wsUrls.size} Frames:${wsFrames} HSreq:${hsReq} HSresp:${hsResp}`);
    }
  } catch(e){ L(`Navigation error: ${e.message}`); }

  // Try in-page fetch (bypasses CORS if site allows)
  let inPageHTTP = { tried:false, status:null, token:null, snippet:null };
  try {
    const picked = [...tokens][0];
    if (picked) {
      inPageHTTP.tried = true;
      inPageHTTP.token = picked;
      L(`In-page fetch try for token=${picked}`);
      const res = await page.evaluate(async (tok) => {
        try {
          const url = `https://offering-api.kambicdn.com/offering/v2018/${tok}/event/live/open.json`;
          const r = await fetch(url, { credentials: 'include' });
          const text = await r.text();
          return { status: r.status, body: text.slice(0, 300) };
        } catch(e){ return { status: 'ERR', body: String(e) }; }
      }, picked);
      inPageHTTP.status = res.status;
      inPageHTTP.snippet = res.body;
      L(`In-page fetch status=${res.status} body[0..300]=${(res.body||'').slice(0,100)}...`);
    }
  } catch(e){ L(`In-page fetch error: ${e.message}`); }

  // Save artifacts
  fs.writeFileSync(path.join(OUTDIR,'ws_urls.txt'), [...wsUrls].join('\n'));
  fs.writeFileSync(path.join(OUTDIR,'http_offering.txt'), [...httpOffering].join('\n'));
  fs.writeFileSync(path.join(OUTDIR,'tokens.txt'), [...tokens].join('\n'));
  fs.writeFileSync(path.join(OUTDIR,'summary.json'), JSON.stringify({
    brand: BRAND, ws_urls: wsUrls.size, ws_frames: wsFrames,
    hs_req: hsReq, hs_resp: hsResp, tokens: [...tokens],
    inpage_http: inPageHTTP
  }, null, 2));

  await context.close();
  log.end();
  console.log(JSON.stringify({ outdir: OUTDIR })); // stdout pointer
})();
