const { chromium } = require('playwright');
const fs = require('fs'), path = require('path');

const HOME_URL = process.env.HOME_URL || 'https://pa.betparx.com/?page=sportsbook#live';
const DURATION_SEC = parseInt(process.env.DURATION_SEC || '120', 10);
const OUTDIR = process.env.OUTDIR || `/tmp/BP_WS_SANDBOX_${Date.now()}`;
fs.mkdirSync(OUTDIR, { recursive: true });
const log = fs.createWriteStream(path.join(OUTDIR,'probe.log'));
const L = (s)=>{ const line = `[${new Date().toISOString()}] ${s}\n`; log.write(line); console.log(line.trim()); };

(async () => {
  L(`Start WS sandbox probe → ${HOME_URL}`);
  const browser = await chromium.launch({ headless: true, args: ['--no-sandbox','--disable-setuid-sandbox','--disable-blink-features=AutomationControlled'] });
  const context = await browser.newContext({
    userAgent: 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    locale: 'en-US', timezoneId: 'America/New_York',
    permissions: ['geolocation'], geolocation: { latitude:39.9526, longitude:-75.1652 }
  });

  // stealth-ish
  await context.addInitScript(() => {
    Object.defineProperty(navigator,'webdriver',{ get:()=>undefined });
    Object.defineProperty(navigator,'languages',{ get:()=>['en-US','en'] });
    Object.defineProperty(navigator,'platform',{ get:()=> 'MacIntel' });
    window.chrome = window.chrome || { runtime:{} };
  });

  const page = await context.newPage();
  const client = await context.newCDPSession(page);
  await client.send('Network.enable');
  await client.send('Target.setAutoAttach', { autoAttach: true, waitForDebuggerOnStart: false, flatten: true });

  const frames = [];
  const wsUrls = new Set();
  let hsReq=0, hsResp=0, wsCount=0;

  const dumpJson = (obj, file) => fs.writeFileSync(path.join(OUTDIR,file), JSON.stringify(obj,null,2));

  // capture WS + try to parse payloads
  const tryExtract = (payload) => {
    // strategies: raw JSON, NDJSON line, JSON inside text, base64 JSON
    const out = { kind:null, json:null };
    if (!payload || typeof payload !== 'string') return out;

    const direct = payload.trim();
    // 1) direct JSON
    if ((direct.startsWith('{') && direct.endsWith('}')) || (direct.startsWith('[') && direct.endsWith(']'))) {
      try { out.json = JSON.parse(direct); out.kind = 'json'; return out; } catch {}
    }
    // 2) first JSON object inside text
    const m = direct.match(/(\{.*\}|\[.*\])/s);
    if (m) {
      try { out.json = JSON.parse(m[1]); out.kind = 'embedded-json'; return out; } catch {}
    }
    // 3) base64 segment that decodes to JSON
    const b64 = direct.match(/[A-Za-z0-9+/=]{40,}/);
    if (b64) {
      try {
        const buf = Buffer.from(b64[0],'base64').toString('utf8');
        if ((buf.startsWith('{') && buf.endsWith('}')) || (buf.startsWith('[') && buf.endsWith(']'))) {
          out.json = JSON.parse(buf); out.kind = 'b64-json'; return out;
        }
      } catch {}
    }
    return out;
  };

  client.on('Network.webSocketCreated', p => { wsUrls.add(p.url); L(`WS created: ${p.url}`); });
  client.on('Network.webSocketWillSendHandshakeRequest', _ => { hsReq++; });
  client.on('Network.webSocketHandshakeResponseReceived', p => { hsResp++; L(`WS handshake ${p.response?.status||'n/a'}`); });
  client.on('Network.webSocketFrameReceived', p => {
    wsCount++;
    const data = p.response?.payloadData || '';
    const extr = tryExtract(data);
    const rec = { ts: Date.now()/1000, size: data.length, kind: extr.kind, hasJson: !!extr.json };
    if (extr.json) {
      // store a capped sample file
      const fname = `frame_${wsCount}_${extr.kind}.json`;
      dumpJson(extr.json, fname);
      rec.sampleFile = fname;
    } else if (wsCount <= 5) {
      fs.writeFileSync(path.join(OUTDIR,`frame_${wsCount}.txt`), data.slice(0,2000));
    }
    frames.push(rec);
    if (wsCount % 25 === 0) L(`Frames=${wsCount} (json samples=${frames.filter(f=>f.hasJson).length})`);
  });

  try {
    await page.goto(HOME_URL, { waitUntil:'domcontentloaded', timeout: 60000 });
    L('DOM loaded; begin gentle scroll');
    for (let i=0;i<Math.floor(DURATION_SEC/5);i++){
      await page.evaluate(()=>window.scrollBy(0,180));
      await page.waitForTimeout(5000);
    }
  } catch(e){ L(`Nav error: ${e.message}`); }

  // summary
  const summary = {
    home: HOME_URL, ws_urls: [...wsUrls], handshakes: { req: hsReq, resp: hsResp },
    frames_total: wsCount, frames_with_json: frames.filter(f=>f.hasJson).length
  };
  dumpJson(summary,'summary.json');
  dumpJson(frames,'frames_meta.json');
  L(`Done. Frames=${wsCount}, JSON_frames=${summary.frames_with_json}`);
  await browser.close();
})();
