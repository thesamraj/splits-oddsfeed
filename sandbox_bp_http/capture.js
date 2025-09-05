const { chromium } = require('playwright');
const fs = require('fs'), path = require('path');

const URLS = [
  process.env.BP_URL_LIVE || 'https://pa.betparx.com/?page=sportsbook#live',
  process.env.BP_URL_PRE  || 'https://pa.betparx.com/?page=sportsbook'
];

const DURATION_SEC = parseInt(process.env.DURATION_SEC || '120', 10);
const OUTDIR = process.env.OUTDIR || `/tmp/BP_HTTP_DOM_${Date.now()}`;
fs.mkdirSync(OUTDIR, { recursive:true });
const log = fs.createWriteStream(path.join(OUTDIR,'run.log'));
const L = s => { const line = `[${new Date().toISOString()}] ${s}\n`; log.write(line); console.log(line.trim()); };

(async () => {
  let jsonCount = 0, xhrCount = 0, domSnaps = 0;
  const seen = new Set();

  const launch = async (headed=false) => {
    const browser = await chromium.launch({ headless: !headed, args:['--no-sandbox','--disable-setuid-sandbox','--disable-blink-features=AutomationControlled'] });
    const context = await browser.newContext({
      userAgent: 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
      locale: 'en-US', timezoneId: 'America/New_York',
      permissions: ['geolocation'], geolocation: { latitude:39.9526, longitude:-75.1652 }
    });
    await context.addInitScript(() => {
      Object.defineProperty(navigator,'webdriver',{ get:()=>undefined });
      Object.defineProperty(navigator,'languages',{ get:()=>['en-US','en'] });
      Object.defineProperty(navigator,'platform',{ get:()=> 'MacIntel' });
      window.chrome = window.chrome || { runtime:{} };
      // hook fetch for visibility
      const origFetch = window.fetch;
      window.fetch = async (...args) => {
        try { const res = await origFetch(...args); return res; } catch(e){ throw e; }
      };
    });
    const page = await context.newPage();

    page.on('response', async (res) => {
      try {
        const req = res.request();
        const rt = req.resourceType();
        if (rt !== 'xhr' && rt !== 'fetch') return;
        xhrCount++;
        const url = req.url();
        if (seen.has(url)) return;
        const ctype = (res.headers()['content-type']||'').toLowerCase();
        if (!ctype.includes('json') && !ctype.includes('text')) return;

        let body = null;
        try { body = await res.text(); } catch { return; }
        if (!body || body.length < 200) return; // skip tiny noise
        seen.add(url);

        // try parse JSON; otherwise keep as text
        let parsed = null;
        try { parsed = JSON.parse(body); } catch {}
        const base = `xhr_${xhrCount}_${Date.now()}`;
        if (parsed) {
          jsonCount++;
          fs.writeFileSync(path.join(OUTDIR, `${base}.json`), JSON.stringify(parsed,null,2));
          L(`Captured JSON from ${url.slice(0,60)}...`);
        } else {
          fs.writeFileSync(path.join(OUTDIR, `${base}.txt`), body.slice(0, 20000));
        }
        fs.writeFileSync(path.join(OUTDIR, `${base}.meta.txt`), `${url}\n${ctype}\nlen=${body.length}`);
      } catch {}
    });

    for (const u of URLS) {
      try {
        L(`Goto ${u}`);
        await page.goto(u, { waitUntil: 'domcontentloaded', timeout: 60000 });
        // light interaction: open main tabs if present
        for (let i=0; i<Math.floor(DURATION_SEC/6); i++){
          await page.evaluate(() => window.scrollBy(0, 200));
          await page.waitForTimeout(1500);
          // Click likely tabs if visible
          for (const label of ['Live','Today','Upcoming','Football','Soccer','NBA','NFL']) {
            try {
              const els = await page.locator(`text="${label}"`).all();
              if (els.length > 0) {
                await els[0].click({ timeout: 500 });
                await page.waitForTimeout(1000);
              }
            } catch {}
          }
        }
      } catch(e){ L(`Nav error: ${e.message}`); }
    }

    // Fallback DOM scrape: capture visible price-like buttons/text
    try {
      const snap = await page.evaluate(() => {
        const odds = [];
        const nodes = document.querySelectorAll('button,div,span');
        const rx = /^[+-]?\d{3,4}$|^-?\d{1,3}(\.\d+)?$/; // +120 / -110 / 1.83
        nodes.forEach(n=>{
          const t=(n.textContent||'').trim();
          if (t && rx.test(t) && !t.includes(':') && !t.includes('/')) {
            odds.push(t);
          }
        });
        return [...new Set(odds)].slice(0, 100);
      });
      if (snap && snap.length) {
        domSnaps = snap.length;
        fs.writeFileSync(path.join(OUTDIR,'dom_odds_sample.txt'), snap.join('\n'));
        L(`Found ${domSnaps} price-like values in DOM`);
      }
    } catch {}

    await context.close(); await browser.close();
  };

  // pass 1: headless
  await launch(false);
  // pass 2 (iterate): if nothing found, try headed
  if (jsonCount === 0 && domSnaps === 0) {
    L('Retrying with headed browser...');
    await launch(true);
  }

  const summary = { xhrCount, jsonCount, domSnaps, urls: URLS };
  fs.writeFileSync(path.join(OUTDIR,'summary.json'), JSON.stringify(summary,null,2));
  L(`SUMMARY json=${jsonCount} xhr=${xhrCount} domSnaps=${domSnaps}`);
})();
