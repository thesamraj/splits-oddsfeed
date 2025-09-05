/**
 * UB DOM collector: no login, no token usage.
 * Strategy:
 *  1) Open UB sportsbook page.
 *  2) Capture any public offering *.json responses (if any).
 *  3) Also scrape visible odds buttons/text (regex on +100/-120/1.90, etc.).
 *  4) Publish minimal envelopes to Redis for normalizer shim (odds.raw.unibet_dom).
 */
const { chromium } = require('playwright');
const redis = require('redis');

const START_URL = process.env.UB_URL || 'https://pa.unibet.com/?page=sportsbook#live';
const REDIS_URL = process.env.REDIS_URL || 'redis://broker:6379/0';
const DURATION_SEC = parseInt(process.env.DURATION_SEC || '300', 10);
const PUBLISH_CH = 'odds.raw.unibet_dom';

function oddsLike(s){
  // very simple matcher: American odds (+110,-125) or decimal (1.60–10.00)
  return /\B[+-]\d{2,4}\b/.test(s) || /\b[1-9]\.\d{2}\b/.test(s);
}
function sanitize(s){ return (s||'').replace(/\s+/g,' ').trim(); }

(async ()=>{
  console.log('[UB_DOM] Starting browser-only collector');
  const r = redis.createClient({ url: REDIS_URL });
  await r.connect();
  console.log('[UB_DOM] Redis connected');

  const browser = await chromium.launch({ headless:true, args:['--no-sandbox','--disable-setuid-sandbox']});
  const ctx = await browser.newContext({ locale:'en-US', timezoneId:'America/New_York' });
  const page = await ctx.newPage();

  const cdp = await ctx.newCDPSession(page);
  await cdp.send('Network.enable');
  await cdp.send('Target.setAutoAttach', { autoAttach: true, waitForDebuggerOnStart: false, flatten: true });

  let jsonHits=0, domHits=0;

  cdp.on('Network.responseReceived', async (ev)=>{
    try{
      const url = ev.response?.url || '';
      if (!url.includes('kambi')) return;
      if (!url.endsWith('.json')) return;
      if (ev.response.status !== 200) return;
      const body = await cdp.send('Network.getResponseBody', { requestId: ev.requestId });
      const text = (body?.body || '').slice(0, 500000); // cap
      if (text && text.trim().startsWith('{')) {
        jsonHits++;
        console.log(`[UB_DOM] JSON hit #${jsonHits} from ${url.substring(0,60)}`);
        await r.publish(PUBLISH_CH, JSON.stringify({
          brand_hint:'unibet',
          transport:'dom-json',
          page_url:url,
          frame:text
        }));
      }
    }catch(e){}
  });

  try {
    console.log(`[UB_DOM] Navigating to ${START_URL}`);
    await page.goto(START_URL, { waitUntil:'domcontentloaded', timeout:60000 });
    console.log('[UB_DOM] Page loaded');
  } catch(e){
    console.log(`[UB_DOM] Navigation error: ${e.message}`);
  }

  const t0 = Date.now();

  while ((Date.now()-t0)/1000 < DURATION_SEC){
    // Gentle scroll to trigger content
    try { await page.evaluate(()=>window.scrollBy(0,180)); } catch(e){}
    await page.waitForTimeout(3000);

    // Scrape visible odds-like texts/buttons
    try{
      const texts = await page.evaluate(()=>{
        const out=[];
        // collect button-like and span/div texts
        const sel = Array.from(document.querySelectorAll('button, [role="button"], span, div'));
        for (const el of sel){
          const s = (el.innerText||'').trim();
          if (s) out.push(s);
        }
        return out.slice(0, 2000);
      });
      const candidates = Array.from(new Set(texts.filter(oddsLike))).slice(0, 200);
      if (candidates.length){
        domHits += candidates.length;
        console.log(`[UB_DOM] Found ${candidates.length} odds-like texts`);
        const payload = { brand_hint:'unibet', transport:'dom-text', page_url:START_URL,
          sample:candidates.slice(0,20), count:candidates.length };
        await r.publish(PUBLISH_CH, JSON.stringify(payload));
      }
    }catch(e){}

    // small wait
    await page.waitForTimeout(2000);
  }

  await r.publish(PUBLISH_CH, JSON.stringify({brand_hint:'unibet', transport:'dom-summary', jsonHits, domHits}));
  await browser.close();
  await r.quit();
  console.log(`[UB_DOM] Complete: ${jsonHits} JSON hits, ${domHits} DOM odds`);
  console.log(JSON.stringify({ jsonHits, domHits }));
})();
