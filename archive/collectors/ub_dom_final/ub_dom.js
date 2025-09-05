const { chromium } = require('playwright'); // v1.45.0 built-in
const redis = require('redis');
const START_URL = process.env.UB_URL || 'https://pa.unibet.com/?page=sportsbook#live';
const REDIS_URL = process.env.REDIS_URL || 'redis://broker:6379/0';
const DURATION_SEC = parseInt(process.env.DURATION_SEC || '420', 10); // 7 min
const CH = 'odds.raw.unibet_dom';

function oddsLike(s){ return /\B[+-]\d{2,4}\b/.test(s) || /\b[1-9]\.\d{2}\b/.test(s); }

(async ()=>{
  console.log('[UB_DOM_FINAL] Starting with built-in Playwright v1.45.0');
  const pub = redis.createClient({ url: REDIS_URL });
  await pub.connect();
  console.log('[UB_DOM_FINAL] Redis connected');

  const browser = await chromium.launch({ headless:true, args:['--no-sandbox','--disable-setuid-sandbox']});
  const ctx = await browser.newContext({ locale:'en-US', timezoneId:'America/New_York' });
  const page = await ctx.newPage();
  const client = await ctx.newCDPSession(page);
  await client.send('Network.enable');

  let jsonHits=0, domHits=0;

  client.on('Network.responseReceived', async ev=>{
    try{
      const url = ev.response?.url||'';
      if (!url) return;
      if (!/kambi|offering/i.test(url)) return;
      if (!/\.json(\?|$)/i.test(url)) return;
      if (ev.response.status !== 200) return;
      const body = await client.send('Network.getResponseBody', { requestId: ev.requestId });
      const text = (body?.body || '');
      if (text.trim().startsWith('{')) {
        jsonHits++;
        console.log(`[UB_DOM_FINAL] JSON hit #${jsonHits} from ${url.substring(0,60)}`);
        await pub.publish(CH, JSON.stringify({brand_hint:'unibet', transport:'dom-json', page_url:url, frame:text.slice(0,500000)}));
      }
    }catch(e){}
  });

  try {
    console.log(`[UB_DOM_FINAL] Navigating to ${START_URL}`);
    await page.goto(START_URL, { waitUntil:'domcontentloaded', timeout:60000 });
    console.log('[UB_DOM_FINAL] Page loaded');
  } catch(e){
    console.log(`[UB_DOM_FINAL] Navigation error: ${e.message}`);
  }

  const t0 = Date.now();
  while ((Date.now()-t0)/1000 < DURATION_SEC){
    try { await page.evaluate(()=>window.scrollBy(0,300)); } catch(e){}
    await page.waitForTimeout(2500);
    try{
      const texts = await page.evaluate(()=>{
        const out=[];
        for (const el of document.querySelectorAll('button,[role="button"],span,div')) {
          const s=(el.innerText||'').trim();
          if (s) out.push(s);
        }
        return out.slice(0,3000);
      });
      const cands = Array.from(new Set(texts.filter(oddsLike))).slice(0,200);
      if (cands.length){
        domHits += cands.length;
        console.log(`[UB_DOM_FINAL] Found ${cands.length} odds-like texts`);
        await pub.publish(CH, JSON.stringify({brand_hint:'unibet', transport:'dom-text', sample:cands.slice(0,20), count:cands.length}));
      }
    }catch(e){}
  }

  await pub.publish(CH, JSON.stringify({brand_hint:'unibet', transport:'dom-summary', jsonHits, domHits}));
  await browser.close();
  await pub.quit();
  console.log(`[UB_DOM_FINAL] Complete: ${jsonHits} JSON hits, ${domHits} DOM odds`);
  console.log(JSON.stringify({ jsonHits, domHits }));
})();
