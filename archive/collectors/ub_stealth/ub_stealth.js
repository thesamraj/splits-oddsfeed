const { chromium } = require('playwright');
const redis = require('redis');

(async ()=>{
  console.log('[UB_STEALTH] Starting CDP collector');
  const r = redis.createClient({ url: process.env.REDIS_URL || 'redis://broker:6379/0' });
  await r.connect();
  console.log('[UB_STEALTH] Redis connected');

  const browser = await chromium.launch({ headless: true, args: ['--no-sandbox','--disable-setuid-sandbox'] });
  const ctx = await browser.newContext({ locale:'en-US', timezoneId:'America/New_York' });
  const page = await ctx.newPage();
  const cdp = await ctx.newCDPSession(page);
  await cdp.send('Network.enable');
  await cdp.send('Target.setAutoAttach', { autoAttach:true, waitForDebuggerOnStart:false, flatten:true });

  // Capture offering .json responses and publish their bodies
  cdp.on('Network.responseReceived', async (ev)=>{
    try {
      const url = ev.response?.url || '';
      if (url.includes('offering') && url.includes('kambi') && url.endsWith('.json') && (ev.response.status==200)) {
        const body = await cdp.send('Network.getResponseBody', { requestId: ev.requestId });
        const text = body?.body || '';
        if (text && text.trim().startsWith('{')) {
          await r.publish('odds.raw.kambi', JSON.stringify({ brand_hint:'unibet', transport:'stealth', page_url:url, frame:text }));
          console.log(`[UB_STEALTH] Published frame from ${url.substring(0,60)}`);
        }
      }
    } catch(e){}
  });

  try {
    console.log('[UB_STEALTH] Navigating to Unibet...');
    await page.goto('https://pa.unibet.com/?page=sportsbook#live', { waitUntil:'domcontentloaded', timeout:60000 });
    console.log('[UB_STEALTH] Page loaded');
  } catch(e){
    console.log(`[UB_STEALTH] Navigation error: ${e.message}`);
  }

  // gentle scroll to trigger widgets
  setInterval(()=>page.evaluate(()=>window.scrollBy(0,160)).catch(()=>{}), 5000);
  // keep alive
  setInterval(()=>{}, 60000);
})();
