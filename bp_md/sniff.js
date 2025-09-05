const { chromium } = require('playwright'); const fs = require('fs');
(async () => {
  const url = process.env.URL || 'https://md.betparx.com/kambi#sports-hub/american_football/nfl';
  const headed = process.env.HEADED === 'true';
  const browser = await chromium.launch({ headless: !headed, args:['--disable-blink-features=AutomationControlled']});
  const ctx = await browser.newContext({
    userAgent:'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    locale:'en-US', timezoneId:'America/New_York',
    geolocation:{ latitude:39.0458, longitude:-76.6413 }, permissions:['geolocation'],
    viewport:{ width:1280, height:860 }
  });
  await ctx.addInitScript(() => {
    Object.defineProperty(navigator,'webdriver',{get:()=>undefined});
    Object.defineProperty(navigator,'languages',{get:()=>['en-US','en']});
    Object.defineProperty(navigator,'platform',{get:()=> 'MacIntel'});
    window.chrome = window.chrome || { runtime:{} };
  });
  const page = await ctx.newPage();
  const cdp = await ctx.newCDPSession(page);
  await cdp.send('Network.enable');
  await cdp.send('Target.setAutoAttach',{autoAttach:true,waitForDebuggerOnStart:false,flatten:true});
  let token=null, samples=[];
  const grab = (u)=>{ const m=u.match(/\/offering\/v2018\/([^/]+)\//); if(m){ token = token||m[1]; samples.push(u); } };
  cdp.on('Network.requestWillBeSent', e=>{ if(e.request && e.request.url) { const u=e.request.url; if(u.includes('kambicdn.com')&&u.includes('/offering/')) grab(u); }});
  cdp.on('Target.attachedToTarget', async ev => {
    try{
      await cdp.send('Target.sendMessageToTarget',{sessionId:ev.sessionId,message:JSON.stringify({id:1,method:'Network.enable'})});
    }catch{}
  });
  await page.goto(url, { waitUntil:'domcontentloaded', timeout:60000 });
  for(let i=0;i<12;i++){ await page.waitForTimeout(1500); await page.evaluate(() => window.scrollBy(0, 400)); }

  // Fallback #1: scrape iframe srcs
  if(!token){
    const ifr = await page.evaluate(() => Array.from(document.querySelectorAll('iframe')).map(f=>f.src));
    ifr.forEach(u=>grab(u||""));
  }
  // Fallback #2: look for widget script or config in the DOM / storage
  if(!token){
    const meta = await page.evaluate(() => {
      const scripts = Array.from(document.scripts).map(s=>s.src||'');
      const ls = Object.keys(localStorage).reduce((a,k)=>{a[k]=localStorage.getItem(k);return a;}, {});
      const ss = Object.keys(sessionStorage).reduce((a,k)=>{a[k]=sessionStorage.getItem(k);return a;}, {});
      return {scripts,ls,ss};
    });
    fs.writeFileSync('/tmp/bp_meta.json', JSON.stringify(meta,null,2));
    (meta.scripts||[]).forEach(u=>grab(u));
    Object.values(meta.ls||{}).concat(Object.values(meta.ss||{})).forEach(v=>{ if(typeof v==='string') grab(v); });
  }

  // Fallback #3: in-page fetch using discovered token
  let probe = null;
  if(token){
    try{
      probe = await page.evaluate(async (tok)=>{
        try{
          const url = `https://offering-api.kambicdn.com/offering/v2018/${tok}/event/live/open.json`;
          const r = await fetch(url,{ credentials:'include' });
          return { status:r.status, ok:r.ok, len:(await r.text()).length };
        }catch(e){ return { status:'ERR', err:String(e) }; }
      }, token);
    }catch(e){ probe = { status:'ERR', err:String(e) }; }
  }
  await browser.close();
  console.log(JSON.stringify({ token, samples, probe }, null, 2));
})();
