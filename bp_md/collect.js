const { chromium } = require('playwright');
const fs = require('fs');

function sleep(ms){ return new Promise(r=>setTimeout(r,ms)); }

// walk all nodes incl. shadow roots and return texts that look like odds
async function extractDeep(frame){
  return frame.evaluate(() => {
    const res = { events: [], prices: [] };

    // Pierce shadow DOM
    const allNodes = [];
    const push = n => {
      allNodes.push(n);
      if(n.shadowRoot) {
        n.shadowRoot.querySelectorAll('*').forEach(push);
      }
    };
    document.querySelectorAll('*').forEach(push);

    // Event links / ids
    allNodes.forEach(n=>{
      if(!n.getAttribute) return;
      const href = n.getAttribute('href') || '';
      const m = href.match(/#event\/(\d+)/);
      if (m) res.events.push({ id: m[1], title: (n.textContent||'').trim().slice(0,120) });
    });

    // Price-like snippets - be more aggressive
    allNodes.forEach(n=>{
      const t = (n.textContent||'').trim();
      if(!t || t.length > 200) return;
      // American odds pattern
      const am = t.match(/[+-]\d{3,4}/g) || [];
      // Totals pattern
      const tot = t.match(/\b(?:O|U)\s?\d+(?:\.\d+)?\b/gi) || [];
      // Decimal odds
      const dec = t.match(/\b\d\.\d{2}\b/g) || [];

      if (am.length>=1 || tot.length>=1 || dec.length>=2) {
        res.prices.push({
          snippet: t.slice(0,160),
          am: am.slice(0,4),
          tot: tot.slice(0,2),
          dec: dec.slice(0,4)
        });
      }
    });

    // Also check for any betting-related text
    const bodyText = document.body.innerText || '';
    const lines = bodyText.split('\n').filter(l => l.length > 0 && l.length < 200);
    lines.forEach(line => {
      if(line.match(/[+-]\d{3,4}/) || line.match(/\b\d\.\d{2}\b/)) {
        res.prices.push({ snippet: line.slice(0,160), source: 'body' });
      }
    });

    // Dedup light
    const seenEv = new Set();
    res.events = res.events.filter(e => !seenEv.has(e.id) && seenEv.add(e.id));
    res.prices = res.prices.slice(0, 100); // Cap at 100

    return res;
  });
}

(async () => {
  const headed = process.env.HEADED === 'true';
  const ctx = await chromium.launchPersistentContext('/tmp/bp_md_profile', {
    headless: !headed,
    viewport: { width: 1320, height: 900 },
    geolocation: { latitude: 39.0458, longitude: -76.6413 },
    permissions: ['geolocation'],
    timezoneId: 'America/New_York',
    userAgent: 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
  });

  await ctx.addInitScript(() => {
    Object.defineProperty(navigator,'webdriver',{get:()=>undefined});
    Object.defineProperty(navigator,'languages',{get:()=>['en-US','en']});
  });

  const page = await ctx.newPage();

  try {
    // Step 1: Try direct MD URL first
    console.log('Going directly to MD BetParx...');
    await page.goto('https://md.betparx.com/', { waitUntil: 'domcontentloaded', timeout: 60000 });
    await sleep(3000);

    // Check if we need to select state
    const stateSelector = await page.$('text=Select your state') || await page.$('text=Choose State');
    if(stateSelector) {
      console.log('State selector found, clicking Maryland...');
      await page.click('text=Maryland').catch(() => {});
      await sleep(2000);
    }

    // Try to navigate to sports/kambi section
    console.log('Looking for sports section...');
    await page.click('text=Sports').catch(() => {
      console.log('Sports link not found, trying Sportsbook...');
      return page.click('text=Sportsbook');
    }).catch(() => {});

    await sleep(3000);

    // Check current URL
    console.log('Current URL:', page.url());

    // If not on kambi page, try direct navigation
    if(!page.url().includes('kambi')) {
      console.log('Navigating to Kambi sports hub...');
      await page.goto('https://md.betparx.com/kambi#sports-hub/american_football/nfl', {
        waitUntil: 'domcontentloaded',
        timeout: 60000
      });
    }

    await sleep(3000);

    // Scroll to trigger lazy loading
    console.log('Scrolling to trigger content...');
    for (let i=0; i<5; i++){
      await page.mouse.wheel(0, 500);
      await sleep(500);
    }

    // Step 3: Extract from all frames
    const frames = page.frames();
    console.log(`Found ${frames.length} frames`);

    let allData = { events: [], prices: [] };

    for(const frame of frames) {
      try {
        const data = await extractDeep(frame);
        allData.events.push(...data.events);
        allData.prices.push(...data.prices);
      } catch(e) {
        console.log('Frame extract error:', e.message);
      }
    }

    // Save artifacts
    const OUT = '/tmp/bp_md_sample.json';
    fs.writeFileSync(OUT, JSON.stringify({
      url: page.url(),
      events: allData.events.slice(0,30),
      prices: allData.prices.slice(0,60)
    }, null, 2));

    await page.screenshot({ path: '/tmp/bp_md_screen.png', fullPage: false }).catch(()=>{});

    console.log(JSON.stringify({
      found_events: allData.events.length,
      price_rows: allData.prices.length,
      url: page.url()
    }));

  } catch(e) {
    console.error('Error:', e.message);
    console.log(JSON.stringify({ found_events: 0, price_rows: 0, error: e.message }));
  }

  await ctx.close();
})();
