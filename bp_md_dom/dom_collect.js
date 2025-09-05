const { chromium } = require('playwright');
const fs = require('fs');

(async () => {
  const START = process.env.START || 'https://md.betparx.com/kambi#sports-hub/american_football/nfl';
  const OUT = process.env.OUT || '/tmp/bp_md_sample.json';
  const headed = process.env.HEADED === 'true';
  const ctx = await chromium.launchPersistentContext('/tmp/bp_md_dom_profile', {
    headless: !headed,
    viewport: { width: 1280, height: 860 },
    geolocation: { latitude: 39.0458, longitude: -76.6413 }, // Maryland-ish
    permissions: ['geolocation'],
    timezoneId: 'America/New_York',
    userAgent: 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
  });

  // Light stealth
  await ctx.addInitScript(() => {
    Object.defineProperty(navigator,'webdriver',{get:()=>undefined});
    Object.defineProperty(navigator,'languages',{get:()=>['en-US','en']});
    Object.defineProperty(navigator,'platform',{get:()=> 'MacIntel'});
    window.chrome = window.chrome || { runtime:{} };
  });

  const page = await ctx.newPage();
  await page.goto(START, { waitUntil: 'domcontentloaded', timeout: 60000 });

  // Wait a bit for content to load
  await page.waitForTimeout(5000);

  // Find the Kambi iframe (some pages render directly, so fallback to main frame)
  const frames = page.frames();
  const kframe = frames.find(f => (f.url()||'').includes('kambi')) || page;

  // Wait for odds grid to exist (broad selector set; we'll sniff and adapt)
  await kframe.waitForSelector('a[href*="#event/"], [data-testid*="event"], [class*="event"], button', { timeout: 30000 }).catch(()=>{});

  // Extract a small sample: event links + visible price cells in the grid
  const sample = await kframe.evaluate(() => {
    const events = Array.from(document.querySelectorAll('a[href*="#event/"]'))
      .slice(0, 20)
      .map(a => ({ href: a.getAttribute('href'), text: a.textContent?.trim() || '' }));

    // Very permissive cell scrape: look for typical price-like nodes in rows
    const allText = document.body.innerText || '';
    const lines = allText.split('\n').slice(0, 500);
    const prices = [];

    lines.forEach(line => {
      // Pick out numbers that look like American odds (+123/-110) and totals (O/U 47.5)
      const amOdds = line.match(/[+-]\d{3,4}/g) || [];
      const totals = line.match(/\b(?:O|U)\s?\d+(\.\d+)?\b/gi) || [];
      if (amOdds.length || totals.length) {
        prices.push({ snippet: line.slice(0, 200), amOdds: amOdds.slice(0,6), totals: totals.slice(0,2) });
      }
    });

    // Try to infer an event_id from any visible event links
    const eventIds = events
      .map(e => (e.href||'').match(/#event\/(\d+)/)?.[1])
      .filter(Boolean)
      .slice(0, 10);

    // Also check for any buttons with odds-like text
    const buttons = Array.from(document.querySelectorAll('button'))
      .map(b => b.textContent?.trim() || '')
      .filter(t => /^[+-]\d{3,4}$/.test(t))
      .slice(0, 20);

    return { url: location.href, frames: window.top === window ? 'top' : 'child', eventIds, events, prices, buttons };
  });

  fs.writeFileSync(OUT, JSON.stringify(sample, null, 2));
  console.log(JSON.stringify({ ok:true, out:OUT, found_events: sample.eventIds?.length || 0, price_rows: sample.prices?.length || 0, buttons: sample.buttons?.length || 0 }, null, 2));

  await ctx.close();
})();
