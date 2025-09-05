const { chromium } = require('playwright');
(async () => {
  const url = process.env.URL || 'https://md.betparx.com/kambi#home';
  const headed = process.env.HEADED === 'true';
  const browser = await chromium.launch({ headless: !headed, args: ['--disable-blink-features=AutomationControlled'] });
  const ctx = await browser.newContext({
    userAgent: 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    locale: 'en-US',
    timezoneId: 'America/New_York',
    geolocation: { latitude: 39.0458, longitude: -76.6413 }, // Maryland-ish
    permissions: ['geolocation'],
    viewport: { width: 1280, height: 860 }
  });
  const page = await ctx.newPage();
  let token = null, samples = [];
  page.on('request', r => {
    const u = r.url();
    if (u.includes('offering') && (u.includes('kambicdn.com') || u.includes('kambi.com')) && u.match(/\/offering\/v2018\/([^/]+)\//)) {
      const t = u.match(/\/offering\/v2018\/([^/]+)\//)[1];
      token = token || t;
      samples.push(u);
    }
  });
  await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 60000 });
  for (let i=0;i<12;i++){ await page.waitForTimeout(2500); await page.evaluate(()=>window.scrollBy(0,300)); }
  await browser.close();
  console.log(JSON.stringify({ token, samples }, null, 2));
  process.exit(0);
})();
