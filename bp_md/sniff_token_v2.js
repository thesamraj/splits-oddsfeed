const { chromium } = require('playwright');
(async () => {
  const url = process.env.URL || 'https://md.betparx.com';
  const browser = await chromium.launch({
    headless: false,
    args: ['--disable-blink-features=AutomationControlled']
  });
  const ctx = await browser.newContext({
    userAgent: 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
    locale: 'en-US',
    timezoneId: 'America/New_York',
    geolocation: { latitude: 39.0458, longitude: -76.6413 },
    permissions: ['geolocation']
  });
  const page = await ctx.newPage();
  let token = null, samples = [];

  // Monitor all requests
  page.on('request', r => {
    const u = r.url();
    // Look for any Kambi API calls
    if (u.includes('kambi') || u.includes('offering')) {
      console.log('Kambi URL:', u);
      if (u.match(/\/v2018\/([^/]+)\//)) {
        const t = u.match(/\/v2018\/([^/]+)\//)[1];
        token = token || t;
        samples.push(u);
      }
    }
  });

  console.log('Going to:', url);
  await page.goto(url, { waitUntil: 'networkidle', timeout: 60000 });

  // Try clicking on sports section
  try {
    await page.click('text=Sports', { timeout: 5000 });
  } catch {}

  // Wait and scroll
  for (let i=0; i<5; i++){
    await page.waitForTimeout(3000);
    await page.evaluate(()=>window.scrollBy(0,300));
  }

  await browser.close();
  console.log(JSON.stringify({ token, samples, url }, null, 2));
})();
