import asyncio
import json
from playwright.async_api import async_playwright


async def monitor_fanduel():
    """Monitor FanDuel network traffic to find API endpoints"""

    api_endpoints = []
    websocket_urls = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=False, args=["--no-sandbox"]  # Run with UI to see what's happening
        )

        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080},
        )

        page = await context.new_page()

        # Monitor all responses
        async def handle_response(response):
            url = response.url
            if response.status == 200:
                # Look for interesting endpoints
                if any(
                    keyword in url
                    for keyword in [
                        "api",
                        "feed",
                        "odds",
                        "events",
                        "markets",
                        "sports",
                    ]
                ):
                    try:
                        content_type = response.headers.get("content-type", "")
                        if "json" in content_type:
                            body = await response.text()
                            if len(body) > 100:
                                print(f"API Found: {url[:150]}")
                                api_endpoints.append(
                                    {
                                        "url": url,
                                        "size": len(body),
                                        "sample": body[:500],
                                    }
                                )
                    except:
                        pass

        # Monitor WebSocket connections
        async def handle_websocket(ws):
            print(f"WebSocket: {ws.url}")
            websocket_urls.append(ws.url)

            # Listen to messages
            async def on_message(message):
                print(f"WS Message: {message[:200]}")

            ws.on("framereceived", on_message)

        page.on("response", handle_response)
        page.on("websocket", handle_websocket)

        print("Loading FanDuel...")
        await page.goto("https://sportsbook.fanduel.com/", wait_until="networkidle")

        # Navigate to NFL section
        print("Navigating to NFL...")
        await page.wait_for_timeout(3000)

        # Try to click on NFL if visible
        try:
            await page.click("text=NFL", timeout=5000)
            await page.wait_for_timeout(5000)
        except:
            print("Could not click NFL, continuing...")

        # Scroll to trigger lazy loading
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await page.wait_for_timeout(3000)

        print(f"\nFound {len(api_endpoints)} API endpoints")
        print(f"Found {len(websocket_urls)} WebSocket connections")

        # Save findings
        with open("fd_apis.json", "w") as f:
            json.dump(
                {"apis": api_endpoints, "websockets": websocket_urls}, f, indent=2
            )

        await browser.close()

        return api_endpoints, websocket_urls


if __name__ == "__main__":
    apis, sockets = asyncio.run(monitor_fanduel())

    print("\n=== Summary ===")
    print(f"APIs: {len(apis)}")
    print(f"WebSockets: {len(sockets)}")

    if apis:
        print("\nTop API endpoints:")
        for api in apis[:5]:
            print(f"  - {api['url'][:100]} ({api['size']} bytes)")
