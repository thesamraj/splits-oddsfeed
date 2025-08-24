import asyncio
import os
import random
from typing import List
from playwright.async_api import Page

# Environment variables for controlling enhanced scrolling
KAMBI_AUTOSCROLL = os.getenv("KAMBI_AUTOSCROLL", "0").lower() == "1"
KAMBI_AUTOSCROLL_PASSES = int(os.getenv("KAMBI_AUTOSCROLL_PASSES", "8"))
KAMBI_CLICK_MORE = os.getenv("KAMBI_CLICK_MORE", "0").lower() == "1"


def log(msg: str):
    print(f"[kambi-browser-enhanced] {msg}", flush=True)


async def enhanced_scroll_and_click_more(page: Page, max_passes: int = None) -> int:
    """Enhanced scrolling with 'Show More' button clicking"""
    if not KAMBI_AUTOSCROLL:
        return 0

    passes = max_passes or KAMBI_AUTOSCROLL_PASSES
    total_clicks = 0

    for pass_num in range(passes):
        log(f"scroll pass {pass_num + 1}/{passes}")

        # Scroll down aggressively to trigger lazy loading
        for scroll_step in range(5):
            await page.mouse.wheel(0, 1200)
            await asyncio.sleep(0.3)

        # Look for and click "Show More" buttons if enabled
        if KAMBI_CLICK_MORE:
            more_buttons_clicked = await click_show_more_buttons(page)
            total_clicks += more_buttons_clicked

            if more_buttons_clicked > 0:
                log(f"clicked {more_buttons_clicked} 'Show More' buttons")
                # Wait for content to load after clicking
                await asyncio.sleep(2)

        # Brief pause between passes
        await asyncio.sleep(random.uniform(0.5, 1.0))

    log(f"enhanced scroll completed: {passes} passes, {total_clicks} buttons clicked")
    return total_clicks


async def click_show_more_buttons(page: Page) -> int:
    """Find and click all visible 'Show More' style buttons"""
    if not KAMBI_CLICK_MORE:
        return 0

    # Comprehensive list of selectors for "Show More" buttons
    show_more_selectors = [
        'button:has-text("Show more")',
        'button:has-text("Show More")',
        'button:has-text("Load more")',
        'button:has-text("Load More")',
        'button:has-text("More events")',
        'button:has-text("More Events")',
        'button:has-text("View more")',
        'button:has-text("View More")',
        '[data-test*="showMore"]',
        '[data-testid*="show-more"]',
        '[data-testid*="load-more"]',
        '[class*="show-more"]',
        '[class*="load-more"]',
        '[class*="expand"]',
        'button[aria-label*="Show more"]',
        'button[aria-label*="Load more"]',
        'a:has-text("Show more")',
        'a:has-text("Load more")',
        ".show-more-button",
        ".load-more-button",
        ".expand-button",
    ]

    total_clicked = 0

    for selector in show_more_selectors:
        try:
            # Find all matching elements
            elements = page.locator(selector)
            count = await elements.count()

            for i in range(count):
                try:
                    element = elements.nth(i)

                    # Check if element is visible and clickable
                    if (
                        await element.is_visible(timeout=1000)
                        and await element.is_enabled()
                    ):
                        # Scroll element into view
                        await element.scroll_into_view_if_needed()
                        await asyncio.sleep(0.2)

                        # Click the button
                        await element.click()
                        total_clicked += 1

                        # Wait for potential content loading
                        await asyncio.sleep(1.5)

                except Exception:
                    # Continue to next element if this one fails
                    continue

        except Exception:
            # Continue to next selector if this one fails
            continue

    return total_clicked


async def enhanced_discover_events_with_scroll(
    page: Page, limit: int = 12
) -> List[str]:
    """Discover events with enhanced scrolling to load more content"""
    # First do enhanced scrolling
    await enhanced_scroll_and_click_more(page)

    # Then use original event discovery logic
    links = []
    try:
        event_selectors = [
            "[data-testid*='event']",
            "[class*='event']",
            "[class*='match']",
            "a[href*='event']",
            "[role='button']:has-text('vs')",
            ".event-card",
            ".match-card",
            "[data-test*='event']",
            "[data-qa*='event']",
        ]

        for selector in event_selectors:
            try:
                events = page.locator(selector)
                count = await events.count()

                for i in range(min(count, limit - len(links))):
                    try:
                        event = events.nth(i)
                        if await event.is_visible(timeout=1000):
                            # Skip live video elements
                            text = await event.text_content() or ""
                            if any(
                                skip in text.lower()
                                for skip in ["live", "video", "stream", "watch"]
                            ):
                                continue
                            links.append(f"selector_{selector}_{i}")
                            if len(links) >= limit:
                                break
                    except Exception:
                        continue
            except Exception:
                continue

        log(f"enhanced discovery found {len(links)} event links")
        return links[:limit]
    except Exception as e:
        log(f"enhanced_discover_events_with_scroll failed: {e}")
        return []
