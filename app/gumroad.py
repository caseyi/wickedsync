"""
Gumroad client using Playwright (headless Chromium).

Why Playwright instead of plain requests?
  Gumroad's product pages are React-rendered — the file list is injected by JS.
  Playwright runs Chromium *inside the Docker container*, which shares the NAS's
  public IP via `network_mode: host`. That means the CDN download URLs are
  IP-locked to the NAS, so the NAS can curl them directly.
"""
import asyncio
import json
import logging
import re
import urllib.parse
from typing import Optional

from playwright.async_api import async_playwright, BrowserContext, Page

logger = logging.getLogger(__name__)

GUMROAD_BASE = "https://gumroad.com"
APP_GUMROAD = "https://app.gumroad.com"


def _parse_cookies(cookie_str: str) -> list[dict]:
    """Convert 'key=val; key2=val2' string into Playwright cookie dicts."""
    cookies = []
    for part in cookie_str.split(";"):
        part = part.strip()
        if "=" not in part:
            continue
        key, _, value = part.partition("=")
        cookies.append({
            "name": key.strip(),
            "value": value.strip(),
            "domain": ".gumroad.com",
            "path": "/",
        })
    return cookies


class GumroadClient:
    def __init__(self, cookies_str: str):
        self._cookies_str = cookies_str
        self._cookies = _parse_cookies(cookies_str)

    async def _make_context(self, playwright) -> BrowserContext:
        browser = await playwright.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"],
        )
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 800},
        )
        await context.add_cookies(self._cookies)
        return context

    # ── Library scan ──────────────────────────────────────────────────────────

    async def get_library_purchases(self) -> list[dict]:
        """
        Scrape app.gumroad.com/library and return a list of purchases:
          [{'name': '...', 'content_url': 'https://gumroad.com/d/...'}, ...]
        """
        async with async_playwright() as p:
            context = await self._make_context(p)
            page = await context.new_page()
            try:
                return await self._scrape_library(page)
            finally:
                await context.browser.close()

    async def _scrape_library(self, page: Page) -> list[dict]:
        purchases = []
        await page.goto(f"{APP_GUMROAD}/library", wait_until="networkidle", timeout=30000)

        # Gumroad library uses infinite scroll — scroll to bottom to load all
        prev_count = 0
        for _ in range(20):  # max 20 scroll attempts
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await asyncio.sleep(1.5)
            items = await page.query_selector_all('[data-product-id], .product-card')
            if len(items) == prev_count:
                break
            prev_count = len(items)

        # Extract content URLs
        links = await page.query_selector_all('a[href*="/d/"]')
        seen = set()
        for link in links:
            href = await link.get_attribute("href")
            if not href or href in seen:
                continue
            seen.add(href)
            if not href.startswith("http"):
                href = GUMROAD_BASE + href
            # Try to get model name from nearby heading
            name = await link.inner_text() or "Unknown"
            name = name.strip()
            purchases.append({"name": name, "content_url": href})

        return purchases

    # ── Content URL discovery ──────────────────────────────────────────────────

    async def resolve_content_url(self, product_url: str) -> Optional[str]:
        """
        Given a product/discount URL like:
          https://3dwicked.gumroad.com/l/BladeSculpture/w3g5s2s
        Return the purchase content URL:
          https://gumroad.com/d/af101d...
        Requires the product to already be claimed/purchased.
        """
        async with async_playwright() as p:
            context = await self._make_context(p)
            page = await context.new_page()
            try:
                await page.goto(product_url, wait_until="networkidle", timeout=30000)

                # Look for "View content" / "Access purchase" button/link
                for selector in [
                    'a[href*="/d/"]',
                    'a:has-text("View content")',
                    'a:has-text("Access")',
                    'a:has-text("Download")',
                ]:
                    el = await page.query_selector(selector)
                    if el:
                        href = await el.get_attribute("href")
                        if href:
                            if not href.startswith("http"):
                                href = GUMROAD_BASE + href
                            return href

                # If we were redirected directly to the content page
                current = page.url
                if "/d/" in current:
                    return current

            except Exception as e:
                logger.error(f"resolve_content_url error for {product_url}: {e}")
            finally:
                await context.browser.close()

        return None

    # ── File discovery ─────────────────────────────────────────────────────────

    async def get_download_files(self, content_url: str) -> list[dict]:
        """
        Given https://gumroad.com/d/[hash], return all downloadable files:
          [{'filename': 'Wicked - Blade (Non Supported).zip',
            'cdn_url': 'https://d2dw6lv4z9w0e2.cloudfront.net/...',
            'size_bytes': 0}, ...]

        Strategy:
          1. Load the page with session cookies.
          2. Intercept all network requests to capture CDN redirect URLs
             (files.gumroad.com / cloudfront.net) as download buttons are clicked.
          3. Extract filenames from the redirected URL path or Content-Disposition.
        """
        async with async_playwright() as p:
            context = await self._make_context(p)
            page = await context.new_page()
            try:
                return await self._collect_file_links(page, content_url)
            finally:
                await context.browser.close()

    async def _collect_file_links(self, page: Page, content_url: str) -> list[dict]:
        files: list[dict] = []
        cdn_responses: dict[str, str] = {}  # cdn_url → filename

        # Track all requests to capture CDN redirects
        async def on_response(response):
            url = response.url
            if "files.gumroad.com" in url or "cloudfront.net" in url:
                filename = _extract_filename_from_url(url)
                cdn_responses[url] = filename

        page.on("response", on_response)

        await page.goto(content_url, wait_until="networkidle", timeout=30000)

        # First, try to find file data embedded as JSON in the page
        embedded = await self._extract_embedded_json(page)
        if embedded:
            return embedded

        # Otherwise, click each download button to trigger the CDN redirects
        purchase_hash = content_url.rstrip("/").split("/")[-1]

        download_links = await page.query_selector_all(
            'a[href*="product_files"], button[data-file-id], a[href*="/r/"]'
        )

        if not download_links:
            logger.warning(f"No download links found on {content_url}")
            return []

        for link in download_links:
            try:
                # Middle-click to navigate in same page context
                href = await link.get_attribute("href") or ""
                file_id_match = re.search(r'product_file_ids\[\]=([a-f0-9]+)', href)
                if not file_id_match:
                    # Try data attribute
                    file_id = await link.get_attribute("data-file-id") or ""
                else:
                    file_id = file_id_match.group(1)

                if file_id:
                    # Construct the redirect URL and navigate to it
                    redirect_url = f"{GUMROAD_BASE}/r/{purchase_hash}/product_files?product_file_ids[]={file_id}"
                    # Use a new tab to avoid navigating away from the file list
                    new_tab = await page.context.new_page()
                    new_tab.on("response", on_response)
                    await new_tab.goto(redirect_url, wait_until="commit", timeout=15000)
                    await asyncio.sleep(0.5)
                    await new_tab.close()

            except Exception as e:
                logger.debug(f"Error clicking download link: {e}")

        # Build file list from captured CDN URLs
        for cdn_url, filename in cdn_responses.items():
            files.append({
                "filename": filename,
                "cdn_url": cdn_url,
                "size_bytes": 0,
            })

        return files

    async def _extract_embedded_json(self, page: Page) -> list[dict]:
        """
        Gumroad sometimes embeds all product file data as JSON in a <script> tag.
        Try to extract it before falling back to clicking.
        """
        try:
            # Look for React props or embedded JSON
            data = await page.evaluate("""
                () => {
                    // Try window.__INITIAL_STATE__ or similar
                    if (window.__INITIAL_STATE__) return window.__INITIAL_STATE__;
                    // Try to find JSON in script tags
                    const scripts = document.querySelectorAll('script[type="application/json"]');
                    for (const s of scripts) {
                        try { return JSON.parse(s.textContent); } catch {}
                    }
                    // Try Gumroad's React root
                    const root = document.getElementById('js-product-permalink') ||
                                 document.getElementById('app');
                    if (root && root.dataset) {
                        try { return JSON.parse(root.dataset.props || '{}'); } catch {}
                    }
                    return null;
                }
            """)

            if not data:
                return []

            # Navigate nested JSON to find product_files
            def find_product_files(obj, depth=0):
                if depth > 8:
                    return []
                if isinstance(obj, list):
                    for item in obj:
                        r = find_product_files(item, depth + 1)
                        if r:
                            return r
                if isinstance(obj, dict):
                    if 'product_files' in obj:
                        return obj['product_files']
                    if 'productFiles' in obj:
                        return obj['productFiles']
                    for v in obj.values():
                        r = find_product_files(v, depth + 1)
                        if r:
                            return r
                return []

            product_files = find_product_files(data)
            if not product_files:
                return []

            # Get purchase hash from URL
            current_url = page.url
            purchase_hash = current_url.rstrip("/").split("/")[-1]

            result = []
            for pf in product_files:
                file_id = str(pf.get('id', pf.get('external_id', '')))
                filename = pf.get('filename') or pf.get('name') or f"file_{file_id}"
                if file_id:
                    cdn_url = f"{GUMROAD_BASE}/r/{purchase_hash}/product_files?product_file_ids[]={file_id}"
                    result.append({
                        "filename": filename,
                        "cdn_url": cdn_url,
                        "size_bytes": pf.get('size', 0),
                    })

            return result

        except Exception as e:
            logger.debug(f"Embedded JSON extraction failed: {e}")
            return []


def _extract_filename_from_url(url: str) -> str:
    """
    Extract a human-readable filename from a CDN URL.
    CloudFront/S3 URLs usually have the filename in the path, URL-encoded.
    """
    try:
        parsed = urllib.parse.urlparse(url)
        path = parsed.path
        # Last segment of path
        name = path.split("/")[-1]
        name = urllib.parse.unquote(name)
        if name and "." in name:
            return name
    except Exception:
        pass
    return "unknown_file.zip"
