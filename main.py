import asyncio
import json
import sys
import xml.etree.ElementTree as ET
from collections import defaultdict
from urllib.parse import urljoin, urlparse, parse_qs

import aiohttp
from bs4 import BeautifulSoup


def has_utm(url: str) -> bool:
    params = parse_qs(urlparse(url).query)
    return any(k.startswith("utm_") for k in params)


def normalize(url: str) -> str:
    parsed = urlparse(url)
    path = parsed.path.rstrip("/") or "/"
    return parsed._replace(fragment="", path=path).geturl()


async def fetch(session: aiohttp.ClientSession, url: str) -> tuple[str, str] | None:
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status == 200 and "text/html" in resp.headers.get("Content-Type", ""):
                return str(resp.url), await resp.text()
    except Exception:
        pass
    return None


async def crawl(start_url: str) -> dict[str, list[str]]:
    parsed_start = urlparse(start_url)
    base_domain = parsed_start.netloc

    visited: set[str] = set()
    queue: asyncio.Queue[str] = asyncio.Queue()
    sem = asyncio.Semaphore(10)

    normalized_start = normalize(start_url)
    visited.add(normalized_start)
    await queue.put(normalized_start)

    utm_report: dict[str, list[str]] = defaultdict(list)

    async def process(session: aiohttp.ClientSession, url: str) -> None:
        async with sem:
            result = await fetch(session, url)
        if result is None:
            return
        final_url, html = result
        if urlparse(final_url).netloc != base_domain:
            return
        soup = BeautifulSoup(html, "html.parser")
        for tag in soup.find_all("a", href=True):
            href = tag["href"].strip()
            absolute = normalize(urljoin(final_url, href))
            parsed = urlparse(absolute)
            if parsed.scheme not in ("http", "https"):
                continue
            if has_utm(absolute):
                utm_report[final_url].append(absolute)
            if parsed.netloc == base_domain and absolute not in visited:
                visited.add(absolute)
                await queue.put(absolute)
        print(f"  crawled: {final_url} ({len(utm_report.get(final_url, []))} utm links found)")

    connector = aiohttp.TCPConnector(limit=10)
    async with aiohttp.ClientSession(connector=connector) as session:
        pending: set[asyncio.Task] = set()
        while True:
            while not queue.empty():
                url = await queue.get()
                task = asyncio.create_task(process(session, url))
                pending.add(task)
                task.add_done_callback(pending.discard)
            if not pending:
                break
            await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)

    return dict(utm_report)


_SITEMAP_NS = "http://www.sitemaps.org/schemas/sitemap/0.9"


async def fetch_sitemap_urls(session: aiohttp.ClientSession, sitemap_url: str) -> list[str]:
    try:
        async with session.get(sitemap_url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status != 200:
                return []
            text = await resp.text()
    except Exception:
        return []

    root = ET.fromstring(text)

    if "sitemapindex" in root.tag:
        child_urls = [el.text.strip() for el in root.findall(f"{{{_SITEMAP_NS}}}sitemap/{{{_SITEMAP_NS}}}loc") if el.text]
        results = await asyncio.gather(*(fetch_sitemap_urls(session, u) for u in child_urls))
        return [url for sub in results for url in sub]

    return [el.text.strip() for el in root.findall(f"{{{_SITEMAP_NS}}}url/{{{_SITEMAP_NS}}}loc") if el.text]


async def crawl_sitemap(sitemap_url: str) -> dict[str, list[str]]:
    utm_report: dict[str, list[str]] = defaultdict(list)
    sem = asyncio.Semaphore(10)

    async def process(session: aiohttp.ClientSession, url: str) -> None:
        async with sem:
            result = await fetch(session, url)
        if result is None:
            return
        final_url, html = result
        soup = BeautifulSoup(html, "html.parser")
        for tag in soup.find_all("a", href=True):
            href = tag["href"].strip()
            absolute = normalize(urljoin(final_url, href))
            parsed = urlparse(absolute)
            if parsed.scheme not in ("http", "https"):
                continue
            if has_utm(absolute):
                utm_report[final_url].append(absolute)
        print(f"  crawled: {final_url} ({len(utm_report.get(final_url, []))} utm links found)")

    connector = aiohttp.TCPConnector(limit=10)
    async with aiohttp.ClientSession(connector=connector) as session:
        print(f"  fetching sitemap: {sitemap_url}")
        page_urls = await fetch_sitemap_urls(session, sitemap_url)
        print(f"  found {len(page_urls)} URL(s) in sitemap")
        await asyncio.gather(*(process(session, url) for url in page_urls))

    return dict(utm_report)


def main():
    if len(sys.argv) < 2:
        print("Usage: utmcrawler <url> [output.json]")
        print("       utmcrawler --sitemap <sitemap_url> [output.json]")
        sys.exit(1)

    if sys.argv[1] == "--sitemap":
        if len(sys.argv) < 3:
            print("Usage: utmcrawler --sitemap <sitemap_url> [output.json]")
            sys.exit(1)
        sitemap_url = sys.argv[2]
        output_file = sys.argv[3] if len(sys.argv) > 3 else "utm_report.json"
        if not sitemap_url.startswith(("http://", "https://")):
            sitemap_url = "https://" + sitemap_url
        print(f"Crawling sitemap {sitemap_url} ...")
        report = asyncio.run(crawl_sitemap(sitemap_url))
    else:
        start_url = sys.argv[1]
        output_file = sys.argv[2] if len(sys.argv) > 2 else "utm_report.json"
        if not start_url.startswith(("http://", "https://")):
            start_url = "https://" + start_url
        print(f"Crawling {start_url} ...")
        report = asyncio.run(crawl(start_url))

    total = sum(len(v) for v in report.values())
    print(f"\nFound {total} UTM link(s) across {len(report)} page(s).")

    with open(output_file, "w") as f:
        json.dump(report, f, indent=2)

    print(f"Report saved to {output_file}")


if __name__ == "__main__":
    main()
