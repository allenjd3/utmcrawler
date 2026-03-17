import asyncio
import json
import sys
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


async def fetch(session: aiohttp.ClientSession, url: str) -> str | None:
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status == 200 and "text/html" in resp.headers.get("Content-Type", ""):
                return await resp.text()
    except Exception:
        pass
    return None


async def crawl(start_url: str) -> dict[str, list[str]]:
    parsed_start = urlparse(start_url)
    base_domain = parsed_start.netloc

    visited: set[str] = set()
    queue: asyncio.Queue[str] = asyncio.Queue()
    await queue.put(normalize(start_url))

    utm_report: dict[str, list[str]] = defaultdict(list)

    connector = aiohttp.TCPConnector(limit=10)
    async with aiohttp.ClientSession(connector=connector) as session:
        while not queue.empty():
            batch = []
            while not queue.empty() and len(batch) < 10:
                url = await queue.get()
                if url not in visited:
                    visited.add(url)
                    batch.append(url)

            if not batch:
                break

            results = await asyncio.gather(*(fetch(session, u) for u in batch))

            for page_url, html in zip(batch, results):
                if html is None:
                    continue

                soup = BeautifulSoup(html, "html.parser")
                for tag in soup.find_all("a", href=True):
                    href = tag["href"].strip()
                    absolute = normalize(urljoin(page_url, href))
                    parsed = urlparse(absolute)

                    if parsed.scheme not in ("http", "https"):
                        continue

                    if has_utm(absolute):
                        utm_report[page_url].append(absolute)

                    if parsed.netloc == base_domain and absolute not in visited:
                        await queue.put(absolute)

                print(f"  crawled: {page_url} ({len(utm_report.get(page_url, []))} utm links found)")

    return dict(utm_report)


def main():
    if len(sys.argv) < 2:
        print("Usage: utmcrawler <url> [output.json]")
        sys.exit(1)

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
