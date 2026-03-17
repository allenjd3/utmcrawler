import pytest
from aioresponses import aioresponses

from main import has_utm, normalize, crawl


# --- unit tests ---

def test_has_utm_detects_utm_source():
    assert has_utm("https://example.com?utm_source=newsletter") is True

def test_has_utm_detects_multiple_utms():
    assert has_utm("https://example.com?utm_source=x&utm_campaign=y") is True

def test_has_utm_false_for_clean_url():
    assert has_utm("https://example.com/about") is False

def test_has_utm_false_for_unrelated_params():
    assert has_utm("https://example.com?ref=homepage&id=42") is False

def test_normalize_strips_fragment():
    assert normalize("https://example.com/page#section") == "https://example.com/page"

def test_normalize_preserves_query():
    assert normalize("https://example.com/page?foo=bar") == "https://example.com/page?foo=bar"


# --- crawl tests ---

HOME = "https://example.com/"
ABOUT = "https://example.com/about"

HOME_HTML = """
<html><body>
  <a href="/about">About</a>
  <a href="/pricing?utm_source=home&utm_medium=nav">Pricing</a>
  <a href="https://other.com/page">External</a>
</body></html>
"""

ABOUT_HTML = """
<html><body>
  <a href="/?utm_source=about&utm_campaign=return">Home</a>
  <a href="/about#team">Team section</a>
</body></html>
"""


@pytest.mark.asyncio
async def test_crawl_finds_utm_links_on_page():
    with aioresponses() as m:
        m.get(HOME, status=200, body=HOME_HTML, content_type="text/html")
        m.get(ABOUT, status=200, body=ABOUT_HTML, content_type="text/html")

        report = await crawl(HOME)

    assert "https://example.com/pricing?utm_source=home&utm_medium=nav" in report[HOME]


@pytest.mark.asyncio
async def test_crawl_finds_utm_links_on_followed_pages():
    with aioresponses() as m:
        m.get(HOME, status=200, body=HOME_HTML, content_type="text/html")
        m.get(ABOUT, status=200, body=ABOUT_HTML, content_type="text/html")

        report = await crawl(HOME)

    assert "https://example.com/?utm_source=about&utm_campaign=return" in report[ABOUT]


@pytest.mark.asyncio
async def test_crawl_does_not_follow_external_links():
    with aioresponses() as m:
        m.get(HOME, status=200, body=HOME_HTML, content_type="text/html")
        m.get(ABOUT, status=200, body=ABOUT_HTML, content_type="text/html")
        # other.com is never registered — aioresponses raises if it gets fetched

        report = await crawl(HOME)

    assert not any("other.com" in k for k in report)


@pytest.mark.asyncio
async def test_crawl_deduplicates_fragment_variants():
    html = """
    <html><body>
      <a href="/about#team">Team</a>
      <a href="/about#history">History</a>
    </body></html>
    """
    with aioresponses() as m:
        m.get(HOME, status=200, body=html, content_type="text/html")
        # register /about only once — raises ConnectionError on second fetch
        m.get(ABOUT, status=200, body="<html><body></body></html>", content_type="text/html")

        report = await crawl(HOME)

    assert report == {}


@pytest.mark.asyncio
async def test_crawl_empty_when_no_utms():
    html = '<html><body><a href="/about">About</a></body></html>'
    with aioresponses() as m:
        m.get(HOME, status=200, body=html, content_type="text/html")
        m.get(ABOUT, status=200, body="<html><body></body></html>", content_type="text/html")

        report = await crawl(HOME)

    assert report == {}
