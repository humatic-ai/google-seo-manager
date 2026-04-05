#!/usr/bin/env python3
"""
Google SEO Manager

CLI tool that automates Google Search Console workflows:
fetch sitemaps, inspect URLs, detect indexing errors,
and submit fixed pages for re-crawling — with parallel execution.

Works with any site property verified in Google Search Console.
https://github.com/humatic-ai/google-seo-manager
"""

import json
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import threading
import time
import xml.etree.ElementTree as ET
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Optional
from urllib.parse import quote, urljoin, urldefrag, urlparse

import click
import requests
from rich.console import Console
from rich.panel import Panel
from rich.progress import BarColumn, MofNCompleteColumn, Progress, TextColumn
from rich.table import Table

console = Console()

SITEMAP_NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
DEFAULT_COOLDOWN_HOURS = 48
DEFAULT_DELAY = 1.5

OAUTH_SCOPES = [
    "https://www.googleapis.com/auth/webmasters",
    "https://www.googleapis.com/auth/indexing",
]

TOKEN_FILE = "token.json"

SETUP_INSTRUCTIONS = """
[bold yellow]No credentials found.[/bold yellow]

[bold]Quick setup (recommended):[/bold]
  Run: [bold]python seo_manager.py setup[/bold]
  This enables APIs and authenticates via gcloud in one step.
  It prints a URL — click it, sign in, and you're done.

[bold]Alternative: Manual OAuth[/bold]
  1. Create OAuth client ID (Desktop app) at Google Cloud Console
  2. Save as client_secret.json here: {dir}
  3. Run: [bold]python seo_manager.py auth[/bold]
"""

# Maps GSC API enum values to human-readable explanations with fix instructions.
# Source: https://developers.google.com/webmaster-tools/v1/urlInspection.index/UrlInspectionResult
ERROR_EXPLANATIONS: dict[str, str] = {
    # PageFetchState
    "SOFT_404": (
        "Google sees this as a soft 404 — the page returns HTTP 200 but has "
        "thin or empty content. Fix: add meaningful content, or return a real 404."
    ),
    "BLOCKED_ROBOTS_TXT": (
        "robots.txt is blocking Google from crawling this page. "
        "Fix: update your robots.txt to allow the Googlebot user-agent."
    ),
    "NOT_FOUND": (
        "Page returns 404 Not Found. "
        "Fix: restore the page, set up a redirect, or remove the URL from your sitemap."
    ),
    "ACCESS_DENIED": (
        "Page returns 401 Unauthorized. "
        "Fix: remove authentication requirements for public pages."
    ),
    "SERVER_ERROR": (
        "Page returns a 5xx server error when Google tries to fetch it. "
        "Fix: check server logs and ensure the page loads reliably."
    ),
    "REDIRECT_ERROR": (
        "Google encountered a redirect loop or broken redirect chain. "
        "Fix: ensure clean single-hop redirects (no loops, no long chains)."
    ),
    "ACCESS_FORBIDDEN": (
        "Page returns 403 Forbidden. "
        "Fix: check server/firewall permissions — Googlebot may be blocked."
    ),
    "BLOCKED_4XX": (
        "Page returns a 4xx client error (not 401, 403, or 404). "
        "Fix: check the exact HTTP status code and resolve the issue."
    ),
    "INTERNAL_CRAWL_ERROR": (
        "Google experienced an internal error while crawling. "
        "This is usually temporary — re-request indexing after a few days."
    ),
    "INVALID_URL": (
        "Google considers this URL invalid. "
        "Fix: check for malformed characters or encoding issues in the URL."
    ),
    # IndexingState
    "BLOCKED_BY_META_TAG": (
        "A <meta name='robots' content='noindex'> tag is preventing indexing. "
        "Fix: remove the noindex meta tag from the page's <head>."
    ),
    "BLOCKED_BY_HTTP_HEADER": (
        "An X-Robots-Tag: noindex HTTP header is preventing indexing. "
        "Fix: remove the noindex directive from your server/CDN response headers."
    ),
    # RobotsTxtState
    "DISALLOWED": (
        "robots.txt disallows crawling of this URL. "
        "Fix: update the Disallow rules in robots.txt to permit Googlebot access."
    ),
    # Verdict
    "FAIL": (
        "Google flagged this URL with an error — it cannot appear in search results."
    ),
    "NEUTRAL": (
        "Google excluded this URL from the index (not necessarily an error — "
        "could be duplicate, canonical mismatch, or crawl anomaly)."
    ),
}

ACTION_COLORS = {
    "OK": "green",
    "SUBMIT": "bold cyan",
    "SKIP_RECENT": "yellow",
    "WARN_NOINDEX": "yellow",
    "WARN_BLOCKED": "yellow",
    "ERROR": "red",
}

# Coverage-state strings that indicate errors blocking indexing
_ERROR_COVERAGE_KEYWORDS = [
    "noindex", "blocked by robots", "soft 404", "not found",
    "server error", "redirect error", "unauthorized", "forbidden", "4xx",
]

# Maps coverage-state category → human-readable fix instruction
_COVERAGE_FIX_INSTRUCTIONS: dict[str, str] = {
    "Excluded by 'noindex' tag": (
        "Remove the <meta name='robots' content='noindex'> tag (or X-Robots-Tag header)."
    ),
    "Blocked by robots.txt": (
        "Update robots.txt to allow Googlebot for these paths."
    ),
    "Soft 404": (
        "Add meaningful content to the page, or return a real 404 status code."
    ),
    "Not found (404)": (
        "Restore the page, create a 301 redirect, or remove the URL from sitemaps."
    ),
    "Server error (5xx)": (
        "Check server logs — the page must load reliably for Googlebot."
    ),
    "Redirect error": (
        "Fix redirect loops/chains — use clean single-hop 301 redirects."
    ),
    "Blocked due to unauthorized request (401)": (
        "Remove authentication requirements for public pages."
    ),
    "Blocked due to access forbidden (403)": (
        "Check server/firewall rules — Googlebot may be blocked."
    ),
    "Crawled - currently not indexed": (
        "Page was crawled but Google chose not to index it. "
        "Improve content quality/uniqueness and submit for re-crawl."
    ),
    "Discovered - currently not indexed": (
        "Google knows this URL but hasn't crawled it yet. Submit for indexing."
    ),
    "URL is unknown to Google": (
        "Google hasn't seen this URL. Submit for indexing."
    ),
}


def _is_error_coverage(state: str) -> bool:
    """True if the coverage state represents an error blocking indexing."""
    low = state.lower()
    return any(kw in low for kw in _ERROR_COVERAGE_KEYWORDS)


def _is_indexed_coverage(state: str) -> bool:
    """True if the coverage state means the page IS indexed."""
    up = state.upper()
    if "NOT INDEXED" in up or "UNKNOWN" in up:
        return False
    return ("SUBMITTED AND INDEXED" in up or "INDEXED" in up)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class UrlStatus:
    url: str
    http_status: Optional[int] = None
    response_time_ms: Optional[float] = None
    redirect_target: Optional[str] = None
    has_noindex: bool = False
    has_x_robots_noindex: bool = False
    content_length: Optional[int] = None
    http_error: Optional[str] = None
    # GSC inspection fields
    coverage_state: Optional[str] = None
    indexing_state: Optional[str] = None
    page_fetch_state: Optional[str] = None
    robots_txt_state: Optional[str] = None
    last_crawl_time: Optional[str] = None
    crawled_as: Optional[str] = None
    inspection_error: Optional[str] = None
    verdict: Optional[str] = None
    gsc_link: Optional[str] = None
    rich_results_verdict: Optional[str] = None
    rich_results_issues: list[str] = field(default_factory=list)
    referring_urls: list[str] = field(default_factory=list)
    in_sitemaps: list[str] = field(default_factory=list)
    # Submission tracking
    last_submitted_at: Optional[str] = None
    submit_count: int = 0
    # Decision
    action: str = ""
    action_reason: str = ""


# ---------------------------------------------------------------------------
# SQLite Submission Tracker
# ---------------------------------------------------------------------------

class SubmissionTracker:
    def __init__(self, db_path: str = "seo_submissions.db"):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self._init_db()

    def _init_db(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS submissions (
                url TEXT PRIMARY KEY,
                last_submitted_at TEXT,
                last_inspected_at TEXT,
                coverage_state TEXT,
                indexing_state TEXT,
                page_fetch_state TEXT,
                robots_txt_state TEXT,
                last_crawl_time TEXT,
                http_status INTEGER,
                submit_count INTEGER DEFAULT 0,
                last_result TEXT,
                action TEXT,
                action_reason TEXT
            )
        """)
        self.conn.commit()

    def get(self, url: str) -> Optional[dict]:
        row = self.conn.execute(
            "SELECT * FROM submissions WHERE url = ?", (url,)
        ).fetchone()
        return dict(row) if row else None

    def upsert(self, status: UrlStatus):
        self.conn.execute("""
            INSERT INTO submissions
                (url, last_inspected_at, coverage_state, indexing_state,
                 page_fetch_state, robots_txt_state, last_crawl_time,
                 http_status, last_result, action, action_reason,
                 last_submitted_at, submit_count)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(url) DO UPDATE SET
                last_inspected_at = excluded.last_inspected_at,
                coverage_state = excluded.coverage_state,
                indexing_state = excluded.indexing_state,
                page_fetch_state = excluded.page_fetch_state,
                robots_txt_state = excluded.robots_txt_state,
                last_crawl_time = excluded.last_crawl_time,
                http_status = excluded.http_status,
                last_result = excluded.last_result,
                action = excluded.action,
                action_reason = excluded.action_reason,
                last_submitted_at = COALESCE(excluded.last_submitted_at, submissions.last_submitted_at),
                submit_count = CASE
                    WHEN excluded.last_submitted_at IS NOT NULL
                    THEN COALESCE(submissions.submit_count, 0) + 1
                    ELSE COALESCE(submissions.submit_count, 0)
                END
        """, (
            status.url,
            datetime.now(timezone.utc).isoformat(),
            status.coverage_state,
            status.indexing_state,
            status.page_fetch_state,
            status.robots_txt_state,
            status.last_crawl_time,
            status.http_status,
            status.action,
            status.action,
            status.action_reason,
            status.last_submitted_at,
            status.submit_count,
        ))
        self.conn.commit()

    def record_submission(self, url: str):
        now = datetime.now(timezone.utc).isoformat()
        self.conn.execute("""
            UPDATE submissions
            SET last_submitted_at = ?, submit_count = COALESCE(submit_count, 0) + 1
            WHERE url = ?
        """, (now, url))
        self.conn.commit()

    def was_recently_submitted(self, url: str, cooldown_hours: int) -> bool:
        row = self.get(url)
        if not row or not row["last_submitted_at"]:
            return False
        submitted = datetime.fromisoformat(row["last_submitted_at"])
        if submitted.tzinfo is None:
            submitted = submitted.replace(tzinfo=timezone.utc)
        cutoff = datetime.now(timezone.utc) - timedelta(hours=cooldown_hours)
        return submitted > cutoff

    def get_all(self) -> list[dict]:
        rows = self.conn.execute(
            "SELECT * FROM submissions ORDER BY last_inspected_at DESC"
        ).fetchall()
        return [dict(r) for r in rows]

    def close(self):
        self.conn.close()


# ---------------------------------------------------------------------------
# Sitemap Fetcher
# ---------------------------------------------------------------------------

def fetch_sitemap(sitemap_url: str, local_fallback: Optional[str] = None,
                  user_agent: str = "Google-SEO-Manager/1.0") -> list[str]:
    """Fetch and parse sitemap XML, returning list of URLs."""
    xml_content = None

    try:
        console.print(f"  Fetching sitemap: {sitemap_url}")
        resp = requests.get(sitemap_url, headers={"User-Agent": user_agent}, timeout=30)
        resp.raise_for_status()
        xml_content = resp.text
    except Exception as e:
        console.print(f"  [yellow]Remote fetch failed: {e}[/yellow]")
        if local_fallback and os.path.exists(local_fallback):
            console.print(f"  Using local fallback: {local_fallback}")
            xml_content = Path(local_fallback).read_text()
        else:
            console.print("[red]No sitemap available.[/red]")
            return []

    root = ET.fromstring(xml_content)

    # Handle sitemap index (contains <sitemap> elements pointing to sub-sitemaps)
    sitemapindex_tag = f"{{{SITEMAP_NS['sm']}}}sitemapindex"
    if root.tag == sitemapindex_tag or root.tag == "sitemapindex":
        sub_locs = root.findall("sm:sitemap/sm:loc", SITEMAP_NS)
        if not sub_locs:
            sub_locs = root.findall(
                "{http://www.sitemaps.org/schemas/sitemap/0.9}sitemap/"
                "{http://www.sitemaps.org/schemas/sitemap/0.9}loc"
            )
        urls = []
        for loc_el in sub_locs:
            sub_urls = fetch_sitemap(loc_el.text.strip(), user_agent=user_agent)
            urls.extend(sub_urls)
        return urls

    urls = []
    for url_el in root.findall("sm:url/sm:loc", SITEMAP_NS):
        urls.append(url_el.text.strip())
    if not urls:
        for url_el in root.findall("{http://www.sitemaps.org/schemas/sitemap/0.9}url/{http://www.sitemaps.org/schemas/sitemap/0.9}loc"):
            urls.append(url_el.text.strip())

    return urls


# ---------------------------------------------------------------------------
# HTTP Health Checker
# ---------------------------------------------------------------------------

def check_http_health(url: str, user_agent: str = "Google-SEO-Manager/1.0") -> UrlStatus:
    """Check a URL for HTTP status, redirects, noindex, and content validity."""
    status = UrlStatus(url=url)

    try:
        start = time.monotonic()
        resp = requests.get(
            url,
            headers={"User-Agent": user_agent},
            timeout=30,
            allow_redirects=True,
        )
        elapsed = (time.monotonic() - start) * 1000
        status.http_status = resp.status_code
        status.response_time_ms = round(elapsed, 1)
        status.content_length = len(resp.content)

        if resp.history:
            status.redirect_target = resp.url

        x_robots = resp.headers.get("X-Robots-Tag", "")
        if "noindex" in x_robots.lower():
            status.has_x_robots_noindex = True

        if resp.status_code == 200 and "text/html" in resp.headers.get("Content-Type", ""):
            body_lower = resp.text[:8192].lower()
            if re.search(r'<meta\s[^>]*name=["\']robots["\'][^>]*content=["\'][^"\']*noindex', body_lower):
                status.has_noindex = True

    except requests.RequestException as e:
        status.http_error = str(e)

    return status


# ---------------------------------------------------------------------------
# Site Crawler (discovers non-sitemap internal URLs)
# ---------------------------------------------------------------------------

class _LinkExtractor(HTMLParser):
    """Extract href values from <a> tags."""
    def __init__(self):
        super().__init__()
        self.links: list[str] = []

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            for name, value in attrs:
                if name == "href" and value:
                    self.links.append(value)

    def error(self, message):
        pass


def crawl_site(start_url: str, max_pages: int = 200, max_depth: int = 3,
               user_agent: str = "Google-SEO-Manager/1.0") -> list[str]:
    """BFS crawl of a site, following internal <a href> links."""
    parsed_base = urlparse(start_url)
    base_domain = parsed_base.netloc
    visited: set[str] = set()
    queue: list[tuple[str, int]] = [(start_url.rstrip("/"), 0)]
    found: list[str] = []

    while queue and len(visited) < max_pages:
        url, depth = queue.pop(0)
        clean = urldefrag(url)[0].split("?")[0].rstrip("/") or url
        if clean in visited:
            continue
        parsed = urlparse(clean)
        if parsed.netloc != base_domain or parsed.scheme not in ("http", "https"):
            continue

        visited.add(clean)
        found.append(clean)
        short = clean.replace(start_url.rstrip("/"), "") or "/"
        console.print(f"  [{len(found)}/{max_pages}] {short}")

        if depth >= max_depth:
            continue

        try:
            resp = requests.get(clean, headers={"User-Agent": user_agent}, timeout=15)
            if resp.status_code != 200:
                continue
            if "text/html" not in resp.headers.get("Content-Type", ""):
                continue
            extractor = _LinkExtractor()
            extractor.feed(resp.text)
            for href in extractor.links:
                abs_url = urldefrag(urljoin(clean, href))[0].split("?")[0].rstrip("/")
                abs_parsed = urlparse(abs_url)
                if (abs_parsed.netloc == base_domain
                        and abs_parsed.scheme in ("http", "https")
                        and abs_url not in visited
                        and not abs_parsed.path.split(".")[-1] in (
                            "png", "jpg", "jpeg", "gif", "svg", "webp",
                            "css", "js", "ico", "woff", "woff2", "ttf", "pdf",
                        )):
                    queue.append((abs_url, depth + 1))
        except Exception:
            continue

    return found


# ---------------------------------------------------------------------------
# Authentication (OAuth + Service Account)
# ---------------------------------------------------------------------------

def _resolve_credentials(credentials_file: str, token_file: str = TOKEN_FILE):
    """
    Resolve Google credentials in order of preference:
      1. Saved OAuth token (token.json)
      2. gcloud Application Default Credentials (~/.config/gcloud/...)
      3. Service account JSON (credentials.json)
    Returns a google.auth.credentials.Credentials object.
    """
    from google.oauth2 import service_account as sa_mod
    from google.oauth2.credentials import Credentials
    from google.auth.transport.requests import Request

    # 1. Try saved OAuth token
    if os.path.exists(token_file):
        creds = Credentials.from_authorized_user_file(token_file, OAUTH_SCOPES)
        if creds.valid:
            return creds
        if creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
                _save_token(creds, token_file)
                return creds
            except Exception as e:
                console.print(f"[yellow]Token refresh failed ({e}), will try other auth methods.[/yellow]")

    # 2. Try gcloud Application Default Credentials
    adc_path = os.path.expanduser("~/.config/gcloud/application_default_credentials.json")
    if os.path.exists(adc_path):
        try:
            import google.auth
            creds, _ = google.auth.default(scopes=OAUTH_SCOPES)
            if creds.valid or hasattr(creds, "refresh_token"):
                if not creds.valid:
                    creds.refresh(Request())
                return creds
        except Exception as e:
            console.print(f"[yellow]ADC auth failed ({e}), will try other methods.[/yellow]")

    # 3. Try service account file
    if os.path.exists(credentials_file):
        with open(credentials_file) as f:
            data = json.load(f)
        if data.get("type") == "service_account":
            return sa_mod.Credentials.from_service_account_file(
                credentials_file, scopes=OAUTH_SCOPES
            )
        if "installed" in data or "web" in data:
            console.print(
                "[yellow]Found OAuth client secret file but no saved token.[/yellow]\n"
                "  Run [bold]python seo_manager.py auth[/bold] to sign in first."
            )
            raise SystemExit(1)

    return None


def _save_token(creds, token_file: str = TOKEN_FILE):
    """Persist OAuth credentials to disk for reuse."""
    with open(token_file, "w") as f:
        f.write(creds.to_json())


def run_oauth_flow(client_secret_file: str, token_file: str = TOKEN_FILE,
                   port: int = 0, headless: bool = False):
    """
    Run the interactive OAuth consent flow.
    Opens a browser for user sign-in, saves the resulting token.
    If headless=True, prints a URL for the user to visit manually.
    """
    from google_auth_oauthlib.flow import InstalledAppFlow

    flow = InstalledAppFlow.from_client_secrets_file(
        client_secret_file, scopes=OAUTH_SCOPES
    )

    if headless:
        creds = flow.run_console()
    else:
        creds = flow.run_local_server(port=port, open_browser=True)

    _save_token(creds, token_file)
    return creds


def _detect_quota_project() -> Optional[str]:
    """Detect the GCP quota project from gcloud config."""
    gcloud = shutil.which("gcloud")
    if gcloud:
        r = subprocess.run([gcloud, "config", "get-value", "project"],
                           capture_output=True, text=True, check=False)
        proj = r.stdout.strip()
        if proj:
            return proj
    return None


def build_gsc_service(credentials_file: str, token_file: str = TOKEN_FILE):
    """Build the Search Console API service."""
    from googleapiclient.discovery import build

    creds = _resolve_credentials(credentials_file, token_file)
    if creds is None:
        raise RuntimeError("No valid credentials found. Run 'setup' first.")
    quota_project = _detect_quota_project()
    if quota_project and hasattr(creds, "with_quota_project"):
        creds = creds.with_quota_project(quota_project)
    return build("searchconsole", "v1", credentials=creds, cache_discovery=False)


def build_indexing_service(credentials_file: str, token_file: str = TOKEN_FILE):
    """Build the Indexing API service."""
    from googleapiclient.discovery import build

    creds = _resolve_credentials(credentials_file, token_file)
    if creds is None:
        raise RuntimeError("No valid credentials found. Run 'setup' first.")
    quota_project = _detect_quota_project()
    if quota_project and hasattr(creds, "with_quota_project"):
        creds = creds.with_quota_project(quota_project)
    return build("indexing", "v3", credentials=creds, cache_discovery=False)


def build_webmasters_service(credentials_file: str, token_file: str = TOKEN_FILE):
    """Build the legacy Webmasters API v3 service (for sites.list)."""
    from googleapiclient.discovery import build

    creds = _resolve_credentials(credentials_file, token_file)
    if creds is None:
        raise RuntimeError("No valid credentials found. Run 'setup' first.")
    quota_project = _detect_quota_project()
    if quota_project and hasattr(creds, "with_quota_project"):
        creds = creds.with_quota_project(quota_project)
    return build("webmasters", "v3", credentials=creds, cache_discovery=False)


def derive_sitemap_url(site_url: str) -> str:
    """Derive a default sitemap URL from a GSC site property string."""
    if site_url.startswith("sc-domain:"):
        domain = site_url.split(":", 1)[1]
        return f"https://{domain}/sitemap.xml"
    return site_url.rstrip("/") + "/sitemap.xml"


def derive_base_url(site_url: str) -> str:
    """Extract the base URL prefix used to shorten displayed URLs."""
    if site_url.startswith("sc-domain:"):
        domain = site_url.split(":", 1)[1]
        return f"https://{domain}"
    return site_url.rstrip("/")


def inspect_url(gsc_service, url: str, site_url: str) -> dict:
    """Inspect a single URL via GSC URL Inspection API."""
    body = {
        "inspectionUrl": url,
        "siteUrl": site_url,
    }
    response = gsc_service.urlInspection().index().inspect(body=body).execute()
    return response.get("inspectionResult", {})


def parse_inspection_result(status: UrlStatus, result: dict):
    """Extract fields from a GSC inspection API response into UrlStatus."""
    status.gsc_link = result.get("inspectionResultLink")

    idx = result.get("indexStatusResult", {})
    status.verdict = idx.get("verdict")
    status.coverage_state = idx.get("coverageState")
    status.indexing_state = idx.get("indexingState")
    status.page_fetch_state = idx.get("pageFetchState")
    status.robots_txt_state = idx.get("robotsTxtState")
    status.last_crawl_time = idx.get("lastCrawlTime")
    status.crawled_as = idx.get("crawledAs")
    status.referring_urls = idx.get("referringUrls", [])
    status.in_sitemaps = idx.get("sitemap", [])

    rr = result.get("richResultsResult", {})
    if rr:
        status.rich_results_verdict = rr.get("verdict")
        for group in rr.get("detectedItems", []):
            for item in group.get("items", []):
                for issue in item.get("issues", []):
                    msg = issue.get("issueMessage", "")
                    sev = issue.get("severity", "")
                    if msg:
                        status.rich_results_issues.append(f"[{sev}] {msg}")


# ---------------------------------------------------------------------------
# Indexing Submitters
# ---------------------------------------------------------------------------

def submit_to_google_indexing(indexing_service, url: str) -> dict:
    """Submit a URL to Google Indexing API."""
    body = {"url": url, "type": "URL_UPDATED"}
    return indexing_service.urlNotifications().publish(body=body).execute()


def submit_to_indexnow(url: str, key: str, host: Optional[str] = None) -> bool:
    """Submit a URL to IndexNow (Bing, Yandex, etc.)."""
    if host is None:
        parsed = urlparse(url)
        host = parsed.netloc
    payload = {
        "host": host,
        "key": key,
        "urlList": [url],
    }
    try:
        resp = requests.post(
            "https://api.indexnow.org/indexnow",
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=15,
        )
        return resp.status_code in (200, 202)
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Decision Engine
# ---------------------------------------------------------------------------

def _is_live_healthy(status: UrlStatus) -> bool:
    """True when our own live HTTP check shows the page is OK."""
    return (
        status.http_status == 200
        and not status.http_error
        and not status.has_noindex
        and not status.has_x_robots_noindex
    )


# GSC page_fetch_state values that indicate a real error
_GSC_FETCH_ERRORS = {
    "SOFT_404", "NOT_FOUND", "SERVER_ERROR", "ACCESS_DENIED",
    "ACCESS_FORBIDDEN", "REDIRECT_ERROR", "BLOCKED_4XX",
    "INTERNAL_CRAWL_ERROR", "INVALID_URL", "BLOCKED_ROBOTS_TXT",
}


# ---------------------------------------------------------------------------
# Parallel URL Inspection
# ---------------------------------------------------------------------------

def _inspect_urls_parallel(
    urls: list[str],
    site_url: str,
    credentials_file: str,
    user_agent: str,
    workers: int = 5,
    token_file: str = TOKEN_FILE,
) -> list[UrlStatus]:
    """Inspect URLs in parallel (HTTP health + GSC API). Returns results in order."""
    from googleapiclient.discovery import build as _build

    creds = _resolve_credentials(credentials_file, token_file)
    if creds is None:
        raise RuntimeError("No valid credentials found. Run 'setup' first.")
    quota_project = _detect_quota_project()
    if quota_project and hasattr(creds, "with_quota_project"):
        creds = creds.with_quota_project(quota_project)

    tls = threading.local()

    def _get_gsc():
        if not hasattr(tls, "gsc"):
            tls.gsc = _build("searchconsole", "v1", credentials=creds,
                             cache_discovery=False)
        return tls.gsc

    def _worker(url: str) -> UrlStatus:
        status = check_http_health(url, user_agent)
        if not status.http_error:
            gsc = _get_gsc()
            try:
                result = inspect_url(gsc, url, site_url)
                parse_inspection_result(status, result)
            except Exception as e:
                status.inspection_error = str(e)
        return status

    result_map: dict[str, UrlStatus] = {}

    with Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TextColumn("[dim]{task.fields[current_url]}[/dim]"),
        console=console,
    ) as progress:
        task = progress.add_task(
            f"Inspecting ({workers} workers)",
            total=len(urls),
            current_url="",
        )
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(_worker, url): url for url in urls}
            for future in as_completed(futures):
                url = futures[future]
                try:
                    status = future.result()
                except Exception as exc:
                    status = UrlStatus(url=url, http_error=str(exc))
                result_map[url] = status
                short = url.rsplit("/", 1)[-1] or "/"
                progress.update(task, advance=1, current_url=short[-40:])

    return [result_map[url] for url in urls]


def decide_action(status: UrlStatus, tracker: SubmissionTracker,
                  cooldown_hours: int) -> tuple[str, str]:
    """
    Determine the action for a URL based on health check + inspection + history.
    Returns (action, reason) tuple.
    """
    # --- Live HTTP errors (our own check) ---
    if status.http_error:
        return "ERROR", f"HTTP request failed: {status.http_error}"

    if status.http_status and status.http_status >= 400:
        return "ERROR", f"HTTP {status.http_status}"

    if status.has_noindex:
        return "WARN_NOINDEX", "Page has <meta name='robots' content='noindex'>"

    if status.has_x_robots_noindex:
        return "WARN_NOINDEX", "X-Robots-Tag contains noindex"

    # --- GSC-reported blocking (check live health before trusting) ---
    if status.robots_txt_state and "DISALLOWED" in str(status.robots_txt_state).upper():
        return "WARN_BLOCKED", "Blocked by robots.txt per GSC"

    if status.indexing_state and "INDEXING_NOT_ALLOWED" in str(status.indexing_state).upper():
        if _is_live_healthy(status):
            return "SUBMIT", (
                f"GSC reported {status.indexing_state} but page is now healthy — "
                "submit to re-crawl"
            )
        return "WARN_NOINDEX", f"GSC says indexing not allowed: {status.indexing_state}"

    coverage = (status.coverage_state or "").upper()

    # "NOT INDEXED" must be excluded before checking for "INDEXED"
    not_indexed = "NOT INDEXED" in coverage or "UNKNOWN" in coverage
    is_indexed = not not_indexed and (
        "SUBMITTED AND INDEXED" in coverage
        or "INDEXED, NOT SUBMITTED" in coverage
        or "INDEXED_NOT_SUBMITTED" in coverage
        or "SUBMITTED_AND_INDEXED" in coverage
        or coverage == "INDEXED"
    )

    if is_indexed:
        return "OK", f"Indexed (coverage: {status.coverage_state})"

    if tracker.was_recently_submitted(status.url, cooldown_hours):
        rec = tracker.get(status.url)
        sub_at = rec["last_submitted_at"] if rec else "?"
        return "SKIP_RECENT", f"Submitted at {sub_at}, awaiting Google crawl"

    # --- GSC fetch errors: trust only if our live check also fails ---
    pfs = status.page_fetch_state or ""
    if pfs in _GSC_FETCH_ERRORS:
        if _is_live_healthy(status):
            return "SUBMIT", (
                f"GSC reported {pfs} but page is now HTTP 200 — "
                "error appears fixed, submit to re-crawl"
            )
        return "ERROR", f"Google cannot fetch page: {pfs}"

    # --- GSC verdict FAIL/NEUTRAL: check live health ---
    if status.verdict in ("FAIL", "NEUTRAL") and _is_live_healthy(status):
        return "SUBMIT", (
            f"GSC verdict {status.verdict} (coverage: {status.coverage_state}) "
            "but page is now healthy — submit to re-crawl"
        )

    return "SUBMIT", f"Not indexed (coverage: {status.coverage_state or 'unknown'}), healthy, eligible"


# ---------------------------------------------------------------------------
# Shared Report
# ---------------------------------------------------------------------------

def _get_error_explanation(status: UrlStatus) -> Optional[str]:
    """Look up a human-readable explanation for the first matching GSC error."""
    for key in (status.page_fetch_state, status.indexing_state,
                status.robots_txt_state, status.verdict):
        if key and key in ERROR_EXPLANATIONS:
            return ERROR_EXPLANATIONS[key]
    return None


def _print_report(results: list[UrlStatus], site_url: str,
                  submitted_count: int = 0, did_submit: bool = False):
    """
    Structured Rich report used by both `check` and `run`.

    Sections:
      A. Errors to Fix  (red)
      B. Ready to Submit / Submitted  (cyan)
      C. Already Indexed  (green)
      D. Summary counters
    """
    base_url = derive_base_url(site_url)

    errors = [s for s in results if s.action in ("ERROR", "WARN_NOINDEX", "WARN_BLOCKED")]
    submittable = [s for s in results if s.action == "SUBMIT"]
    ok_list = [s for s in results if s.action == "OK"]
    skipped = [s for s in results if s.action == "SKIP_RECENT"]

    # --- Section A: Errors to Fix ---
    if errors:
        console.print()
        console.print(Panel(
            f"[bold red]Errors to Fix ({len(errors)} URLs)[/bold red]",
            border_style="red",
        ))
        for s in errors:
            short = s.url.replace(base_url, "") or "/"
            console.print(f"\n  [bold]{short}[/bold]")

            parts = []
            if s.http_status:
                parts.append(f"HTTP {s.http_status}")
            if s.page_fetch_state and s.page_fetch_state != "SUCCESSFUL":
                parts.append(f"Fetch: {s.page_fetch_state}")
            if s.indexing_state and s.indexing_state != "INDEXING_ALLOWED":
                parts.append(f"Indexing: {s.indexing_state}")
            if s.robots_txt_state and s.robots_txt_state != "ALLOWED":
                parts.append(f"Robots: {s.robots_txt_state}")
            if s.coverage_state:
                parts.append(f"Coverage: {s.coverage_state}")
            if parts:
                console.print(f"    [dim]{' | '.join(parts)}[/dim]")

            explanation = _get_error_explanation(s)
            if explanation:
                console.print(f"    [yellow]→ {explanation}[/yellow]")
            elif s.action_reason:
                console.print(f"    [yellow]→ {s.action_reason}[/yellow]")

            if s.rich_results_issues:
                for issue in s.rich_results_issues:
                    console.print(f"    [yellow]→ Rich Results: {issue}[/yellow]")

            if s.gsc_link:
                console.print(f"    [dim]GSC: {s.gsc_link}[/dim]")

    # --- Section B: Ready to Submit / Submitted ---
    if submittable:
        console.print()
        if did_submit and submitted_count > 0:
            console.print(Panel(
                f"[bold cyan]Submitted for Indexing ({submitted_count} URLs)[/bold cyan]",
                border_style="cyan",
            ))
        else:
            console.print(Panel(
                f"[bold cyan]No Issues Found — Ready to Submit "
                f"({len(submittable)} URLs)[/bold cyan]",
                border_style="cyan",
            ))
        for s in submittable:
            short = s.url.replace(base_url, "") or "/"
            cov = s.coverage_state or "unknown"
            console.print(f"    {short}  [dim]({cov})[/dim]")
        if not did_submit:
            console.print(
                f"\n  [bold]These pages have no errors. Submit them now:[/bold]"
            )
            console.print(
                f"  python seo_manager.py check --site '{site_url}' --submit"
            )

    # --- Section C: Already Indexed ---
    if ok_list:
        console.print()
        console.print(Panel(
            f"[bold green]Already Indexed ({len(ok_list)} URLs)[/bold green]",
            border_style="green",
        ))

    # --- Section D: Skipped ---
    if skipped:
        console.print()
        console.print(
            f"  [yellow]Skipped (recently submitted, awaiting crawl): {len(skipped)}[/yellow]"
        )

    # --- All clean ---
    if not errors and not submittable and ok_list:
        console.print()
        console.print(
            "  [bold green]All pages are indexed and healthy — nothing to do![/bold green]"
        )

    # --- Summary counters ---
    console.print()
    console.print(f"  Total URLs:       [bold]{len(results)}[/bold]")
    console.print(f"  Indexed (OK):     [green]{len(ok_list)}[/green]")
    if did_submit:
        console.print(f"  Submitted:        [cyan]{submitted_count}[/cyan]")
    elif submittable:
        console.print(f"  Ready to submit:  [cyan]{len(submittable)}[/cyan]")
    console.print(f"  Skipped (recent): [yellow]{len(skipped)}[/yellow]")
    console.print(f"  Errors/warnings:  [red]{len(errors)}[/red]")
    console.print()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def load_config(config_path: Optional[str]) -> dict:
    if config_path and os.path.exists(config_path):
        with open(config_path) as f:
            return json.load(f)
    return {}


@click.group()
@click.option("--config", "config_path", default=None,
              help="Path to config JSON file")
@click.pass_context
def cli(ctx, config_path):
    """Google SEO Manager — works with any Google Search Console property."""
    ctx.ensure_object(dict)
    ctx.obj["config"] = load_config(config_path)


def _run_cmd(cmd: list[str], check: bool = True, capture: bool = True) -> subprocess.CompletedProcess:
    """Run a shell command, returning the result."""
    return subprocess.run(cmd, capture_output=capture, text=True, check=check)


def _enable_apis_via_gcloud(project: str):
    """Enable required GCP APIs using gcloud CLI."""
    gcloud = shutil.which("gcloud")
    if not gcloud:
        return False

    apis = ["searchconsole.googleapis.com", "indexing.googleapis.com"]
    for api in apis:
        console.print(f"  Enabling {api} ...")
        r = _run_cmd([gcloud, "services", "enable", api, f"--project={project}"], check=False)
        if r.returncode != 0:
            console.print(f"    [yellow]Failed: {r.stderr.strip()}[/yellow]")
            return False
        console.print(f"    [green]OK[/green]")
    return True


def _do_builtin_oauth(token_file: str = TOKEN_FILE):
    """
    Run OAuth using gcloud CLI's public OAuth client ID with localhost redirect.
    After authorizing, the browser redirects to http://localhost/?code=...
    The page will show a connection error, but the auth code is in the URL bar.
    The user copies the FULL redirect URL and pastes it here.
    """
    from google_auth_oauthlib.flow import InstalledAppFlow
    from urllib.parse import urlparse, parse_qs

    client_config = {
        "installed": {
            "client_id": "764086051850-6qr4p6gpi6hn506pt8ejuq83di341hur.apps.googleusercontent.com",
            "client_secret": "d-FL95Q19q7MQmFpd7hHD0Ty",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "redirect_uris": ["http://localhost"],
        }
    }

    flow = InstalledAppFlow.from_client_config(client_config, scopes=OAUTH_SCOPES)
    flow.redirect_uri = "http://localhost"

    auth_url, _ = flow.authorization_url(prompt="consent", access_type="offline")

    # Save auth URL to an HTML file so the user can open it without terminal wrapping issues
    auth_html = os.path.join(os.path.dirname(os.path.abspath(__file__)), "open_this.html")
    with open(auth_html, "w") as f:
        f.write(f'<html><head><meta http-equiv="refresh" content="0;url={auth_url}"></head>'
                f'<body><a href="{auth_url}">Click here if not redirected</a></body></html>')

    console.print(f"\n  [bold]Auth URL saved to:[/bold] {auth_html}")
    console.print(f"  Open that file in your browser, or copy the URL below.\n")
    # Print URL raw (no Rich markup) to avoid wrapping issues
    sys.stdout.write(f"  {auth_url}\n\n")
    sys.stdout.flush()
    console.print("  1. Sign in and click [bold]Allow[/bold]")
    console.print("  2. Browser redirects to a page that [bold]won't load[/bold] -- this is normal!")
    console.print("  3. Copy the [bold]entire URL[/bold] from your browser's address bar")
    console.print('     (it starts with [dim]http://localhost/?code=...[/dim])\n')

    raw = click.prompt("  Paste the full URL from your browser's address bar")
    raw = raw.strip()

    if raw.startswith("http"):
        parsed = urlparse(raw)
        params = parse_qs(parsed.query)
        code = params.get("code", [None])[0]
        if not code:
            console.print("[red]Could not find 'code' parameter in the URL.[/red]")
            raise SystemExit(1)
    else:
        code = raw

    flow.fetch_token(code=code)
    creds = flow.credentials
    _save_token(creds, token_file)
    return creds


@cli.command()
@click.option("--project", default=None, help="GCP project ID (auto-detects from gcloud)")
def setup(project):
    """One-step setup: enable APIs and sign in with Google (no files needed)."""

    # Step 1: Enable APIs via gcloud
    console.print(Panel("[bold]Step 1: Enable APIs[/bold]", border_style="cyan"))

    gcloud = shutil.which("gcloud")
    if not project and gcloud:
        r = _run_cmd([gcloud, "config", "get-value", "project"], check=False)
        project = r.stdout.strip()

    if project and gcloud:
        console.print(f"  Project: [bold]{project}[/bold]")
        _enable_apis_via_gcloud(project)
    else:
        console.print("  [yellow]Skipping (no gcloud or project). Make sure APIs are enabled manually.[/yellow]")

    # Step 2: OAuth
    console.print()
    console.print(Panel("[bold]Step 2: Sign in with Google[/bold]", border_style="cyan"))

    try:
        creds = _do_builtin_oauth(TOKEN_FILE)
        console.print(f"\n[bold green]Setup complete![/bold green]")
        console.print(f"  Token saved to: {os.path.abspath(TOKEN_FILE)}")
        console.print(f"\n  Now run: [bold]python seo_manager.py run[/bold]")
    except Exception as e:
        console.print(f"\n[red]Authentication failed: {e}[/red]")
        raise SystemExit(1)


@cli.command()
@click.option("--credentials", default="credentials.json", help="Path to credentials JSON")
@click.pass_context
def sites(ctx, credentials):
    """List all verified sites in your Google Search Console account."""
    cfg = ctx.obj["config"]
    credentials = credentials if credentials != "credentials.json" else cfg.get("credentials_file", credentials)

    try:
        wm_service = build_webmasters_service(credentials)
        resp = wm_service.sites().list().execute()
    except Exception as e:
        console.print(f"[red]Failed to list sites: {e}[/red]")
        console.print("Run [bold]python seo_manager.py setup[/bold] first.")
        raise SystemExit(1)

    entries = resp.get("siteEntry", [])
    if not entries:
        console.print("[yellow]No sites found. Add a site in Google Search Console first.[/yellow]")
        return

    table = Table(title="Your Search Console Sites", show_header=True,
                  header_style="bold", width=100)
    table.add_column("#", justify="right", width=3)
    table.add_column("Site URL", no_wrap=True, ratio=4)
    table.add_column("Permission", width=14)
    table.add_column("Sitemap (derived)", ratio=3)

    for i, entry in enumerate(entries, 1):
        site = entry.get("siteUrl", "")
        perm = entry.get("permissionLevel", "?")
        perm_color = "green" if "owner" in perm.lower() else "yellow"
        sm = derive_sitemap_url(site)
        table.add_row(str(i), site, f"[{perm_color}]{perm}[/{perm_color}]", sm)

    console.print(table)
    console.print(f"\n  Use with: [bold]python seo_manager.py run --site 'SITE_URL'[/bold]")
    console.print(f"  Example:  [bold]python seo_manager.py run --site '{entries[0].get('siteUrl', '')}'[/bold]")


@cli.command()
@click.option("--site", required=True, help="Site URL for GSC (e.g. sc-domain:example.com)")
@click.option("--sitemap", default=None, help="Sitemap URL (auto-derived from --site if omitted)")
@click.option("--submit", "do_submit", is_flag=True, help="Also submit clean/eligible pages for indexing")
@click.option("--credentials", default="credentials.json", help="Path to credentials JSON")
@click.option("--cooldown", type=int, default=DEFAULT_COOLDOWN_HOURS, help="Hours before re-submitting")
@click.option("--workers", type=int, default=5, help="Parallel workers (default 5)")
@click.option("--db", default="seo_submissions.db", help="Path to SQLite database")
@click.pass_context
def check(ctx, site, sitemap, do_submit, credentials, cooldown, workers, db):
    """Inspect all sitemap URLs and report SEO errors with fix instructions.

    Without --submit: inspect only, never submits (safe to run anytime).
    With --submit: also submits clean/eligible pages for indexing.
    """
    cfg = ctx.obj["config"]
    credentials = credentials if credentials != "credentials.json" else cfg.get("credentials_file", credentials)
    sitemap_url = sitemap or derive_sitemap_url(site)
    user_agent = cfg.get("user_agent", "Google-SEO-Manager/1.0")

    console.print(Panel(
        f"[bold]SEO Check — {site}[/bold]"
        + ("  [cyan](+ submit)[/cyan]" if do_submit else "  [dim](inspect only)[/dim]"),
        border_style="cyan",
    ))

    console.print("[bold]Authenticating with Google APIs...[/bold]")
    try:
        creds_check = _resolve_credentials(credentials)
        if creds_check is None:
            raise RuntimeError("No valid credentials found. Run 'setup' first.")
        indexing_service = build_indexing_service(credentials) if do_submit else None
        console.print("  [green]Authenticated[/green]")
    except Exception as e:
        console.print(f"[red]Auth failed: {e}[/red]")
        raise SystemExit(1)

    tracker = SubmissionTracker(db)

    console.print()
    console.print(Panel("[bold]Fetching sitemap[/bold]", border_style="cyan"))
    urls = fetch_sitemap(sitemap_url, user_agent=user_agent)
    if not urls:
        console.print("[red]No URLs found in sitemap. Exiting.[/red]")
        raise SystemExit(1)
    console.print(f"  Found [bold green]{len(urls)}[/bold green] URLs")

    # --- Parallel inspection ---
    console.print()
    console.print(Panel(
        f"[bold]Inspecting URLs[/bold]  ({workers} parallel workers)",
        border_style="cyan",
    ))

    statuses = _inspect_urls_parallel(
        urls, site, credentials, user_agent, workers=workers,
    )

    # --- Sequential: decisions + submissions ---
    base_url = derive_base_url(site)
    results: list[UrlStatus] = []
    submitted_count = 0

    for status in statuses:
        action, reason = decide_action(status, tracker, cooldown)
        status.action = action
        status.action_reason = reason

        if do_submit and action == "SUBMIT" and indexing_service:
            try:
                submit_to_google_indexing(indexing_service, status.url)
                status.last_submitted_at = datetime.now(timezone.utc).isoformat()
                submitted_count += 1
                tracker.record_submission(status.url)
            except Exception as e:
                short = status.url.replace(base_url, "") or "/"
                console.print(f"  [red]Submit error ({short}): {e}[/red]")

        tracker.upsert(status)
        results.append(status)

    _print_report(results, site, submitted_count=submitted_count, did_submit=do_submit)
    console.print(f"  DB saved to: {db}")
    tracker.close()


@cli.command()
@click.option("--site", required=True, help="Site URL (e.g. sc-domain:example.com)")
@click.option("--sitemap", default=None, help="Sitemap URL (auto-derived from --site)")
@click.option("--crawl", "do_crawl", is_flag=True,
              help="Also crawl the site to discover non-sitemap URLs")
@click.option("--max-pages", type=int, default=200,
              help="Max pages to crawl (only with --crawl)")
@click.option("--submit", "do_submit", is_flag=True,
              help="Submit fixed/eligible URLs for re-indexing")
@click.option("--credentials", default="credentials.json", help="Path to credentials JSON")
@click.option("--cooldown", type=int, default=DEFAULT_COOLDOWN_HOURS,
              help="Hours before re-submitting a URL")
@click.option("--workers", type=int, default=5,
              help="Parallel workers for URL inspection (default 5)")
@click.option("--db", default="seo_submissions.db", help="Path to SQLite database")
@click.pass_context
def report(ctx, site, sitemap, do_crawl, max_pages, do_submit,
           credentials, cooldown, workers, db):
    """Page indexing report — mirrors the GSC 'Why pages aren't indexed' view.

    Inspects all sitemap URLs (and optionally crawled URLs) via the GSC API,
    groups them by the same error categories shown in Google Search Console,
    and shows which errors appear fixed on the live site.

    \b
    Without --submit: inspect and report only (safe to run anytime).
    With --submit:    also submit fixed/eligible URLs for re-indexing.
    With --crawl:     also discover non-sitemap URLs by following links.
    """
    cfg = ctx.obj["config"]
    credentials = (credentials if credentials != "credentials.json"
                   else cfg.get("credentials_file", credentials))
    sitemap_url = sitemap or derive_sitemap_url(site)
    base_url = derive_base_url(site)
    user_agent = cfg.get("user_agent", "Google-SEO-Manager/1.0")

    console.print(Panel(
        f"[bold]Page Indexing Report — {site}[/bold]",
        border_style="cyan",
    ))

    # --- Auth (quick validation) ---
    console.print("[bold]Authenticating...[/bold]")
    try:
        creds_check = _resolve_credentials(credentials)
        if creds_check is None:
            raise RuntimeError("No valid credentials found. Run 'setup' first.")
        idx = build_indexing_service(credentials) if do_submit else None
        console.print("  [green]Authenticated[/green]")
    except Exception as e:
        console.print(f"[red]Auth failed: {e}[/red]")
        raise SystemExit(1)

    tracker = SubmissionTracker(db)

    # --- Collect URLs ---
    console.print()
    console.print(Panel("[bold]Collecting URLs[/bold]", border_style="cyan"))
    sitemap_urls = list(dict.fromkeys(fetch_sitemap(sitemap_url, user_agent=user_agent)))
    console.print(f"  Sitemap: [bold]{len(sitemap_urls)}[/bold] URLs")

    crawled_urls: list[str] = []
    if do_crawl:
        console.print()
        console.print(f"  Crawling {base_url} (max {max_pages} pages, depth 3)...")
        crawled_urls = crawl_site(base_url, max_pages=max_pages, user_agent=user_agent)
        new_from_crawl = [u for u in crawled_urls if u not in set(sitemap_urls)]
        console.print(
            f"  Crawled [bold]{len(crawled_urls)}[/bold] pages, "
            f"[bold cyan]{len(new_from_crawl)}[/bold cyan] not in sitemap"
        )

    all_urls = list(dict.fromkeys(sitemap_urls + crawled_urls))
    console.print(f"\n  Total unique URLs to inspect: [bold]{len(all_urls)}[/bold]")

    # --- Parallel inspection ---
    console.print()
    console.print(Panel(
        f"[bold]Inspecting URLs[/bold]  ({workers} parallel workers)",
        border_style="cyan",
    ))

    statuses = _inspect_urls_parallel(
        all_urls, site, credentials, user_agent, workers=workers,
    )

    results: list[UrlStatus] = []
    for status in statuses:
        action, reason = decide_action(status, tracker, cooldown)
        status.action = action
        status.action_reason = reason
        tracker.upsert(status)
        results.append(status)

    # --- Categorise ---
    indexed = [s for s in results if _is_indexed_coverage(s.coverage_state or "")]
    not_indexed = [s for s in results if not _is_indexed_coverage(s.coverage_state or "")]

    categories: dict[str, list[UrlStatus]] = defaultdict(list)
    for s in not_indexed:
        cat = s.coverage_state or "Unknown / inspection failed"
        categories[cat].append(s)

    sorted_cats = sorted(
        categories.items(),
        key=lambda x: (0 if _is_error_coverage(x[0]) else 1, -len(x[1])),
    )

    # --- Summary table (mirrors GSC screenshot) ---
    console.print()
    console.print(Panel(
        f"[bold green]Indexed pages: {len(indexed)}[/bold green]",
        border_style="green",
    ))

    if sorted_cats:
        console.print()
        console.print(Panel(
            "[bold]Why pages aren't indexed[/bold]",
            subtitle="Pages that aren't indexed can't be served on Google",
            border_style="yellow",
        ))

        table = Table(show_header=True, header_style="bold", width=100, padding=(0, 1))
        table.add_column("Reason", ratio=5, no_wrap=False)
        table.add_column("Pages", justify="right", width=6)
        table.add_column("Live OK", justify="right", width=7)
        table.add_column("Status", width=14)

        for cat, urls in sorted_cats:
            is_err = _is_error_coverage(cat)
            live_ok = sum(1 for s in urls if _is_live_healthy(s))

            if is_err:
                if live_ok == len(urls):
                    status_label = "[green]All fixed[/green]"
                elif live_ok > 0:
                    status_label = f"[yellow]{live_ok} fixed[/yellow]"
                else:
                    status_label = "[red]Fix needed[/red]"
            elif live_ok > 0:
                status_label = "[cyan]Submit[/cyan]"
            else:
                status_label = "[dim]Informational[/dim]"

            cat_color = "red" if is_err else "yellow"
            table.add_row(
                f"[{cat_color}]{cat}[/{cat_color}]",
                str(len(urls)),
                str(live_ok),
                status_label,
            )

        console.print(table)

    # --- Drill-down per category ---
    submitted_count = 0
    for cat, urls in sorted_cats:
        is_err = _is_error_coverage(cat)
        live_ok = sum(1 for s in urls if _is_live_healthy(s))

        if not is_err and live_ok == 0:
            continue

        console.print()
        fix_label = f"{live_ok} appear fixed" if live_ok else "none fixed yet"
        border = "red" if is_err else "cyan"
        console.print(Panel(
            f"[bold]{cat}[/bold] — {len(urls)} URLs ({fix_label})",
            border_style=border,
        ))

        for s in urls:
            short = s.url.replace(base_url, "") or "/"
            healthy = _is_live_healthy(s)

            if healthy:
                marker = "[green]✓[/green]"
                detail = f"HTTP {s.http_status}"
            else:
                marker = "[red]✗[/red]"
                parts = []
                if s.http_status and s.http_status != 200:
                    parts.append(f"HTTP {s.http_status}")
                if s.has_noindex:
                    parts.append("noindex tag")
                if s.has_x_robots_noindex:
                    parts.append("X-Robots noindex")
                if s.http_error:
                    parts.append(s.http_error[:40])
                detail = ", ".join(parts) if parts else "still blocked"

            console.print(f"  {marker} {short}  [dim]({detail})[/dim]")

            if do_submit and healthy and s.action == "SUBMIT" and idx:
                if not tracker.was_recently_submitted(s.url, cooldown):
                    try:
                        submit_to_google_indexing(idx, s.url)
                        console.print(f"     [bold green]↳ Submitted[/bold green]")
                        s.last_submitted_at = datetime.now(timezone.utc).isoformat()
                        submitted_count += 1
                        tracker.record_submission(s.url)
                    except Exception as e:
                        console.print(f"     [red]↳ Submit error: {e}[/red]")

        fix_instr = _COVERAGE_FIX_INSTRUCTIONS.get(cat)
        if fix_instr:
            console.print(f"\n  [yellow]→ {fix_instr}[/yellow]")

    # --- Next steps ---
    console.print()
    console.print(Panel("[bold]Next Steps[/bold]", border_style="cyan"))

    error_cats = [cat for cat, _ in sorted_cats if _is_error_coverage(cat)]

    if error_cats:
        step = 1
        unfixed = [
            cat for cat, urls in sorted_cats
            if _is_error_coverage(cat) and any(not _is_live_healthy(s) for s in urls)
        ]
        if unfixed:
            console.print(f"  {step}. Fix remaining broken pages in:")
            for cat in unfixed:
                console.print(f"     • {cat}")
            step += 1

        if do_submit:
            console.print(f"  {step}. {submitted_count} fixed URLs submitted to Google Indexing API")
        else:
            console.print(
                f"  {step}. Re-run with [bold]--submit[/bold] to notify Google of fixed pages:"
            )
            console.print(
                f"     python seo_manager.py report --site '{site}' --submit"
            )
        step += 1

        encoded_site = quote(site, safe="")
        gsc_url = f"https://search.google.com/search-console/index?resource_id={encoded_site}"

        console.print(f"  {step}. Click [bold]Validate Fix[/bold] in GSC for each error category:")
        for cat in error_cats:
            console.print(f"     • {cat}")
        console.print(f"\n     {gsc_url}")
    else:
        if submitted_count > 0:
            console.print(f"  [green]No errors![/green] Submitted {submitted_count} eligible URLs.")
        elif any(s.action == "SUBMIT" for s in results):
            eligible = sum(1 for s in results if s.action == "SUBMIT")
            console.print(f"  [green]No errors![/green] {eligible} URLs eligible for submission.")
            console.print(
                f"  Run: python seo_manager.py report --site '{site}' --submit"
            )
        else:
            console.print("  [bold green]All pages indexed and healthy — nothing to do![/bold green]")

    # --- Footer ---
    console.print()
    console.print(f"  Total: {len(results)}  |  Indexed: {len(indexed)}  |  Not indexed: {len(not_indexed)}")
    if submitted_count:
        console.print(f"  Submitted this run: {submitted_count}")
    console.print(f"  DB saved to: {db}")
    console.print()
    tracker.close()


@cli.command()
@click.option("--client-secret", "client_secret_file", default="client_secret.json",
              help="Path to OAuth client secret JSON (download from Google Cloud Console)")
@click.option("--token-file", default=TOKEN_FILE, help="Where to save the OAuth token")
@click.option("--port", default=0, type=int,
              help="Local port for OAuth callback (0 = auto)")
@click.option("--headless", is_flag=True,
              help="Use console-based flow instead of opening a browser")
def auth(client_secret_file, token_file, port, headless):
    """Authenticate with Google via interactive OAuth (opens browser)."""
    if not os.path.exists(client_secret_file):
        console.print(Panel(
            f"[bold red]OAuth client secret file not found:[/bold red] {client_secret_file}\n\n"
            "To create one:\n"
            "  1. Go to https://console.cloud.google.com/apis/credentials\n"
            "  2. Click [bold]Create Credentials → OAuth client ID[/bold]\n"
            "  3. Application type: [bold]Desktop app[/bold]\n"
            "  4. Download the JSON file\n"
            "  5. Save it as [bold]client_secret.json[/bold] in this directory",
            title="OAuth Setup",
            border_style="yellow",
        ))
        raise SystemExit(1)

    console.print("[bold]Starting OAuth flow...[/bold]")
    if headless:
        console.print("(console mode — follow the URL printed below)")
    else:
        console.print("(a browser window will open for you to sign in)")

    try:
        creds = run_oauth_flow(client_secret_file, token_file, port, headless)
        console.print(f"\n[bold green]Authentication successful![/bold green]")
        console.print(f"  Token saved to: {os.path.abspath(token_file)}")
        console.print(f"  Scopes granted: {', '.join(creds.scopes or OAUTH_SCOPES)}")
        console.print(f"\nYou can now run: [bold]python seo_manager.py run[/bold]")
    except Exception as e:
        console.print(f"\n[red]Authentication failed: {e}[/red]")
        raise SystemExit(1)


@cli.command()
@click.option("--credentials", default=None, help="Path to service account JSON key")
@click.option("--site", default=None, help="Site URL for GSC (e.g. sc-domain:example.com or https://example.com/)")
@click.option("--sitemap", default=None, help="Sitemap URL (auto-derived from --site if omitted)")
@click.option("--local-sitemap", default=None, help="Local fallback sitemap file path")
@click.option("--dry-run", is_flag=True, help="Inspect only, do not submit for indexing")
@click.option("--cooldown", type=int, default=None, help="Hours to wait before re-submitting")
@click.option("--delay", type=float, default=None, help="Seconds between API requests")
@click.option("--db", default=None, help="Path to SQLite database file")
@click.option("--skip-gsc", is_flag=True, help="Skip GSC inspection (HTTP check only)")
@click.pass_context
def run(ctx, credentials, site, sitemap, local_sitemap, dry_run, cooldown, delay, db, skip_gsc):
    """Full audit: fetch sitemap, inspect, and submit eligible URLs."""
    cfg = ctx.obj["config"]
    credentials = credentials or cfg.get("credentials_file", "credentials.json")
    site_url = site or cfg.get("site_url")
    if not site_url:
        console.print("[red]--site is required.[/red]")
        console.print("  Run [bold]python seo_manager.py sites[/bold] to see your verified properties.")
        raise SystemExit(1)
    sitemap_url = sitemap or cfg.get("sitemap_url") or derive_sitemap_url(site_url)
    local_fallback = local_sitemap or cfg.get("sitemap_local_fallback")
    cooldown_hours = cooldown if cooldown is not None else cfg.get("cooldown_hours", DEFAULT_COOLDOWN_HOURS)
    req_delay = delay if delay is not None else cfg.get("request_delay_seconds", DEFAULT_DELAY)
    db_path = db or cfg.get("db_path", "seo_submissions.db")
    user_agent = cfg.get("user_agent", "Google-SEO-Manager/1.0")
    indexing_enabled = cfg.get("indexing_api_enabled", True) and not dry_run
    indexnow_enabled = cfg.get("indexnow_enabled", False) and not dry_run
    indexnow_key = cfg.get("indexnow_key")

    gsc_service = None
    indexing_service = None

    if not skip_gsc:
        has_token = os.path.exists(TOKEN_FILE)
        has_creds = os.path.exists(credentials)
        adc_path = os.path.expanduser("~/.config/gcloud/application_default_credentials.json")
        has_adc = os.path.exists(adc_path)
        if not has_token and not has_creds and not has_adc:
            console.print(Panel(
                SETUP_INSTRUCTIONS.format(dir=os.path.dirname(os.path.abspath(credentials)) or os.getcwd()),
                title="Setup Required",
                border_style="red",
            ))
            raise SystemExit(1)

        console.print("[bold]Authenticating with Google APIs...[/bold]")
        try:
            gsc_service = build_gsc_service(credentials)
            if indexing_enabled:
                indexing_service = build_indexing_service(credentials)
            console.print("  [green]Authenticated successfully[/green]")
        except Exception as e:
            console.print(f"[red]Authentication failed: {e}[/red]")
            raise SystemExit(1)

    tracker = SubmissionTracker(db_path)

    # Step 1: Fetch sitemap
    console.print()
    console.print(Panel("[bold]Step 1: Fetching sitemap[/bold]", border_style="cyan"))
    urls = fetch_sitemap(sitemap_url, local_fallback, user_agent)
    if not urls:
        console.print("[red]No URLs found in sitemap. Exiting.[/red]")
        raise SystemExit(1)
    console.print(f"  Found [bold green]{len(urls)}[/bold green] URLs in sitemap")

    # Step 2-5: Process each URL
    console.print()
    console.print(Panel("[bold]Step 2: Inspecting URLs[/bold]", border_style="cyan"))
    base_url = derive_base_url(site_url)

    results: list[UrlStatus] = []
    submitted_count = 0

    for i, url in enumerate(urls, 1):
        short = url.replace(base_url, "") or "/"
        console.print(f"\n  [{i}/{len(urls)}] {short}")

        # HTTP health check
        status = check_http_health(url, user_agent)
        if status.http_status:
            color = "green" if status.http_status == 200 else "yellow" if status.http_status < 400 else "red"
            redir = f" → {status.redirect_target}" if status.redirect_target else ""
            console.print(f"    HTTP: [{color}]{status.http_status}[/{color}] ({status.response_time_ms}ms){redir}")
        elif status.http_error:
            console.print(f"    HTTP: [red]FAIL - {status.http_error}[/red]")

        if status.has_noindex:
            console.print("    [yellow]⚠ noindex meta tag detected[/yellow]")
        if status.has_x_robots_noindex:
            console.print("    [yellow]⚠ X-Robots-Tag: noindex detected[/yellow]")

        # GSC Inspection
        if gsc_service and not status.http_error:
            try:
                result = inspect_url(gsc_service, url, site_url)
                parse_inspection_result(status, result)
                cov_color = "green" if status.coverage_state and "INDEX" in status.coverage_state.upper() else "yellow"
                console.print(f"    GSC coverage: [{cov_color}]{status.coverage_state or 'unknown'}[/{cov_color}]")
                if status.last_crawl_time:
                    console.print(f"    Last crawl: {status.last_crawl_time}")
            except Exception as e:
                status.inspection_error = str(e)
                console.print(f"    GSC: [red]inspection error - {e}[/red]")

        # Decision
        action, reason = decide_action(status, tracker, cooldown_hours)
        status.action = action
        status.action_reason = reason

        color = ACTION_COLORS.get(action, "white")
        console.print(f"    Action: [{color}]{action}[/{color}] - {reason}")

        # Submit if eligible
        if action == "SUBMIT" and not dry_run:
            submitted_this = False
            if indexing_service:
                try:
                    submit_to_google_indexing(indexing_service, url)
                    console.print(f"    [bold green]✓ Submitted to Google Indexing API[/bold green]")
                    submitted_this = True
                except Exception as e:
                    console.print(f"    [red]Google Indexing API error: {e}[/red]")

            if indexnow_enabled and indexnow_key:
                ok = submit_to_indexnow(url, indexnow_key)
                label = "[bold green]✓ Submitted to IndexNow[/bold green]" if ok else "[red]IndexNow submission failed[/red]"
                console.print(f"    {label}")
                if ok:
                    submitted_this = True

            if submitted_this:
                status.last_submitted_at = datetime.now(timezone.utc).isoformat()
                submitted_count += 1
                tracker.record_submission(url)

        # Save to DB
        tracker.upsert(status)
        results.append(status)

        if i < len(urls) and req_delay > 0 and gsc_service:
            time.sleep(req_delay)

    if dry_run:
        console.print("\n  [yellow]DRY RUN: No URLs were submitted.[/yellow]")

    _print_report(results, site_url, submitted_count=submitted_count,
                  did_submit=not dry_run)
    console.print(f"  DB saved to: {db_path}")

    tracker.close()


@cli.command()
@click.option("--db", default="seo_submissions.db", help="Path to SQLite database")
@click.option("--site", default=None, help="Filter by site (e.g. sc-domain:example.com)")
def status(db, site):
    """Show status of all tracked URLs from the database."""
    if not os.path.exists(db):
        console.print("[yellow]No database found. Run 'run' first.[/yellow]")
        return

    tracker = SubmissionTracker(db)
    rows = tracker.get_all()
    tracker.close()

    base_url = derive_base_url(site) if site else None

    if site and base_url:
        rows = [r for r in rows if r["url"].startswith(base_url)]

    if not rows:
        console.print("[yellow]No URLs tracked yet.[/yellow]")
        return

    title = f"Tracked URL Status — {site}" if site else "Tracked URL Status"
    table = Table(title=title, show_header=True, header_style="bold",
                  show_lines=False, width=120)
    table.add_column("URL", no_wrap=True, ratio=4)
    table.add_column("HTTP", justify="center", width=5)
    table.add_column("Coverage", width=16)
    table.add_column("Action", width=10)
    table.add_column("Submitted At", width=20)
    table.add_column("#", justify="center", width=2)

    for row in rows:
        http_str = str(row.get("http_status") or "-")
        url_display = row["url"] or ""
        if base_url:
            url_display = url_display.replace(base_url, "")
        sub_at = row.get("last_submitted_at") or "-"
        if sub_at != "-" and len(sub_at) > 19:
            sub_at = sub_at[:19]
        table.add_row(
            url_display or "/",
            http_str,
            row.get("coverage_state") or "-",
            row.get("action") or "-",
            sub_at,
            str(row.get("submit_count") or 0),
        )

    console.print(table)
    console.print(f"\n  Total tracked: {len(rows)}")


@cli.command()
@click.option("--url", required=True, help="URL to force-submit for indexing")
@click.option("--credentials", default="credentials.json", help="Path to credentials JSON")
@click.option("--db", default="seo_submissions.db", help="Path to SQLite database")
@click.option("--indexnow-key", default=None, help="IndexNow API key")
def submit(url, credentials, db, indexnow_key):
    """Force-submit a specific URL for indexing."""
    has_token = os.path.exists(TOKEN_FILE)
    has_creds = os.path.exists(credentials)
    adc_path = os.path.expanduser("~/.config/gcloud/application_default_credentials.json")
    has_adc = os.path.exists(adc_path)
    if not has_token and not has_creds and not has_adc:
        console.print(Panel(
            SETUP_INSTRUCTIONS.format(dir=os.getcwd()),
            title="Setup Required",
            border_style="red",
        ))
        raise SystemExit(1)

    tracker = SubmissionTracker(db)
    submitted = False

    try:
        console.print(f"Submitting: {url}")
        indexing_service = build_indexing_service(credentials)
        resp = submit_to_google_indexing(indexing_service, url)
        console.print(f"[bold green]✓ Submitted to Google Indexing API[/bold green]")
        console.print(f"  Response: {json.dumps(resp, indent=2)}")
        submitted = True
    except Exception as e:
        console.print(f"[red]Google Indexing API error: {e}[/red]")

    if indexnow_key:
        ok = submit_to_indexnow(url, indexnow_key)
        if ok:
            console.print("[bold green]✓ Submitted to IndexNow[/bold green]")
            submitted = True
        else:
            console.print("[red]IndexNow submission failed[/red]")

    if submitted:
        tracker.record_submission(url)
        console.print(f"Submission recorded in {db}")

    tracker.close()


if __name__ == "__main__":
    cli()
