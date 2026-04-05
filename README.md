# Google SEO Manager

CLI tool that automates Google Search Console workflows: fetch sitemaps, inspect URLs, detect indexing errors, and submit fixed pages for re-crawling — with parallel execution.

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Python 3.9+](https://img.shields.io/badge/Python-3.9+-green.svg)](https://python.org)
[![Zero Config](https://img.shields.io/badge/Zero_Config-gcloud_auth-orange.svg)](#quick-start)

Python Google Search Console API Google Indexing API OAuth 2.0 Rich CLI SQLite

---

### Why

Google Search Console shows you what's broken, but fixing SEO at scale means clicking through dozens of pages, cross-referencing errors with live HTTP status, and manually submitting URLs one by one. This tool does all of that from the command line — inspect every page in your sitemap, identify what Google can't index, check if it's actually fixed, and submit for re-crawling. All in under 2 minutes with parallel workers.

### The Problem

| Task | Manual (GSC UI) | Google SEO Manager |
|---|---|---|
| Check all sitemap URLs | Click each one in URL Inspection | `report --site ...` — all at once |
| Cross-reference errors with live HTTP | Open page in browser, check headers | Automatic — HTTP + GSC in parallel |
| Find fixed-but-still-flagged pages | Compare GSC error vs current state | Built-in: "GSC says 404 but page is now 200" |
| Submit pages for re-indexing | One URL at a time in GSC UI | `--submit` flag — batch submission |
| Track submission cooldowns | Remember what you submitted when | SQLite DB — automatic 48h cooldown |
| Discover non-sitemap URLs | Browse GSC coverage report | `--crawl` — BFS link crawler |

### Features

- **Parallel inspection** — 5+ concurrent workers, 67 URLs in ~90 seconds
- **GSC-style report** — mirrors the "Why pages aren't indexed" view with error categories
- **Smart fix detection** — cross-checks GSC errors against live HTTP status to find pages that were fixed but not re-crawled
- **Batch submission** — submit fixed/eligible URLs to Google Indexing API
- **Submission tracking** — SQLite database with configurable cooldown to avoid re-submitting
- **Site crawler** — BFS crawl to discover non-sitemap internal URLs
- **Multi-site support** — works with any property verified in Google Search Console
- **Zero config** — authenticates via `gcloud` CLI, no service account files needed
- **Rich terminal UI** — progress bars, colored tables, structured error reports

### Commands

| Command | Description |
|---|---|
| `setup` | One-step auth: enable APIs + OAuth sign-in via gcloud |
| `sites` | List all verified Search Console properties |
| `report` | Page indexing report (mirrors GSC "Why pages aren't indexed") |
| `check` | Inspect sitemap URLs + optionally submit eligible pages |
| `run` | Full audit: fetch, inspect, submit (legacy, feature-complete) |
| `status` | Show tracked URL status from the local database |
| `submit` | Force-submit a single URL for indexing |

### Architecture

```
                    ┌─────────────┐
                    │   Sitemap   │
                    │  XML Fetch  │
                    └──────┬──────┘
                           │
                    ┌──────▼──────┐     ┌──────────────┐
                    │  Optional   │────▶│  BFS Crawler  │
                    │   --crawl   │     │ (internal <a>)│
                    └──────┬──────┘     └──────┬───────┘
                           │                   │
                    ┌──────▼───────────────────▼──┐
                    │   Parallel Inspection Pool   │
                    │   (ThreadPoolExecutor × N)   │
                    ├──────────────┬───────────────┤
                    │  HTTP Health │  GSC URL      │
                    │  Check       │  Inspection   │
                    └──────────────┴───────┬───────┘
                                           │
                    ┌──────────────────────▼──────┐
                    │      Decision Engine         │
                    │  GSC error + live status →   │
                    │  OK / SUBMIT / ERROR / SKIP  │
                    └──────────────┬───────────────┘
                                   │
                    ┌──────────────▼───────────────┐
                    │  Google Indexing API Submit   │
                    │  + SQLite Tracking + Report   │
                    └──────────────────────────────┘
```

---

### Quick Start

#### 1. Install

```bash
git clone https://github.com/humatic-ai/google-seo-manager.git
cd google-seo-manager
pip install -r requirements.txt
```

#### 2. Authenticate

```bash
python seo_manager.py setup
```

This enables the required Google APIs and runs OAuth sign-in. A URL is printed — open it in your browser, sign in, paste the redirect URL back.

> **Requires:** `gcloud` CLI installed and a GCP project configured (`gcloud config set project YOUR_PROJECT`).

#### 3. List your sites

```bash
python seo_manager.py sites
```

#### 4. Run a report

```bash
# Inspect-only (safe, no submissions)
python seo_manager.py report --site 'sc-domain:example.com'

# Inspect + submit fixed/eligible pages
python seo_manager.py report --site 'sc-domain:example.com' --submit

# Also discover non-sitemap URLs via crawling
python seo_manager.py report --site 'sc-domain:example.com' --crawl --submit
```

### CLI Reference

#### `report` — Page indexing report

```
python seo_manager.py report --site SITE [OPTIONS]

Options:
  --site TEXT          GSC property (e.g. sc-domain:example.com)  [required]
  --sitemap TEXT       Sitemap URL (auto-derived from site)
  --crawl             Also crawl the site to discover non-sitemap URLs
  --max-pages INT     Max pages to crawl (default 200)
  --submit            Submit fixed/eligible URLs for re-indexing
  --workers INT       Parallel workers (default 5)
  --cooldown INT      Hours before re-submitting a URL (default 48)
  --db TEXT           Path to SQLite database
```

#### `check` — Inspect + submit

```
python seo_manager.py check --site SITE [OPTIONS]

Options:
  --site TEXT          GSC property  [required]
  --submit            Also submit eligible pages
  --workers INT       Parallel workers (default 5)
  --cooldown INT      Hours before re-submitting (default 48)
```

#### `run` — Full audit (legacy)

```
python seo_manager.py run --site SITE [OPTIONS]

Options:
  --site TEXT          GSC property  [required]
  --dry-run           Inspect only, do not submit
  --skip-gsc          HTTP check only, skip GSC API
  --cooldown INT      Hours before re-submitting (default 48)
  --delay FLOAT       Seconds between sequential requests (default 1.5)
```

### Configuration

An optional `config.json` can set defaults (see `config.example.json`):

```json
{
  "site_url": "sc-domain:example.com",
  "cooldown_hours": 48,
  "indexing_api_enabled": true,
  "user_agent": "Google-SEO-Manager/1.0"
}
```

### Error Categories

The `report` command groups URLs by the same categories shown in the GSC "Page indexing" report:

| Category | Type | What the tool does |
|---|---|---|
| Excluded by 'noindex' tag | Error | Flags it, checks if noindex was removed |
| Blocked by robots.txt | Error | Flags it, shows fix instruction |
| Soft 404 | Error | Flags it, checks if content was added |
| Not found (404) | Error | Flags it, checks if page was restored |
| Server error (5xx) | Error | Flags it, checks if server is healthy now |
| Crawled - currently not indexed | Eligible | Submits if page is healthy |
| Discovered - currently not indexed | Eligible | Submits if page is healthy |
| URL is unknown to Google | Eligible | Submits if page is healthy |
| Page with redirect | Info | Informational |
| Duplicate, Google chose different canonical | Info | Informational |

For each error, the tool cross-checks whether the **live page is now healthy** (HTTP 200, no noindex). If it is, the action becomes "SUBMIT" with the note: *"GSC reported X but page is now healthy — submit to re-crawl."*

### Security

- **No secrets in the repo** — `token.json`, `credentials.json`, `.env`, and `*.db` are git-ignored
- **OAuth tokens are local** — stored in `token.json` with user-level permissions
- **No hardcoded domains** — works with any GSC-verified site property
- **Quota project auto-detected** — from `gcloud config`, no manual setup needed

### File Structure

```
google-seo-manager/
├── seo_manager.py         # All CLI commands and logic
├── config.example.json    # Example configuration
├── requirements.txt       # Python dependencies
├── .env.example           # Environment variable template
├── .gitignore             # Excludes credentials, tokens, DB
├── LICENSE                # MIT
└── README.md
```

### Requirements

- **Python** >= 3.9
- **gcloud CLI** (for setup and API enablement)
- **GCP project** with Search Console and Indexing APIs enabled

### Contributing

1. Fork the repo
2. Create a feature branch
3. Submit a pull request

### License

MIT

---

Built by [Humatic AI](https://humaticai.com) · [humatic-ai](https://github.com/humatic-ai)
