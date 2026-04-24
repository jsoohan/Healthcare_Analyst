#!/usr/bin/env python3
"""
Phase 0b: Collect IR Presentations.
Selenium-based collector that downloads quarterly IR presentation PDFs/PPTXs
for each company in the HealthcareIntel database.

Flow per company (collect_one):
  Step 0 (NEW): Try ir_url_map direct lookup
  Step 1: Google search for direct PDF/PPTX links
  Step 2: Google search for IR pages, then crawl for presentation links
  Step 3: Fall back to broader search terms

Output: ./ir_presentations/{sanitize(name)}_{Q}_{YYYY}.{pdf|pptx}

Usage:
  python scripts/collect_ir_presentations.py --quarter Q4 --year 2025
  python scripts/collect_ir_presentations.py --quarter Q4 --year 2025 --sector Biopharma
  python scripts/collect_ir_presentations.py --quarter Q4 --year 2025 --headless --limit 50
"""
import argparse
import csv
import json
import os
import re
import shutil
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse, unquote, quote as url_quote

# Allow direct execution: python scripts/collect_ir_presentations.py
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    WebDriverException,
    StaleElementReferenceException,
)
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup

from scripts.db_loader import load_companies, find_db_path, sanitize

# ============================================================
# Constants
# ============================================================

DEFAULT_DELAY = 5
RESTART_EVERY = 20
MAX_RETRY = 2
DOWNLOAD_WAIT = 45
MIN_FILE_SIZE = 30 * 1024  # 30 KB

QUARTER_KEYWORDS = {
    "Q1": [
        "q1", "first quarter", "1st quarter", "first-quarter",
        "march", "mar", "1q",
    ],
    "Q2": [
        "q2", "second quarter", "2nd quarter", "second-quarter",
        "june", "jun", "2q",
    ],
    "Q3": [
        "q3", "third quarter", "3rd quarter", "third-quarter",
        "september", "sep", "sept", "3q",
    ],
    "Q4": [
        "q4", "fourth quarter", "4th quarter", "fourth-quarter",
        "december", "dec", "4q", "full year", "full-year", "annual",
    ],
}

BLOCKED_DOMAINS = [
    "google.", "youtube.", "wikipedia.", "facebook.", "twitter.", "x.com",
    "linkedin.", "reddit.", "amazon.", "yahoo.", "bing.",
    "seekingalpha.", "zacks.", "tipranks.", "morningstar.",
    "stockanalysis.", "macrotrends.", "simplywall.", "gurufocus.",
    "marketwatch.", "fool.", "investopedia.", "glassdoor.",
    "crunchbase.", "bloomberg.", "reuters.", "wsj.",
]

# ============================================================
# Driver setup
# ============================================================


def create_driver(headless=False, download_dir=None):
    """Create Chrome driver. Respects STEALTH_BROWSER and CHROME_PROFILE env vars."""
    from scripts.browser_utils import create_driver as _make
    driver = _make(headless=headless, download_dir=download_dir)
    driver.set_page_load_timeout(20)
    driver.implicitly_wait(3)
    return driver


# ============================================================
# URL utilities
# ============================================================


def quarter_label(quarter, year):
    """Format quarter label, e.g. 'Q4_2025'."""
    return f"{quarter}_{year}"


def is_blocked_domain(url):
    """Check if URL belongs to a blocked domain."""
    try:
        domain = urlparse(url).netloc.lower()
        return any(b in domain for b in BLOCKED_DOMAINS)
    except Exception:
        return True


def domain_relevance(url, company_name, ticker):
    """Score how relevant a URL domain is to the company (0-10).

    Ported from user's original working script. Recognizes IR hosting CDNs
    (q4cdn, cloudfront), filing systems (sec.gov, dart.fss, hkexnews),
    and filters out known irrelevant domains (pension funds, ETFs).
    """
    try:
        domain = urlparse(url).netloc.lower()
        path = urlparse(url).path.lower()
    except Exception:
        return 0

    full = domain + path
    name_parts = re.sub(r'[^a-zA-Z0-9\s]', '', company_name).lower().split()
    score = 0

    if name_parts and name_parts[0] in domain:
        score += 6
    if ticker and ticker.lower() in full:
        score += 5
    if any(p in domain for p in ["investor", "ir.", "q4cdn", "sec.gov",
                                  "hkexnews", "cninfo", "sse.com",
                                  "szse.cn", "tdnet", "edinet"]):
        score += 4
    if any(p in domain for p in ["dart.fss", "edgar", "sedar"]):
        score += 4
    if any(p in domain for p in ["q4cdn", "s3.amazonaws", "cloudfront",
                                  "bfrqr.com", "notified.com"]):
        score += 3
    if any(p in domain for p in ["swissfund", "hesta.", "mbs.com", "wisesheets",
                                  "funddata", "pension", "voting", "indexfunds",
                                  "etf.", "mutualfund", "fund.", "assetmanag"]):
        return 0

    if score == 0:
        return 0
    return score


def extract_search_url(href):
    """Extract actual URL from search engine result link.

    Google: /url?q=https%3A//actual.com/file.pdf&...
    Bing:   /ck/a?...&u=a1aHR0cHM6Ly9hY3R1YWwuY29t...&...  (base64 in u= param)
    Direct: https://actual.com/file.pdf
    """
    import base64 as _b64

    # Google redirect
    if "/url?q=" in href:
        m = re.search(r'/url\?q=([^&]+)', href)
        if m:
            return unquote(m.group(1))

    # Bing tracking redirect (u=a1 + base64-encoded URL)
    if "bing.com" in href and "u=a1" in href:
        m = re.search(r'u=a1([A-Za-z0-9_\-]+)', href)
        if m:
            try:
                padded = m.group(1) + "==="
                decoded = _b64.urlsafe_b64decode(padded).decode("utf-8", errors="ignore")
                if decoded.startswith("http"):
                    return decoded
            except Exception:
                pass

    # Direct link (not a search engine domain)
    if href.startswith("http"):
        domain = urlparse(href).netloc.lower()
        if "google." not in domain and "bing." not in domain and "yahoo." not in domain:
            return href

    return None


# Backward compat alias
extract_google_url = extract_search_url


def is_pdf_or_pptx(url):
    """Check if URL points to a PDF or PPTX file."""
    try:
        path = urlparse(url).path.lower()
        return path.endswith(".pdf") or path.endswith(".pptx")
    except Exception:
        return False


# ============================================================
# Download management
# ============================================================


def clear_temp_dir(temp_dir):
    """Remove all files in the temp download directory."""
    if os.path.exists(temp_dir):
        for f in os.listdir(temp_dir):
            fp = os.path.join(temp_dir, f)
            try:
                if os.path.isfile(fp):
                    os.remove(fp)
            except OSError:
                pass


def wait_for_download(temp_dir, timeout=DOWNLOAD_WAIT):
    """Wait until a download completes in temp_dir (no .crdownload files)."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        files = os.listdir(temp_dir) if os.path.exists(temp_dir) else []
        # Filter out hidden/system files
        files = [f for f in files if not f.startswith(".")]
        if files:
            # Still downloading?
            downloading = [f for f in files if f.endswith(".crdownload") or f.endswith(".tmp")]
            if not downloading:
                return True
        time.sleep(1)
    return False


def get_downloaded_file(temp_dir):
    """Get the path and size of the downloaded file in temp_dir."""
    if not os.path.exists(temp_dir):
        return None, 0
    files = [
        f for f in os.listdir(temp_dir)
        if not f.startswith(".") and not f.endswith(".crdownload") and not f.endswith(".tmp")
    ]
    if not files:
        return None, 0
    # Pick the largest file
    best = None
    best_size = 0
    for f in files:
        fp = os.path.join(temp_dir, f)
        try:
            sz = os.path.getsize(fp)
            if sz > best_size:
                best = fp
                best_size = sz
        except OSError:
            continue
    return best, best_size


def download_and_rename(driver, url, temp_dir, output_dir, final_name):
    """Download a file and rename it to final_name in output_dir.

    Returns (output_path, file_size) on success, or None on failure.
    """
    clear_temp_dir(temp_dir)

    # Determine extension from URL
    parsed_path = urlparse(url).path.lower()
    if parsed_path.endswith(".pptx"):
        ext = ".pptx"
    elif parsed_path.endswith(".pdf"):
        ext = ".pdf"
    else:
        ext = ".pdf"  # default

    try:
        driver.get(url)
    except TimeoutException:
        # Page load timeout is expected for direct downloads
        pass
    except WebDriverException as e:
        if "net::ERR_" in str(e):
            return None
        # Some download triggers cause exceptions, continue
        pass

    time.sleep(3)

    if not wait_for_download(temp_dir):
        return None

    downloaded, fsize = get_downloaded_file(temp_dir)
    if not downloaded or fsize < MIN_FILE_SIZE:
        return None

    # Determine extension from the actual downloaded file if possible
    _, dl_ext = os.path.splitext(downloaded)
    if dl_ext.lower() in (".pdf", ".pptx", ".ppt", ".xlsx"):
        ext = dl_ext.lower()

    output_path = os.path.join(output_dir, f"{final_name}{ext}")
    try:
        shutil.move(downloaded, output_path)
    except OSError:
        return None

    actual_size = os.path.getsize(output_path)
    return output_path, actual_size


# ============================================================
# Google search & IR page scanning
# ============================================================


def google_search_candidates(driver, company_name, quarter, year):
    """Search Google for IR presentation links.

    Returns two lists:
      direct_pdfs: list of dicts {url, text, score} for direct PDF/PPTX links
      ir_pages:    list of dicts {url, text, score} for IR pages to crawl
    """
    q_kws = QUARTER_KEYWORDS.get(quarter, [quarter.lower()])
    q_label = quarter_label(quarter, year)

    queries = [
        f'"{company_name}" investor presentation {quarter} {year} filetype:pdf',
        f'"{company_name}" earnings presentation {quarter} {year}',
        f'"{company_name}" investor relations presentation {year}',
    ]

    direct_pdfs = []
    ir_pages = []
    seen_urls = set()

    for query in queries:
        search_url = f"https://www.bing.com/search?q={url_quote(query)}&count=20"
        try:
            driver.get(search_url)
            time.sleep(3)
        except (TimeoutException, WebDriverException):
            continue

        soup = BeautifulSoup(driver.page_source, "html.parser")

        for tag in soup.find_all("a", href=True):
            raw = extract_google_url(tag["href"])
            if not raw or not raw.startswith("http"):
                continue
            if is_blocked_domain(raw):
                continue
            if raw in seen_urls:
                continue
            seen_urls.add(raw)

            text = tag.get_text(strip=True).lower()
            url_lower = raw.lower()

            # Check quarter/year relevance
            has_quarter = any(kw in url_lower or kw in text for kw in q_kws)
            has_year = str(year) in url_lower or str(year) in text

            if is_pdf_or_pptx(raw):
                score = 0
                if has_quarter:
                    score += 3
                if has_year:
                    score += 2
                if "presentation" in text or "presentation" in url_lower:
                    score += 2
                if "earnings" in text or "earnings" in url_lower:
                    score += 1
                if "investor" in text or "investor" in url_lower:
                    score += 1
                direct_pdfs.append({"url": raw, "text": text, "score": score})
            else:
                # Potential IR page
                is_ir = any(kw in url_lower for kw in [
                    "investor", "/ir/", "/ir.", "presentations",
                    "events", "quarterly-results",
                ])
                if is_ir or "investor" in text or "presentation" in text:
                    score = 0
                    if has_year:
                        score += 1
                    if "investor" in url_lower or "investor" in text:
                        score += 1
                    if "presentation" in url_lower or "presentation" in text:
                        score += 1
                    ir_pages.append({"url": raw, "text": text, "score": score})

    # Sort by score descending
    direct_pdfs.sort(key=lambda x: x["score"], reverse=True)
    ir_pages.sort(key=lambda x: x["score"], reverse=True)

    return direct_pdfs, ir_pages


def scan_ir_page(driver, ir_url, quarter, year):
    """Crawl an IR page and extract presentation download links.

    Returns list of dicts {url, text, score} sorted by relevance.
    """
    try:
        driver.get(ir_url)
        time.sleep(3)
    except (TimeoutException, WebDriverException):
        return []

    soup = BeautifulSoup(driver.page_source, "html.parser")
    q_kws = QUARTER_KEYWORDS.get(quarter, [quarter.lower()])
    base_domain = urlparse(ir_url).scheme + "://" + urlparse(ir_url).netloc

    candidates = []
    seen = set()

    for tag in soup.find_all("a", href=True):
        href = tag["href"]

        # Resolve relative URLs
        if href.startswith("/"):
            href = base_domain + href
        elif not href.startswith("http"):
            # Relative path
            base_path = ir_url.rsplit("/", 1)[0]
            href = base_path + "/" + href

        if href in seen:
            continue
        seen.add(href)

        text = tag.get_text(strip=True).lower()
        url_lower = href.lower()

        # Only care about PDF/PPTX or pages with presentation keywords
        if not is_pdf_or_pptx(href):
            # Also check for download links that don't have extensions
            if not any(kw in url_lower for kw in ["download", "getfile", "document"]):
                continue

        has_quarter = any(kw in url_lower or kw in text for kw in q_kws)
        has_year = str(year) in url_lower or str(year) in text

        score = 0
        if has_quarter:
            score += 4
        if has_year:
            score += 3
        if "presentation" in text or "presentation" in url_lower:
            score += 2
        if "earnings" in text or "earnings" in url_lower:
            score += 2
        if "quarterly" in text or "results" in text:
            score += 1
        if is_pdf_or_pptx(href):
            score += 1

        if score >= 2:
            candidates.append({"url": href, "text": text, "score": score})

    candidates.sort(key=lambda x: x["score"], reverse=True)
    return candidates


# ============================================================
# IR URL Map (NEW Step 0)
# ============================================================


def load_ir_url_map(path="data/ir_url_map.json"):
    """Load the IR URL map produced by build_ir_url_map.py."""
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


# ============================================================
# Progress tracking
# ============================================================


def load_progress(log_path):
    """Load progress CSV and return set of already-collected keys."""
    collected = set()
    if os.path.exists(log_path):
        with open(log_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                key = row.get("key", "")
                status = row.get("status", "")
                if key and status == "OK":
                    collected.add(key)
    return collected


def append_progress(log_path, entry):
    """Append a single progress entry to the CSV log."""
    fieldnames = [
        "key", "company", "ticker", "quarter", "year",
        "status", "method", "source_url", "file_path",
        "file_size", "timestamp",
    ]
    write_header = not os.path.exists(log_path)
    with open(log_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        writer.writerow(entry)


def already_collected(collected_set, company_name, quarter, year):
    """Check if this company+quarter+year is already collected."""
    key = f"{sanitize(company_name)}_{quarter_label(quarter, year)}"
    return key in collected_set


def already_collected_greenwood(output_root, company_name, period, sector,
                                  ticker=None):
    """Check company-name path first, ticker path as legacy fallback."""
    from scripts.greenwood_adapter import make_output_path
    candidates = [company_name]
    if ticker:
        candidates.append(ticker)
    for name in candidates:
        for ext in [".pdf", ".pptx", ".ppt", ".xlsx"]:
            filepath = make_output_path(
                name, period, sector, "Presentation", ext, output_root
            )
            if filepath.exists() and filepath.stat().st_size >= MIN_FILE_SIZE:
                return True
    return False


def resolve_output_for_company(args, company, greenwood_period):
    """Return (output_dir, final_name) for a company based on output mode.

    Flat mode:      ({args.output}, {sanitize(name)}_{Q}_{YYYY})
    Greenwood mode: ({root}/{period_dir}/{sector}/{Company}/, {Company}_{period}_Presentation)
    """
    if args.output_mode == "greenwood":
        from scripts.greenwood_adapter import (
            period_dir_name, sanitize_sector_name, _sanitize_company,
        )
        sector = company.get("sector", "") or company.get("tier1", "")
        sector_dir_name = sanitize_sector_name(sector) if sector else "_unmapped"
        company_dir_name = _sanitize_company(company["company_name"])
        company_dir = (Path(args.output_root)
                       / period_dir_name(greenwood_period)
                       / sector_dir_name
                       / company_dir_name)
        company_dir.mkdir(parents=True, exist_ok=True)
        final_name = f"{company_dir_name}_{greenwood_period}_Presentation"
        return str(company_dir), final_name

    # Flat mode (legacy)
    final_name = f"{sanitize(company['company_name'])}_{quarter_label(args.quarter, args.year)}"
    return args.output, final_name


# ============================================================
# Core collection logic
# ============================================================


def collect_one(driver, company, quarter, year, temp_dir, output_dir, tag="",
                 final_name=None, edgar_client=None):
    """Collect IR presentation for one company.

    5-step approach:
      Step 0a: SEC EDGAR 8-K exhibit (US companies, no browser needed)
      Step 0b: ir_url_map direct lookup
      Step 1: Google search -> direct PDF/PPTX links
      Step 2: Google search -> IR pages -> crawl for links
      Step 3: Broader search fallback

    Args:
        final_name: Override computed name (for greenwood mode).
        edgar_client: EdgarClient instance (reused across companies).

    Returns (file_path, file_size, method, source_url) or (None, 0, None, None).
    """
    name = company["company_name"]
    ticker = company["ticker"]
    exchange = company.get("exchange", "")
    search_term = company.get("search_term", ticker)
    q_label = quarter_label(quarter, year)
    if final_name is None:
        final_name = f"{sanitize(name)}_{q_label}"

    # ----------------------------------------------------------
    # Step 0a: SEC EDGAR 8-K exhibit (US companies — no browser, no rate limit)
    # ----------------------------------------------------------
    us_exchanges = {"NYSE", "NASDAQ", "Nasdaq", "AMEX", "OTC"}
    if edgar_client and any(ex in exchange for ex in us_exchanges):
        print(f"  {tag} Step 0a: SEC EDGAR 8-K search...")
        try:
            result = edgar_client.find_earnings_presentation(
                ticker, quarter, year, verbose=True)
            if result:
                ext = os.path.splitext(result["filename"])[1].lower() or ".pdf"
                out_path = os.path.join(output_dir, f"{final_name}{ext}")
                os.makedirs(output_dir, exist_ok=True)
                fsize = edgar_client.download(result["url"], out_path)
                if fsize >= MIN_FILE_SIZE:
                    return out_path, fsize, "sec_edgar", result["url"]
                else:
                    try:
                        os.remove(out_path)
                    except OSError:
                        pass
        except Exception as e:
            print(f"  {tag} Step 0a: EDGAR failed: {str(e)[:100]}")

    # ----------------------------------------------------------
    # Step 0b: Try ir_url_map (direct IR page access)
    # ----------------------------------------------------------
    ir_map = load_ir_url_map()
    if ticker in ir_map and ir_map[ticker].get("ir_url"):
        ir_url = ir_map[ticker]["ir_url"]
        print(f"  {tag} Step 0: Trying IR URL map -> {ir_url[:60]}")
        try:
            links = scan_ir_page(driver, ir_url, quarter, year)
            for link in links[:3]:
                if is_pdf_or_pptx(link["url"]):
                    clear_temp_dir(temp_dir)
                    result = download_and_rename(
                        driver, link["url"], temp_dir, output_dir, final_name
                    )
                    if result and result[1] >= MIN_FILE_SIZE:
                        return result[0], result[1], "ir_url_map", link["url"]
        except (TimeoutException, WebDriverException) as e:
            print(f"  {tag} Step 0: IR map failed: {str(e)[:60]}")

    # ----------------------------------------------------------
    # Step 1: Google search for direct PDF/PPTX links
    # ----------------------------------------------------------
    print(f"  {tag} Step 1: Bing search (direct files)...")
    try:
        direct_pdfs, ir_pages = google_search_candidates(
            driver, search_term, quarter, year
        )
    except Exception as e:
        print(f"  {tag} Step 1: Bing search failed: {str(e)[:60]}")
        direct_pdfs, ir_pages = [], []

    for link in direct_pdfs[:5]:
        clear_temp_dir(temp_dir)
        result = download_and_rename(
            driver, link["url"], temp_dir, output_dir, final_name
        )
        if result and result[1] >= MIN_FILE_SIZE:
            return result[0], result[1], "bing_direct", link["url"]

    # ----------------------------------------------------------
    # Step 2: Crawl IR pages found via Google
    # ----------------------------------------------------------
    print(f"  {tag} Step 2: Crawling {len(ir_pages[:3])} IR pages...")
    for ir_page in ir_pages[:3]:
        try:
            links = scan_ir_page(driver, ir_page["url"], quarter, year)
        except Exception:
            continue

        for link in links[:3]:
            if is_pdf_or_pptx(link["url"]):
                clear_temp_dir(temp_dir)
                result = download_and_rename(
                    driver, link["url"], temp_dir, output_dir, final_name
                )
                if result and result[1] >= MIN_FILE_SIZE:
                    return result[0], result[1], "ir_page_crawl", link["url"]

    # ----------------------------------------------------------
    # Step 3: Bing fallback search
    # ----------------------------------------------------------
    print(f"  {tag} Step 3: Bing fallback search...")
    fallback_queries = [
        f'{name} {ticker} quarterly presentation {year} filetype:pdf',
        f'{name} earnings slides {quarter} {year}',
    ]

    for query in fallback_queries:
        search_url = f"https://www.bing.com/search?q={url_quote(query)}&count=15"
        try:
            driver.get(search_url)
            time.sleep(3)
        except (TimeoutException, WebDriverException):
            continue

        soup = BeautifulSoup(driver.page_source, "html.parser")

        for a_tag in soup.find_all("a", href=True):
            raw = extract_google_url(a_tag["href"])
            if not raw or not raw.startswith("http"):
                continue
            if is_blocked_domain(raw):
                continue
            if not is_pdf_or_pptx(raw):
                continue

            # Quick relevance check
            url_lower = raw.lower()
            text_lower = a_tag.get_text(strip=True).lower()
            has_year = str(year) in url_lower or str(year) in text_lower

            if not has_year:
                continue

            clear_temp_dir(temp_dir)
            result = download_and_rename(
                driver, raw, temp_dir, output_dir, final_name
            )
            if result and result[1] >= MIN_FILE_SIZE:
                return result[0], result[1], "bing_fallback", raw

    return None, 0, None, None


# ============================================================
# Main
# ============================================================


def main():
    parser = argparse.ArgumentParser(
        description="Collect IR Presentations for HealthcareIntel companies"
    )
    parser.add_argument(
        "--input", default=None,
        help="Path to HealthcareIntel xlsx (auto-detected if omitted)",
    )
    parser.add_argument(
        "--output", default="./ir_presentations",
        help="Output directory for downloaded presentations",
    )
    parser.add_argument(
        "--logs", default="./logs",
        help="Directory for progress logs",
    )
    parser.add_argument(
        "--quarter", required=True, choices=["Q1", "Q2", "Q3", "Q4"],
        help="Quarter to collect (Q1-Q4)",
    )
    parser.add_argument(
        "--year", required=True, type=int,
        help="Year to collect (e.g. 2025)",
    )
    parser.add_argument(
        "--sector", default=None,
        help="Filter by Tier 1 sector name",
    )
    parser.add_argument(
        "--delay", type=int, default=DEFAULT_DELAY,
        help=f"Delay between companies in seconds (default: {DEFAULT_DELAY})",
    )
    parser.add_argument(
        "--headless", action="store_true", default=False,
        help="Run browser in headless mode (default: visible window)",
    )
    parser.add_argument(
        "--no-headless", dest="headless", action="store_false",
        help="Show browser window",
    )
    parser.add_argument(
        "--start", type=int, default=0,
        help="Start index (skip first N companies)",
    )
    parser.add_argument(
        "--limit", type=int, default=0,
        help="Max companies to process (0 = all)",
    )
    parser.add_argument(
        "--output-mode", default="flat",
        choices=["flat", "greenwood"],
        help="flat: ./ir_presentations/{name}_Q4_2025.pdf | "
             "greenwood: {root}/{period}/{sector}/{ticker}/{ticker}_{period}_Presentation.pdf",
    )
    parser.add_argument(
        "--output-root", default=None,
        help="Root for greenwood mode (e.g. C:/Greenwood/Research/Earnings)",
    )
    args = parser.parse_args()

    if args.output_mode == "greenwood" and not args.output_root:
        print("[ERROR] --output-root required for --output-mode greenwood")
        return

    # Compute Greenwood period tag (Q4 -> 2025FY, else 2026Q1/Q2/Q3)
    if args.quarter == "Q4":
        greenwood_period = f"{args.year}FY"
    else:
        greenwood_period = f"{args.year}{args.quarter}"

    # ---- Load companies ----
    db_path = find_db_path(args.input)
    if not db_path:
        print("[ERROR] HealthcareIntel_Database_*.xlsx not found")
        return

    companies = load_companies(db_path, sector_filter=args.sector)
    print(f"[INIT] Loaded {len(companies)} companies from {db_path}"
          + (f" (sector: {args.sector})" if args.sector else ""))

    if not companies:
        print("[ERROR] No companies found. Check sector filter or database.")
        return

    # ---- Apply start/limit ----
    if args.start > 0:
        companies = companies[args.start:]
        print(f"[INIT] Starting from index {args.start}")
    if args.limit > 0:
        companies = companies[:args.limit]
        print(f"[INIT] Limited to {len(companies)} companies")

    # ---- Setup directories ----
    output_dir = os.path.abspath(args.output)
    log_dir = os.path.abspath(args.logs)
    temp_dir = os.path.join(output_dir, "_temp_download")
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(temp_dir, exist_ok=True)
    os.makedirs(log_dir, exist_ok=True)

    q_label = quarter_label(args.quarter, args.year)
    log_path = os.path.join(log_dir, f"ir_progress_{q_label}.csv")

    # ---- Initialize SEC EDGAR client (no browser needed) ----
    try:
        from scripts.sec_edgar import EdgarClient
        edgar_client = EdgarClient(cache_dir=log_dir)
        print(f"[INIT] SEC EDGAR client ready "
              f"({len(edgar_client.get_ticker_map())} US tickers mapped)")
    except Exception as e:
        print(f"[WARN] SEC EDGAR unavailable: {e}")
        edgar_client = None

    # ---- Load progress ----
    collected_set = load_progress(log_path)
    print(f"[INIT] Progress log: {log_path} ({len(collected_set)} already collected)")

    # ---- Create driver ----
    driver = create_driver(headless=args.headless, download_dir=temp_dir)

    # ---- Collection loop ----
    total = len(companies)
    success_count = 0
    skip_count = 0
    fail_count = 0
    consecutive_fails = 0

    try:
        for idx, company in enumerate(companies):
            name = company["company_name"]
            ticker = company["ticker"]
            label = f"[{idx + 1}/{total}]"

            # Check if already collected
            if args.output_mode == "greenwood":
                sector = company.get("sector", "") or company.get("tier1", "")
                if already_collected_greenwood(args.output_root, name,
                                                greenwood_period, sector,
                                                ticker=ticker):
                    print(f"{label} {name:35s} -> SKIP (already collected)")
                    skip_count += 1
                    continue
            elif already_collected(collected_set, name, args.quarter, args.year):
                print(f"{label} {name:35s} -> SKIP (already collected)")
                skip_count += 1
                continue

            # Resolve output directory + final name per company
            per_output_dir, per_final_name = resolve_output_for_company(
                args, company, greenwood_period)

            # Periodic browser restart to avoid memory leaks
            if idx > 0 and idx % RESTART_EVERY == 0:
                print(f"\n[RESTART] Browser restart at index {idx}...")
                try:
                    driver.quit()
                except Exception:
                    pass
                time.sleep(2)
                driver = create_driver(headless=args.headless, download_dir=temp_dir)

            # Attempt collection — only retry on actual errors (not "nothing found")
            file_path = None
            file_size = 0
            method = None
            source_url = None

            try:
                file_path, file_size, method, source_url = collect_one(
                    driver, company, args.quarter, args.year,
                    temp_dir, per_output_dir, tag=label,
                    final_name=per_final_name,
                    edgar_client=edgar_client,
                )
            except Exception as e:
                print(f"  {label} ERROR: {str(e)[:80]}")
                file_path = None
                # Restart driver on unexpected errors, then retry once
                try:
                    driver.quit()
                except Exception:
                    pass
                time.sleep(3)
                driver = create_driver(
                    headless=args.headless, download_dir=temp_dir
                )
                try:
                    file_path, file_size, method, source_url = collect_one(
                        driver, company, args.quarter, args.year,
                        temp_dir, per_output_dir, tag=label,
                        final_name=per_final_name,
                    )
                except Exception:
                    pass

            # Track consecutive failures for Google rate-limit detection
            if file_path and file_size >= MIN_FILE_SIZE:
                consecutive_fails = 0
            else:
                consecutive_fails += 1
                if consecutive_fails >= 5 and consecutive_fails % 5 == 0:
                    cooldown = min(consecutive_fails * 10, 120)
                    print(f"\n  [COOLDOWN] {consecutive_fails} consecutive failures "
                          f"— Google may be rate-limiting. Pausing {cooldown}s...\n")
                    time.sleep(cooldown)

            # Record result
            key = f"{sanitize(name)}_{q_label}"
            if file_path and file_size >= MIN_FILE_SIZE:
                success_count += 1
                collected_set.add(key)
                size_kb = file_size / 1024
                print(
                    f"{label} {name:35s} -> OK ({method}, "
                    f"{size_kb:.0f} KB) {os.path.basename(file_path)}"
                )
                append_progress(log_path, {
                    "key": key,
                    "company": name,
                    "ticker": ticker,
                    "quarter": args.quarter,
                    "year": args.year,
                    "status": "OK",
                    "method": method,
                    "source_url": source_url or "",
                    "file_path": file_path,
                    "file_size": file_size,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                })
            else:
                fail_count += 1
                print(f"{label} {name:35s} -> FAIL (no presentation found)")
                append_progress(log_path, {
                    "key": key,
                    "company": name,
                    "ticker": ticker,
                    "quarter": args.quarter,
                    "year": args.year,
                    "status": "FAIL",
                    "method": "",
                    "source_url": "",
                    "file_path": "",
                    "file_size": 0,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                })

            time.sleep(args.delay)

    finally:
        try:
            driver.quit()
        except Exception:
            pass
        # Clean up temp dir
        clear_temp_dir(temp_dir)

    # ---- Summary ----
    print(f"\n{'=' * 60}")
    print(f"  IR Presentation Collection Complete")
    print(f"  Quarter: {q_label}")
    print(f"  Total:   {total}")
    print(f"  Success: {success_count}")
    print(f"  Skipped: {skip_count}")
    print(f"  Failed:  {fail_count}")
    print(f"  Output:  {output_dir}")
    print(f"  Log:     {log_path}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
