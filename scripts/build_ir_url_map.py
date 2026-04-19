#!/usr/bin/env python3
"""
Phase 0a: Build IR URL Map.
Discovers investor relations page URLs for each company in the HealthcareIntel DB.
Outputs data/ir_url_map.json for use by collect_ir_presentations.py.

Usage:
  python scripts/build_ir_url_map.py
  python scripts/build_ir_url_map.py --sector Biopharma
  python scripts/build_ir_url_map.py --verify
  python scripts/build_ir_url_map.py --input path/to/db.xlsx
"""
import argparse
import json
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse, quote as url_quote

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup

from scripts.db_loader import load_companies, find_db_path

OUTPUT = "data/ir_url_map.json"
DEFAULT_DELAY = 3
RESTART_EVERY = 30

IR_PATH_PATTERNS = [
    "/investors", "/investor-relations", "/ir",
    "/investor", "/investorrelations",
    "/en/investors", "/en/investor-relations",
    "/corporate/investors", "/about/investors",
]

BLOCKED_DOMAINS = [
    "google.", "youtube.", "wikipedia.", "facebook.", "twitter.", "x.com",
    "linkedin.", "reddit.", "amazon.", "yahoo.", "bing.",
    "seekingalpha.", "zacks.", "tipranks.", "morningstar.",
    "stockanalysis.", "macrotrends.", "simplywall.", "gurufocus.",
]


def create_driver(headless=True):
    options = Options()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    driver.set_page_load_timeout(15)
    driver.implicitly_wait(3)
    return driver


def is_blocked(url):
    domain = urlparse(url).netloc.lower()
    return any(b in domain for b in BLOCKED_DOMAINS)


def extract_google_url(href):
    if "/url?q=" in href:
        m = re.search(r'/url\?q=([^&]+)', href)
        if m:
            from urllib.parse import unquote
            return unquote(m.group(1))
    return href if href.startswith("http") else None


def verify_ir_url(driver, url):
    """Check if URL loads and contains investor-relations keywords."""
    try:
        driver.get(url)
        time.sleep(2)
        text = driver.page_source.lower()
        ir_keywords = ["investor", "financial result", "quarterly", "annual report",
                        "earnings", "presentation", "sec filing", "stock",
                        "shareholder"]
        hits = sum(1 for kw in ir_keywords if kw in text)
        return hits >= 2
    except (TimeoutException, WebDriverException):
        return False


def discover_via_domain_patterns(driver, company_name, ticker):
    """Try common IR URL patterns based on company domain guesses."""
    name_parts = re.sub(r'[^a-zA-Z0-9\s]', '', company_name).lower().split()
    domain_guesses = []

    if name_parts:
        domain_guesses.append(f"www.{name_parts[0]}.com")
        if len(name_parts) >= 2:
            domain_guesses.append(f"www.{''.join(name_parts[:2])}.com")
    if ticker:
        domain_guesses.append(f"www.{ticker.lower()}.com")

    for domain in domain_guesses:
        for path in IR_PATH_PATTERNS[:4]:
            url = f"https://{domain}{path}"
            try:
                driver.get(url)
                time.sleep(1)
                if driver.current_url and "404" not in driver.title.lower():
                    text = driver.page_source.lower()
                    if "investor" in text and len(text) > 5000:
                        return driver.current_url, "domain_pattern"
            except (TimeoutException, WebDriverException):
                continue
    return None, None


def discover_via_google(driver, company_name, ticker):
    """Google search for company IR page."""
    queries = [
        f'"{company_name}" investor relations',
        f'{company_name} {ticker} investor relations site:*.com',
    ]

    for query in queries:
        search_url = f"https://www.google.com/search?q={url_quote(query)}&num=10"
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
            if is_blocked(raw):
                continue

            url_lower = raw.lower()
            text = tag.get_text(strip=True).lower()

            has_ir = any(p in url_lower for p in
                         ["investor", "/ir/", "/ir.", "ir."])
            has_company = False
            name_parts = re.sub(r'[^a-zA-Z0-9]', '', company_name).lower()
            domain = urlparse(raw).netloc.lower()
            if name_parts[:4] in domain or (ticker and ticker.lower() in domain):
                has_company = True

            if has_ir and has_company:
                return raw, "google_search"
            if has_ir and "investor" in text:
                return raw, "google_search"

    return None, None


def discover_via_marketscreener(driver, company_name, ticker):
    """Search MarketScreener for company → extract website link → try IR patterns."""
    search_url = f"https://www.marketscreener.com/search/?q={url_quote(ticker or company_name)}"
    try:
        driver.get(search_url)
        time.sleep(2)
    except (TimeoutException, WebDriverException):
        return None, None

    soup = BeautifulSoup(driver.page_source, "html.parser")

    company_url = None
    for tag in soup.find_all("a", href=True):
        href = tag["href"]
        if "/quote/stock/" in href or "/equities/" in href:
            row_text = ""
            for parent in tag.parents:
                row_text = parent.get_text(" ", strip=True)
                if len(row_text) > 20:
                    break
            if ticker and ticker.upper() in row_text.upper():
                company_url = href if href.startswith("http") else f"https://www.marketscreener.com{href}"
                break

    if not company_url:
        return None, None

    try:
        driver.get(company_url)
        time.sleep(2)
    except (TimeoutException, WebDriverException):
        return None, None

    soup = BeautifulSoup(driver.page_source, "html.parser")
    for tag in soup.find_all("a", href=True):
        href = tag["href"]
        text = tag.get_text(strip=True).lower()
        if "website" in text or "official site" in text or "corporate site" in text:
            if href.startswith("http") and not is_blocked(href):
                domain = urlparse(href).netloc
                for path in IR_PATH_PATTERNS[:4]:
                    ir_url = f"https://{domain}{path}"
                    if verify_ir_url(driver, ir_url):
                        return ir_url, "marketscreener"
                break

    return None, None


def discover_ir_url(driver, company):
    """4-stage IR URL discovery for a single company."""
    name = company["company_name"]
    ticker = company["ticker"]

    url, method = discover_via_domain_patterns(driver, name, ticker)
    if url:
        return url, method

    url, method = discover_via_google(driver, name, ticker)
    if url:
        return url, method

    url, method = discover_via_marketscreener(driver, name, ticker)
    if url:
        return url, method

    return None, None


def load_existing_map(path):
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_map(ir_map, path):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(ir_map, f, ensure_ascii=False, indent=2)


def main():
    parser = argparse.ArgumentParser(description="Build IR URL Map")
    parser.add_argument("--input", default=None, help="Path to HealthcareIntel xlsx")
    parser.add_argument("--output", default=OUTPUT)
    parser.add_argument("--sector", default=None, help="Filter by Tier 1 sector")
    parser.add_argument("--verify", action="store_true", help="Re-verify existing URLs")
    parser.add_argument("--delay", type=int, default=DEFAULT_DELAY)
    parser.add_argument("--headless", action="store_true", default=True)
    parser.add_argument("--no-headless", dest="headless", action="store_false")
    parser.add_argument("--limit", type=int, default=0, help="Max companies (0=all)")
    args = parser.parse_args()

    db_path = find_db_path(args.input)
    if not db_path:
        print("[ERROR] HealthcareIntel_Database_*.xlsx not found")
        return

    companies = load_companies(db_path, sector_filter=args.sector)
    print(f"[INIT] Loaded {len(companies)} companies"
          + (f" (sector: {args.sector})" if args.sector else ""))

    ir_map = load_existing_map(args.output)
    print(f"[INIT] Existing IR URL map: {len(ir_map)} entries")

    if args.verify:
        targets = [c for c in companies if c["ticker"] in ir_map
                    and ir_map[c["ticker"]].get("ir_url")]
        print(f"[VERIFY] Re-verifying {len(targets)} existing URLs")
    else:
        targets = [c for c in companies if c["ticker"] not in ir_map
                    or not ir_map[c["ticker"]].get("ir_url")]
        print(f"[INIT] New companies to discover: {len(targets)}")

    if args.limit > 0:
        targets = targets[:args.limit]

    if not targets:
        print("[DONE] All companies already mapped!")
        return

    driver = create_driver(headless=args.headless)
    found = 0
    failed = 0

    try:
        for idx, company in enumerate(targets):
            ticker = company["ticker"]
            name = company["company_name"]
            label = f"[{idx + 1}/{len(targets)}]"

            if idx > 0 and idx % RESTART_EVERY == 0:
                print(f"\n[RESTART] Browser restart at {idx}...")
                try:
                    driver.quit()
                except:
                    pass
                time.sleep(2)
                driver = create_driver(headless=args.headless)

            if args.verify:
                existing_url = ir_map[ticker]["ir_url"]
                if verify_ir_url(driver, existing_url):
                    ir_map[ticker]["verified"] = True
                    ir_map[ticker]["verified_at"] = datetime.now(timezone.utc).isoformat()
                    print(f"{label} {name:35s} -> VERIFIED")
                else:
                    ir_map[ticker]["verified"] = False
                    print(f"{label} {name:35s} -> BROKEN, re-discovering...")
                    url, method = discover_ir_url(driver, company)
                    if url:
                        ir_map[ticker]["ir_url"] = url
                        ir_map[ticker]["discovered_via"] = method
                        ir_map[ticker]["discovered_at"] = datetime.now(timezone.utc).isoformat()
                        ir_map[ticker]["verified"] = True
                        found += 1
                    else:
                        failed += 1
            else:
                try:
                    url, method = discover_ir_url(driver, company)
                except Exception as e:
                    print(f"{label} {name:35s} -> ERROR: {str(e)[:60]}")
                    url, method = None, None

                ir_map[ticker] = {
                    "company": name,
                    "ir_url": url,
                    "domain": urlparse(url).netloc if url else None,
                    "discovered_via": method,
                    "verified": url is not None,
                    "discovered_at": datetime.now(timezone.utc).isoformat(),
                }

                if url:
                    found += 1
                    print(f"{label} {name:35s} -> {method:20s} {url[:60]}")
                else:
                    failed += 1
                    print(f"{label} {name:35s} -> NOT FOUND")

            time.sleep(args.delay)

            if (idx + 1) % 50 == 0:
                save_map(ir_map, args.output)
                print(f"\n=== CHECKPOINT: {idx + 1}/{len(targets)}, "
                      f"found={found}, failed={failed} ===\n")

    finally:
        try:
            driver.quit()
        except:
            pass

    save_map(ir_map, args.output)

    total_mapped = sum(1 for v in ir_map.values() if v.get("ir_url"))
    print(f"\n{'=' * 60}")
    print(f"  IR URL Map Complete")
    print(f"  This run: found={found}, failed={failed}")
    print(f"  Total mapped: {total_mapped}/{len(ir_map)}")
    print(f"  Saved: {os.path.abspath(args.output)}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
