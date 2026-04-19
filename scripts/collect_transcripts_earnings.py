#!/usr/bin/env python3
"""
Phase 0b: MarketScreener Earnings Call Transcript Collector.
Selenium-based. Reads company list from HealthcareIntel DB (or CSV fallback)
and downloads earnings call transcripts for a given quarter.

Usage:
  # Interactive mode (prompts for quarter)
  python scripts/collect_transcripts_earnings.py

  # Non-interactive
  python scripts/collect_transcripts_earnings.py --quarter Q4 --year 2025

  # With filters
  python scripts/collect_transcripts_earnings.py --quarter Q4 --year 2025 --sector Biopharma --limit 10

Requires local execution: Chrome browser + manual MarketScreener login on first run.
"""
import argparse
import csv
import glob
import json
import os
import re
import shutil
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import quote as url_quote

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup

from scripts.db_loader import load_companies, find_db_path, sanitize

BASE_URL = "https://www.marketscreener.com"
DEFAULT_DELAY = 5
RESTART_EVERY = 20
MAX_RETRY = 2

EARNINGS_KEYWORDS = [
    "earnings call", "results call", "quarterly results",
    "q4 results", "q3 results", "q2 results", "q1 results",
    "annual results", "full year results", "fy results",
    "half year results", "h1 results", "h2 results",
]

EXCLUDE_KEYWORDS = [
    "j.p. morgan", "jpmorgan", "investor day", "capital markets day",
    "goldman sachs", "morgan stanley", "barclays", "cowen", "leerink",
    "bernstein", "evercore", "jefferies", "citi annual", "td cowen",
    "bofa", "wells fargo", "ubs",
]

VALIDATION_KEYWORDS = [
    "revenue", "earnings", "quarter", "fiscal", "growth",
    "results", "guidance", "margin", "operating", "outlook",
]


# ══════════════════════════════════════════════════════════════
# Browser + Login
# ══════════════════════════════════════════════════════════════

def create_driver(headless=False):
    options = Options()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    driver.set_page_load_timeout(30)
    driver.set_script_timeout(15)
    driver.implicitly_wait(5)
    try:
        driver.execute_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )
    except Exception:
        pass
    return driver


def safe_get(driver, url, retries=2):
    for attempt in range(retries):
        try:
            driver.get(url)
            return True
        except TimeoutException:
            print(f"  [WARN] page load timeout (try {attempt + 1}/{retries}): {url}")
            try:
                driver.execute_script("window.stop();")
                time.sleep(1)
            except Exception:
                pass
            if attempt < retries - 1:
                time.sleep(2)
    return False


def save_cookies(driver, path):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(driver.get_cookies(), f)


def load_cookies(driver, path):
    if not os.path.exists(path):
        return False
    try:
        if not safe_get(driver, BASE_URL):
            return False
        time.sleep(2)
        with open(path) as f:
            cookies = json.load(f)
        for c in cookies:
            c.pop("sameSite", None)
            try:
                driver.add_cookie(c)
            except Exception:
                pass
        driver.refresh()
        time.sleep(2)
        return is_logged_in(driver)
    except Exception:
        return False


def is_logged_in(driver):
    try:
        if "login" in driver.current_url.lower():
            return False
        soup = BeautifulSoup(driver.page_source, "html.parser")
        header = soup.find("header")
        if header:
            ht = str(header)
            if 'id="user_data_modal"' in ht or 'user-icon' in ht:
                return True
            if 'href="/login/' in ht:
                return False
        return True
    except Exception:
        return True


def ensure_logged_in(driver, cookie_path):
    if not is_logged_in(driver):
        return do_login(driver, cookie_path)
    return True


def do_login(driver, cookie_path):
    print("[LOGIN] Attempting cookie-based login...")
    if load_cookies(driver, cookie_path):
        print("[LOGIN] Cookie login successful!")
        return True

    print("[LOGIN] Manual login required")
    if not safe_get(driver, f"{BASE_URL}/login/"):
        print("[LOGIN] Login page failed to load, retrying...")
        driver.set_page_load_timeout(60)
        if not safe_get(driver, f"{BASE_URL}/login/"):
            print("[LOGIN] Cannot reach login page")
            driver.set_page_load_timeout(30)
            return False
        driver.set_page_load_timeout(30)
    time.sleep(2)

    print("\n" + "=" * 60)
    print("  Log in to MarketScreener in the browser, then press Enter")
    print("=" * 60)
    input("  >>> ")

    if is_logged_in(driver):
        save_cookies(driver, cookie_path)
        print("[LOGIN] Success! Cookies saved.\n")
        return True

    input("  Not yet complete. Press Enter again >>> ")
    if is_logged_in(driver):
        save_cookies(driver, cookie_path)
        return True

    print("[LOGIN] Failed")
    return False


def kill_browsers():
    if sys.platform.startswith("win"):
        subprocess.run("taskkill /F /IM chromedriver.exe >nul 2>&1", shell=True)
        subprocess.run("taskkill /F /IM chrome.exe >nul 2>&1", shell=True)
    else:
        subprocess.run("pkill -f chromedriver 2>/dev/null", shell=True)
        subprocess.run("pkill -f 'Google Chrome' 2>/dev/null", shell=True)


# ══════════════════════════════════════════════════════════════
# Search + Navigation
# ══════════════════════════════════════════════════════════════

def go_to_company_page(driver, company):
    search_term = company.get("search_term", company["company_name"])
    ticker = company["ticker"].upper()

    if not safe_get(driver, BASE_URL):
        return None
    time.sleep(1.5)

    try:
        wait = WebDriverWait(driver, 10)
        sb = wait.until(EC.presence_of_element_located(
            (By.CSS_SELECTOR,
             "#autocomplete, input[name='q'], input.js-search, "
             "input[placeholder='Search...'], input[placeholder*='Search']")
        ))
        sb.clear()
        sb.send_keys(search_term)
        time.sleep(0.5)
        sb.send_keys(Keys.RETURN)
        time.sleep(2.5)
    except TimeoutException:
        safe_get(driver, f"{BASE_URL}/search/?q={url_quote(search_term)}")
        time.sleep(2.5)

    soup = BeautifulSoup(driver.page_source, "html.parser")
    target_url = None

    for row in soup.select("table tbody tr"):
        cells = row.find_all(["td", "th"])
        row_text = " ".join(c.get_text(strip=True) for c in cells)
        if ticker in row_text.upper():
            link = row.find("a", href=True)
            if link and ("/quote/stock/" in link["href"] or "/equities/" in link["href"]):
                href = link["href"]
                target_url = href if href.startswith("http") else BASE_URL + href
                break

    if not target_url:
        for tag in soup.find_all("a", href=True):
            href = tag.get("href", "")
            if "/quote/stock/" not in href and "/equities/" not in href:
                continue
            parent_text = ""
            for parent in tag.parents:
                parent_text = parent.get_text(" ", strip=True)
                if len(parent_text) > 20:
                    break
            if ticker in parent_text.upper():
                target_url = href if href.startswith("http") else BASE_URL + href
                break

    if not target_url and search_term != company["company_name"]:
        safe_get(driver, f"{BASE_URL}/search/?q={url_quote(company['company_name'])}")
        time.sleep(2.5)
        soup = BeautifulSoup(driver.page_source, "html.parser")
        for tag in soup.find_all("a", href=True):
            href = tag.get("href", "")
            if "/quote/stock/" not in href:
                continue
            parent_text = ""
            for parent in tag.parents:
                parent_text = parent.get_text(" ", strip=True)
                if len(parent_text) > 20:
                    break
            name_lower = company["company_name"].lower().split()[0]
            if ticker in parent_text.upper() or name_lower in parent_text.lower():
                target_url = href if href.startswith("http") else BASE_URL + href
                break

    if not target_url:
        return None
    if not safe_get(driver, target_url):
        return None
    time.sleep(2)
    return driver.current_url


def navigate_to_transcripts(driver):
    wait = WebDriverWait(driver, 10)
    try:
        news_tab = wait.until(EC.presence_of_element_located(
            (By.XPATH, "//nav//a[normalize-space()='News'] | //ul//a[normalize-space()='News']")
        ))
        ActionChains(driver).move_to_element(news_tab).perform()
        time.sleep(1)
        t_link = wait.until(EC.element_to_be_clickable(
            (By.XPATH, "//a[normalize-space()='Transcripts']")
        ))
        t_link.click()
        time.sleep(2)
        if "transcript" in driver.page_source.lower():
            return True
    except Exception:
        pass

    base = driver.current_url.split("/news")[0].rstrip("/")
    for path in ["/news-call-transcripts/", "/news/transcripts/"]:
        try:
            safe_get(driver, base + path)
            time.sleep(2)
            if "transcript" in driver.page_source.lower():
                return True
        except Exception:
            pass
    return False


def find_earnings_transcript(driver, quarter, year):
    """Find earnings call transcript link for given Q/year. Returns scored list."""
    soup = BeautifulSoup(driver.page_source, "html.parser")
    results = []

    q_label = quarter.upper()
    q_num = q_label[1]
    fy_year = str(year)
    next_year = str(year + 1)

    for tag in soup.find_all("a", href=True):
        title = tag.get_text(strip=True)
        href = tag["href"]
        if not title or len(title) < 10:
            continue

        title_lower = title.lower()

        if any(ex in title_lower for ex in EXCLUDE_KEYWORDS):
            continue

        is_earnings = any(kw in title_lower for kw in EARNINGS_KEYWORDS)
        if not is_earnings:
            continue

        direct_match = (f"q{q_num}" in title_lower and fy_year in title)
        year_match = (fy_year in title or next_year in title)

        if direct_match:
            full_url = href if href.startswith("http") else BASE_URL + href
            results.insert(0, {"title": title, "url": full_url, "score": 10})
        elif year_match:
            full_url = href if href.startswith("http") else BASE_URL + href
            results.append({"title": title, "url": full_url, "score": 5})

    results.sort(key=lambda x: x["score"], reverse=True)
    return results


# ══════════════════════════════════════════════════════════════
# Extract + Save
# ══════════════════════════════════════════════════════════════

def extract_transcript_text(driver, url):
    try:
        driver.get(url)
    except TimeoutException:
        print("  [!] page load timeout, extracting current state")
        try:
            driver.execute_script("window.stop()")
        except Exception:
            pass
    time.sleep(3)

    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "article, .article-body, .transcript, main")
            )
        )
    except TimeoutException:
        pass

    soup = BeautifulSoup(driver.page_source, "html.parser")
    for tag in soup(["script", "style", "nav", "header", "footer", "aside", "iframe"]):
        tag.decompose()

    for sel in ["article", ".article-content", ".article-body",
                ".transcript-content", ".news-body", "main"]:
        el = soup.select_one(sel)
        if el and len(el.get_text(strip=True)) > 300:
            return re.sub(r"\n{3,}", "\n\n", el.get_text(separator="\n", strip=True))

    return re.sub(r"\n{3,}", "\n\n", soup.get_text(separator="\n", strip=True))


def save_transcript(output_dir, company_name, event_tag, title, url, text):
    filename = f"{sanitize(company_name)}_{event_tag}.txt"
    filepath = os.path.join(output_dir, filename)
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(f"Company : {company_name}\n")
        f.write(f"Title   : {title}\n")
        f.write(f"Source  : {url}\n")
        f.write(f"Saved   : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("=" * 80 + "\n\n")
        f.write(text)
    return filepath


def validate_file(filepath):
    if not os.path.exists(filepath):
        return "FAIL"
    if os.path.getsize(filepath) < 1024:
        return "FAIL"
    with open(filepath, "r", encoding="utf-8") as f:
        content = f.read()
    if len(content) < 2000:
        return "WARN_highlight"
    found = sum(1 for kw in VALIDATION_KEYWORDS if kw in content.lower())
    return "PASS" if found >= 1 else "WARN_no_kw"


def already_collected(output_dir, company_name, event_tag):
    filename = f"{sanitize(company_name)}_{event_tag}.txt"
    filepath = os.path.join(output_dir, filename)
    return os.path.exists(filepath) and os.path.getsize(filepath) > 1024


def load_progress(log_dir):
    p = os.path.join(log_dir, "progress.csv")
    done = set()
    if os.path.exists(p):
        with open(p, "r", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                done.add(row["ticker"])
    return done


def append_progress(log_dir, row):
    p = os.path.join(log_dir, "progress.csv")
    exists = os.path.exists(p)
    fields = ["ticker", "company_name", "sector", "found", "file_size_kb",
              "event_title", "event_date", "url", "note", "collected_at"]
    with open(p, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        if not exists:
            w.writeheader()
        w.writerow(row)


# ══════════════════════════════════════════════════════════════
# Input loading (DB priority, CSV fallback)
# ══════════════════════════════════════════════════════════════

def load_companies_from_csv(csv_path):
    """Fallback: load from input_companies.csv (Format A or B)."""
    with open(csv_path, "r", encoding="utf-8-sig") as f:
        raw = list(csv.DictReader(f))
    if not raw:
        return []

    us_exchanges = ["NYSE", "NASDAQ", "Nasdaq"]
    if "Company Name" in raw[0]:
        companies = []
        for r in raw:
            name = r.get("Company Name", "").strip()
            ticker = r.get("Ticker", "").strip()
            exchange = r.get("Exchange", "").strip()
            sector = r.get("Sector", "").strip()
            if not name:
                continue
            if any(ex in exchange for ex in us_exchanges) and ticker.isalpha():
                search_term = ticker
            else:
                search_term = name
            companies.append({
                "ticker": ticker, "company_name": name,
                "search_term": search_term, "sector": sector,
            })
        return companies
    elif "ticker" in raw[0]:
        return list(raw)
    return []


def resolve_input(args):
    """Try DB first, fall back to CSV."""
    db_path = find_db_path(args.input)
    if db_path:
        print(f"  [INIT] Loading from DB: {db_path}")
        return load_companies(db_path, sector_filter=args.sector)

    csv_path = args.csv or "input_companies.csv"
    if os.path.exists(csv_path):
        print(f"  [INIT] Loading from CSV: {csv_path}")
        return load_companies_from_csv(csv_path)

    print(f"  [!] No input found. Tried DB ({args.input}) and CSV ({csv_path})")
    return []


def prompt_quarter():
    print()
    print("  Enter target quarter (e.g. Q4 2025, Q1 2026):")
    raw = input("  >>> ").strip()
    m = re.match(r'(Q[1-4])\s*(\d{4})', raw, re.IGNORECASE)
    if not m:
        print(f"  [!] Invalid format: '{raw}'")
        return None, None
    return m.group(1).upper(), int(m.group(2))


# ══════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════

def main():
    print()
    print("=" * 60)
    print("  MarketScreener Earnings Call Transcript Collector")
    print("=" * 60)

    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=None, help="Path to HealthcareIntel xlsx")
    parser.add_argument("--csv", default=None, help="Fallback CSV path")
    parser.add_argument("--quarter", default=None, choices=["Q1", "Q2", "Q3", "Q4"])
    parser.add_argument("--year", type=int, default=None)
    parser.add_argument("--sector", default=None)
    parser.add_argument("--delay", type=int, default=DEFAULT_DELAY)
    parser.add_argument("--headless", action="store_true")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--start", type=int, default=0)
    args = parser.parse_args()

    if args.quarter is None or args.year is None:
        q, y = prompt_quarter()
        if q is None:
            return
        args.quarter = q
        args.year = y

    quarter = args.quarter.upper()
    year = args.year
    event_tag = f"EC_{quarter}_{year}"

    print(f"\n  [OK] Target: {quarter} {year} Earnings Call")
    print(f"  [OK] Filename: {{company}}_{event_tag}.txt")

    output_dir = f"./transcripts_{event_tag}"
    log_dir = f"./logs_{event_tag}"
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(log_dir, exist_ok=True)
    cookie_path = os.path.join(log_dir, "ms_cookies.json")

    if not os.path.exists(cookie_path):
        candidates = [
            "./logs/ms_cookies.json",
            "../logs/ms_cookies.json",
        ] + glob.glob("./logs_*/ms_cookies.json")
        for candidate in candidates:
            if os.path.exists(candidate) and candidate != cookie_path:
                shutil.copy2(candidate, cookie_path)
                print(f"  [OK] Reused cookies: {candidate} -> {cookie_path}")
                break

    companies = resolve_input(args)
    if not companies:
        print("  [ABORT] No companies loaded")
        return
    print(f"  [INIT] Companies: {len(companies)}"
          + (f" (sector: {args.sector})" if args.sector else ""))

    if args.start > 0:
        companies = companies[args.start:]
        print(f"  [INIT] Start index: {args.start}")

    done = load_progress(log_dir)
    remaining = [c for c in companies if c["ticker"] not in done]
    print(f"  [INIT] Already done: {len(done)}, remaining: {len(remaining)}")

    if args.limit > 0:
        remaining = remaining[:args.limit]
        print(f"  [INIT] Limited to: {args.limit}")

    if not remaining:
        print("  [DONE] All companies collected!")
        return

    print()
    driver = create_driver(headless=args.headless)
    summary = {"found": 0, "skip": 0}

    try:
        if not do_login(driver, cookie_path):
            print("[ABORT] Login failed")
            return

        for idx, company in enumerate(remaining):
            ticker = company["ticker"]
            name = company["company_name"]
            num = len(done) + idx + 1
            tag_str = f"[{num}/{len(companies)}]"

            if already_collected(output_dir, name, event_tag):
                print(f"{tag_str} {name:35s} -> SKIP (already collected)")
                summary["skip"] += 1
                continue

            if idx > 0 and idx % RESTART_EVERY == 0:
                print(f"\n[RESTART] Browser restart...")
                try:
                    driver.quit()
                except Exception:
                    pass
                kill_browsers()
                time.sleep(5)
                driver = create_driver(headless=args.headless)
                if not do_login(driver, cookie_path):
                    print("[ABORT] Login failed after restart")
                    break

            if not ensure_logged_in(driver, cookie_path):
                print(f"{tag_str} {name:35s} -> SKIP (login failed)")
                summary["skip"] += 1
                continue

            result = {
                "ticker": ticker, "company_name": name,
                "sector": company.get("sector", ""), "found": "N",
                "file_size_kb": 0, "event_title": "", "event_date": "",
                "url": "", "note": "", "collected_at": datetime.now().isoformat(),
            }

            for attempt in range(1, MAX_RETRY + 1):
                try:
                    company_url = go_to_company_page(driver, company)
                    if not company_url:
                        result["note"] = "search_failed"
                        break

                    if not navigate_to_transcripts(driver):
                        result["note"] = "no_transcripts_tab"
                        break

                    links = find_earnings_transcript(driver, quarter, year)
                    if not links:
                        result["note"] = f"no_earnings_{quarter}_{year}"
                        break

                    link = links[0]
                    text = extract_transcript_text(driver, link["url"])
                    if len(text) < 200:
                        result["note"] = "too_short"
                        break

                    fp = save_transcript(output_dir, name, event_tag,
                                          link["title"], link["url"], text)
                    size_kb = round(os.path.getsize(fp) / 1024, 1)
                    validation = validate_file(fp)

                    result.update({
                        "found": "Y", "file_size_kb": size_kb,
                        "event_title": link["title"],
                        "url": link["url"], "note": validation,
                    })
                    break

                except Exception as e:
                    err = str(e)[:80]
                    print(f"  [!] Error (try {attempt}/{MAX_RETRY}): {err}")
                    if attempt < MAX_RETRY:
                        try:
                            driver.quit()
                        except Exception:
                            pass
                        kill_browsers()
                        time.sleep(5)
                        try:
                            driver = create_driver(headless=args.headless)
                            do_login(driver, cookie_path)
                        except Exception:
                            result["note"] = "restart_failed"
                            break
                    else:
                        result["note"] = f"error:{err}"

            append_progress(log_dir, result)
            if result["found"] == "Y":
                summary["found"] += 1
                print(f"{tag_str} {name:35s} -> Y  "
                      f"{result['file_size_kb']:6.1f}KB  {result['note']}")
            else:
                summary["skip"] += 1
                print(f"{tag_str} {name:35s} -> N  ({result['note']})")

            time.sleep(args.delay)

            if (idx + 1) % 50 == 0:
                print(f"\n=== CHECKPOINT: {idx + 1}/{len(remaining)}, "
                      f"found={summary['found']}, skip={summary['skip']} ===\n")

    finally:
        try:
            driver.quit()
        except Exception:
            pass

    print(f"\n{'=' * 60}")
    print(f"  {quarter} {year} Earnings Call Collection Complete")
    print(f"  Collected: {summary['found']}")
    print(f"  Skipped:   {summary['skip']}")
    print(f"  Output:    {os.path.abspath(output_dir)}")
    print(f"  Logs:      {os.path.abspath(log_dir)}/progress.csv")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
