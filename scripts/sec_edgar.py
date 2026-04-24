#!/usr/bin/env python3
"""
SEC EDGAR integration for downloading IR presentations.

8-K filings contain earnings presentations as Exhibit 99.2 (PDF/PPTX).
No Selenium needed — pure HTTP, no rate limit issues (10 req/sec allowed).

Usage:
    from scripts.sec_edgar import EdgarClient

    client = EdgarClient()
    result = client.find_earnings_presentation("ABBV", "Q4", 2025)
    if result:
        client.download(result["url"], "/path/to/output.pdf")
"""
import json
import os
import re
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin

import requests

SEC_BASE = "https://www.sec.gov"
EDGAR_DATA = "https://data.sec.gov"
TICKER_MAP_URL = "https://www.sec.gov/files/company_tickers.json"
TICKER_MAP_CACHE = "data/sec_ticker_map.json"

HEADERS = {
    "User-Agent": "HealthcareIntel-Research/1.0 (healthcare-research@greenwood.io)",
    "Accept-Encoding": "gzip, deflate",
    "Accept": "application/json, text/html, */*",
}

QUARTER_MONTHS = {
    "Q1": (4, 5, 6),
    "Q2": (7, 8, 9),
    "Q3": (10, 11, 12),
    "Q4": (1, 2, 3, 4),
}

PRESENTATION_PATTERNS = [
    re.compile(r"ex[\-_]?99[\-_.]?2", re.I),
    re.compile(r"earnings.*presentation", re.I),
    re.compile(r"investor.*presentation", re.I),
    re.compile(r"quarterly.*presentation", re.I),
    re.compile(r"results.*presentation", re.I),
    re.compile(r"supplemental", re.I),
    re.compile(r"slide", re.I),
]

PRESS_RELEASE_PATTERNS = [
    re.compile(r"ex[\-_]?99[\-_.]?1", re.I),
    re.compile(r"earnings.*release", re.I),
    re.compile(r"press.*release", re.I),
]


class EdgarClient:
    def __init__(self, cache_dir="data"):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.cache_dir = Path(cache_dir)
        self._ticker_map = None
        self._rate_ts = 0

    def _rate_limit(self):
        """SEC allows 10 requests/second. We do 5/sec to be safe."""
        elapsed = time.time() - self._rate_ts
        if elapsed < 0.2:
            time.sleep(0.2 - elapsed)
        self._rate_ts = time.time()

    def _get(self, url):
        self._rate_limit()
        resp = self.session.get(url, timeout=15)
        resp.raise_for_status()
        return resp

    def get_ticker_map(self):
        """Load ticker -> CIK mapping. Cached to disk."""
        if self._ticker_map:
            return self._ticker_map

        cache_path = self.cache_dir / "sec_ticker_map.json"
        if cache_path.exists():
            age_hours = (time.time() - cache_path.stat().st_mtime) / 3600
            if age_hours < 168:  # 7 days
                with open(cache_path, encoding="utf-8") as f:
                    self._ticker_map = json.load(f)
                return self._ticker_map

        resp = self._get(TICKER_MAP_URL)
        raw = resp.json()

        ticker_map = {}
        for entry in raw.values():
            ticker = str(entry.get("ticker", "")).upper()
            cik = str(entry.get("cik_str", ""))
            name = entry.get("title", "")
            if ticker and cik:
                ticker_map[ticker] = {"cik": cik, "name": name}

        cache_path.parent.mkdir(parents=True, exist_ok=True)
        with open(cache_path, "w", encoding="utf-8") as f:
            json.dump(ticker_map, f, indent=2)

        self._ticker_map = ticker_map
        return ticker_map

    def ticker_to_cik(self, ticker):
        """Get CIK for a ticker. Returns None if not found."""
        m = self.get_ticker_map()
        entry = m.get(ticker.upper())
        return entry["cik"] if entry else None

    def get_recent_filings(self, cik, form_type="8-K", max_results=20):
        """Get recent filings for a CIK."""
        cik_padded = str(cik).zfill(10)
        url = f"{EDGAR_DATA}/submissions/CIK{cik_padded}.json"

        resp = self._get(url)
        data = resp.json()

        recent = data.get("filings", {}).get("recent", {})
        forms = recent.get("form", [])
        dates = recent.get("filingDate", [])
        accessions = recent.get("accessionNumber", [])
        descriptions = recent.get("primaryDocDescription", [])
        primary_docs = recent.get("primaryDocument", [])

        results = []
        for i, form in enumerate(forms):
            if form != form_type:
                continue
            results.append({
                "form": form,
                "date": dates[i] if i < len(dates) else "",
                "accession": accessions[i] if i < len(accessions) else "",
                "description": descriptions[i] if i < len(descriptions) else "",
                "primary_doc": primary_docs[i] if i < len(primary_docs) else "",
            })
            if len(results) >= max_results:
                break

        return results

    def _is_earnings_8k(self, filing, quarter, year):
        """Check if an 8-K is likely the earnings release for given quarter.

        8-K descriptions are usually empty or just '8-K' — cannot rely on
        description to classify. We filter purely by filing date window
        and let document scoring find the earnings presentation.
        """
        date_str = filing.get("date", "")
        if not date_str:
            return True

        try:
            filing_date = datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError:
            return True

        expected_months = QUARTER_MONTHS.get(quarter.upper(), ())
        if quarter.upper() == "Q4":
            # Q4 earnings released Jan-Apr of FOLLOWING year (e.g. Q4 2025 -> early 2026)
            if filing_date.year == year + 1 and filing_date.month in expected_months:
                return True
            # Also accept late-year filings (some companies release Q4 in Dec)
            if filing_date.year == year and filing_date.month in (12, 11):
                return True
        else:
            if filing_date.year == year and filing_date.month in expected_months:
                return True

        return False

    def get_filing_documents(self, cik, accession):
        """Get list of documents in a filing.

        NOTE: Archives URLs use CIK WITHOUT leading zeros (unlike
        submissions API which needs 10-digit padded CIK).
        """
        cik_nopad = str(cik).lstrip("0") or str(cik)
        acc_clean = accession.replace("-", "")
        url = f"{SEC_BASE}/Archives/edgar/data/{cik_nopad}/{acc_clean}/index.json"

        resp = self._get(url)
        data = resp.json()

        docs = []
        directory = data.get("directory", {})
        items = directory.get("item", [])
        base_url = f"{SEC_BASE}/Archives/edgar/data/{cik_nopad}/{acc_clean}/"

        for item in items:
            name = item.get("name", "")
            doc_type = item.get("type", "")
            size = item.get("size", "")
            docs.append({
                "name": name,
                "type": doc_type,
                "size": size,
                "url": base_url + name,
            })

        return docs

    def _score_document(self, doc, want_presentation=True):
        """Score how likely a document is the earnings presentation."""
        name = doc.get("name", "").lower()
        doc_type = doc.get("type", "").lower()

        patterns = PRESENTATION_PATTERNS if want_presentation else PRESS_RELEASE_PATTERNS

        score = 0

        is_pdf = name.endswith(".pdf")
        is_pptx = name.endswith(".pptx") or name.endswith(".ppt")
        is_htm = name.endswith(".htm") or name.endswith(".html")

        # Base extension score — any PDF/PPTX from an earnings 8-K is a candidate
        if is_pptx:
            score += 8
        elif is_pdf:
            score += 4
        elif is_htm:
            score += 1

        # Name pattern bonuses
        for pat in patterns:
            if pat.search(name) or pat.search(doc_type):
                score += 5

        if "presentation" in name:
            score += 8
        if "slide" in name or "slides" in name:
            score += 6
        if "supplement" in name:
            score += 4
        if "earnings" in name:
            score += 3
        if "quarterly" in name or "quarter" in name:
            score += 2
        if "press" in name and want_presentation:
            score -= 3  # Press release is Step 0c, not presentation
        if "release" in name and want_presentation:
            score -= 2

        # Size heuristic (earnings presentations are typically 500KB - 10MB)
        try:
            size = int(doc.get("size", "0"))
            if is_pdf and 500_000 <= size <= 20_000_000:
                score += 2
            if is_pdf and 2_000_000 <= size <= 10_000_000:
                score += 2  # Sweet spot for earnings presentations
        except (ValueError, TypeError):
            pass

        return score

    def find_earnings_presentation(self, ticker, quarter, year, verbose=False):
        """Find Q4 earnings presentation PDF for a US-listed company.

        Returns dict with {url, filename, filing_date, accession} or None.
        """
        cik = self.ticker_to_cik(ticker)
        if not cik:
            if verbose:
                print(f"    [edgar] {ticker}: CIK not found")
            return None

        filings = self.get_recent_filings(cik, "8-K", max_results=40)
        earnings_filings = [f for f in filings
                             if self._is_earnings_8k(f, quarter, year)]

        if verbose:
            print(f"    [edgar] {ticker}: CIK={cik}, "
                  f"{len(filings)} recent 8-Ks, "
                  f"{len(earnings_filings)} in Q{quarter[-1]} window")

        if not earnings_filings:
            earnings_filings = filings[:10]

        for filing in earnings_filings[:10]:
            try:
                docs = self.get_filing_documents(cik, filing["accession"])
            except Exception as e:
                if verbose:
                    print(f"    [edgar] Failed to get docs for "
                          f"{filing['accession']}: {e}")
                continue

            scored = [(self._score_document(d, want_presentation=True), d)
                       for d in docs]
            scored.sort(key=lambda x: x[0], reverse=True)

            if verbose:
                print(f"    [edgar] {filing['date']} {filing['accession']}:")
                for s, d in scored[:8]:
                    if s > 0:
                        print(f"      {s:3d}  {d['name']:50s}  {d.get('size','')}")

            # Pass 1: PDF/PPTX with good score
            for score, doc in scored:
                if score < 3:
                    break
                name = doc["name"].lower()
                if name.endswith((".pdf", ".pptx", ".ppt")):
                    return {
                        "url": doc["url"],
                        "filename": doc["name"],
                        "filing_date": filing["date"],
                        "accession": filing["accession"],
                        "score": score,
                    }

            # Pass 2: ANY PDF in this filing (likely presentation even if
            # filename doesn't match patterns — it's an earnings 8-K)
            for score, doc in scored:
                name = doc["name"].lower()
                if name.endswith((".pdf", ".pptx", ".ppt")) and score > 0:
                    return {
                        "url": doc["url"],
                        "filename": doc["name"],
                        "filing_date": filing["date"],
                        "accession": filing["accession"],
                        "score": score,
                    }

            # Pass 3: HTML presentation (some companies only file HTML)
            for score, doc in scored:
                if score < 5:
                    break
                name = doc["name"].lower()
                if name.endswith((".htm", ".html")):
                    if "presentation" in name or "slide" in name or "supplement" in name:
                        return {
                            "url": doc["url"],
                            "filename": doc["name"],
                            "filing_date": filing["date"],
                            "accession": filing["accession"],
                            "score": score,
                        }

        return None

    def find_earnings_release(self, ticker, quarter, year):
        """Find Q4 earnings press release (Exhibit 99.1)."""
        cik = self.ticker_to_cik(ticker)
        if not cik:
            return None

        filings = self.get_recent_filings(cik, "8-K", max_results=30)
        earnings_filings = [f for f in filings
                             if self._is_earnings_8k(f, quarter, year)]
        if not earnings_filings:
            earnings_filings = filings[:10]

        for filing in earnings_filings[:5]:
            try:
                docs = self.get_filing_documents(cik, filing["accession"])
            except Exception:
                continue

            scored = [(self._score_document(d, want_presentation=False), d)
                       for d in docs]
            scored.sort(key=lambda x: x[0], reverse=True)

            for score, doc in scored:
                if score < 3:
                    continue
                name = doc["name"].lower()
                if name.endswith((".htm", ".html", ".pdf", ".txt")):
                    return {
                        "url": doc["url"],
                        "filename": doc["name"],
                        "filing_date": filing["date"],
                        "accession": filing["accession"],
                        "score": score,
                    }

        return None

    def download(self, url, output_path):
        """Download a file from EDGAR."""
        resp = self._get(url)
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "wb") as f:
            f.write(resp.content)
        return len(resp.content)
