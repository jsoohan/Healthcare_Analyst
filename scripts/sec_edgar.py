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
        """Check if an 8-K is likely the earnings release for given quarter."""
        desc = (filing.get("description", "") or "").lower()
        date_str = filing.get("date", "")

        earnings_kw = ["earnings", "results", "quarter", "financial",
                        "press release", "exhibit 99"]
        if not any(kw in desc for kw in earnings_kw):
            if not desc:
                pass  # Many 8-Ks have empty descriptions — check by date
            else:
                return False

        if not date_str:
            return True

        try:
            filing_date = datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError:
            return True

        expected_months = QUARTER_MONTHS.get(quarter.upper(), ())
        report_year = year + 1 if quarter.upper() == "Q4" else year
        if filing_date.month in expected_months and filing_date.year == report_year:
            return True
        if filing_date.month in expected_months and filing_date.year == year:
            return True

        return False

    def get_filing_documents(self, cik, accession):
        """Get list of documents in a filing."""
        cik_padded = str(cik).zfill(10)
        acc_clean = accession.replace("-", "")
        url = f"{SEC_BASE}/Archives/edgar/data/{cik_padded}/{acc_clean}/index.json"

        resp = self._get(url)
        data = resp.json()

        docs = []
        directory = data.get("directory", {})
        items = directory.get("item", [])
        base_url = f"{SEC_BASE}/Archives/edgar/data/{cik_padded}/{acc_clean}/"

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

        if want_presentation:
            patterns = PRESENTATION_PATTERNS
        else:
            patterns = PRESS_RELEASE_PATTERNS

        score = 0

        is_pdf = name.endswith(".pdf")
        is_pptx = name.endswith(".pptx") or name.endswith(".ppt")
        is_htm = name.endswith(".htm") or name.endswith(".html")

        if is_pptx:
            score += 10
        elif is_pdf:
            score += 5
        elif is_htm:
            score += 1

        for pat in patterns:
            if pat.search(name) or pat.search(doc_type):
                score += 5

        if "presentation" in name:
            score += 8
        if "slide" in name:
            score += 6
        if "supplement" in name:
            score += 4
        if "press" in name and want_presentation:
            score -= 3

        try:
            size = int(doc.get("size", "0"))
            if is_pdf and size > 500000:
                score += 3
            if is_pdf and size > 2000000:
                score += 2
        except (ValueError, TypeError):
            pass

        return score

    def find_earnings_presentation(self, ticker, quarter, year):
        """Find Q4 earnings presentation PDF for a US-listed company.

        Returns dict with {url, filename, filing_date, accession} or None.
        """
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

            scored = [(self._score_document(d, want_presentation=True), d)
                       for d in docs]
            scored.sort(key=lambda x: x[0], reverse=True)

            for score, doc in scored:
                if score < 5:
                    continue
                name = doc["name"].lower()
                if name.endswith((".pdf", ".pptx", ".ppt")):
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
