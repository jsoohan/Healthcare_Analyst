#!/usr/bin/env python3
"""
SEC EDGAR batch downloader for IR presentations.
No browser, no captcha, no search engine.

Downloads earnings presentation PDFs directly from SEC 8-K filings
for all US-listed companies in the HealthcareIntel DB.

Usage:
    python scripts/edgar_batch_download.py \\
        --db C:/Greenwood/Research/Earnings/HealthcareIntel_Database_20260412.xlsx \\
        --quarter Q4 --year 2025 \\
        --output-root C:/Greenwood/Research/Earnings
"""
import argparse
import json
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.db_loader import load_companies, find_db_path, sanitize
from scripts.sec_edgar import EdgarClient
from scripts.greenwood_adapter import (
    quarter_to_period, sanitize_sector_name, period_dir_name, _sanitize_company
)

US_EXCHANGES = {"NYSE", "NASDAQ", "Nasdaq", "AMEX", "OTC", "NYSE MKT",
                "NYSE Arca", "CBOE", "BATS"}


def main():
    parser = argparse.ArgumentParser(
        description="Download IR presentations from SEC EDGAR (no browser needed)")
    parser.add_argument("--db", default=None)
    parser.add_argument("--quarter", required=True, choices=["Q1", "Q2", "Q3", "Q4"])
    parser.add_argument("--year", required=True, type=int)
    parser.add_argument("--output-root", required=True,
                        help="Greenwood root (e.g. C:/Greenwood/Research/Earnings)")
    parser.add_argument("--sector", default=None)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    db_path = find_db_path(args.db)
    if not db_path:
        print("[ERROR] DB not found")
        sys.exit(1)

    companies = load_companies(db_path, sector_filter=args.sector)
    us_companies = [c for c in companies
                     if any(ex in c.get("exchange", "")
                            for ex in US_EXCHANGES)]

    print(f"[INIT] Total companies: {len(companies)}")
    print(f"[INIT] US-listed (EDGAR eligible): {len(us_companies)}")

    if args.limit > 0:
        us_companies = us_companies[:args.limit]

    period = quarter_to_period(f"{args.quarter}_{args.year}")
    p_dir = period_dir_name(period)

    # Check already downloaded
    remaining = []
    for c in us_companies:
        company_dir = _sanitize_company(c["company_name"])
        sector_dir = sanitize_sector_name(c.get("sector", ""))
        base = Path(args.output_root) / p_dir / sector_dir / company_dir

        found = False
        for ext in [".pdf", ".pptx", ".ppt"]:
            candidate = base / f"{company_dir}_{period}_Presentation{ext}"
            if candidate.exists() and candidate.stat().st_size > 30000:
                found = True
                break
        if not found:
            remaining.append(c)

    print(f"[INIT] Already downloaded: {len(us_companies) - len(remaining)}")
    print(f"[INIT] Remaining: {len(remaining)}")
    print()

    client = EdgarClient(cache_dir="data")
    print(f"[INIT] SEC ticker map: {len(client.get_ticker_map())} tickers")
    print()

    found = 0
    not_found = 0
    errors = 0

    for idx, company in enumerate(remaining):
        name = company["company_name"]
        ticker = company["ticker"]
        sector = company.get("sector", "")
        label = f"[{idx+1}/{len(remaining)}]"

        try:
            result = client.find_earnings_presentation(
                ticker, args.quarter, args.year, verbose=args.verbose)
        except Exception as e:
            print(f"{label} {name:40s} ERROR: {str(e)[:60]}")
            errors += 1
            continue

        if not result:
            print(f"{label} {name:40s} -> not found in EDGAR")
            not_found += 1
            continue

        # Download
        ext = os.path.splitext(result["filename"])[1].lower() or ".pdf"
        company_dir = _sanitize_company(name)
        sector_dir = sanitize_sector_name(sector)
        out_dir = Path(args.output_root) / p_dir / sector_dir / company_dir
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"{company_dir}_{period}_Presentation{ext}"

        try:
            fsize = client.download(result["url"], str(out_path))
            size_kb = fsize / 1024
            print(f"{label} {name:40s} -> OK  {size_kb:7.0f}KB  "
                  f"{result['filing_date']}  {result['filename']}")
            found += 1
        except Exception as e:
            print(f"{label} {name:40s} -> download failed: {str(e)[:60]}")
            errors += 1

    print(f"\n{'='*60}")
    print(f"  SEC EDGAR Batch Download Complete")
    print(f"  Found:     {found}")
    print(f"  Not found: {not_found}")
    print(f"  Errors:    {errors}")
    print(f"  Output:    {args.output_root}/{p_dir}/")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
