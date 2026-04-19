#!/usr/bin/env python3
"""
Migrate flat-mode transcripts into the Greenwood hierarchical layout.

Use this when you collected transcripts without --output-mode greenwood and
want to move them into {root}/{period_dir}/{sector_dir}/{TICKER}/ so the
Phase 1 precheck (greenwood mode) can find them.

Usage:
    python scripts/migrate_flat_to_greenwood.py \\
        --flat-dir ./transcripts_EC_Q4_2025 \\
        --log-dir ./logs_EC_Q4_2025 \\
        --root C:/Greenwood/Research/Earnings \\
        --db C:/Greenwood/Research/Earnings/HealthcareIntel_Database_20260412.xlsx \\
        --quarter Q4 --year 2025 --dry-run

The flat progress CSV (logs_EC_Q4_2025/progress.csv) is used to map
company_name -> ticker. The DB provides ticker -> sector.

Each flat file has a 5-line metadata header + '=' separator that is stripped
before writing the Greenwood .txt (matches user's existing layout). Header
values are preserved as a sidecar .meta.json.
"""
import argparse
import csv
import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.db_loader import load_companies, find_db_path, sanitize
from scripts.greenwood_adapter import make_output_path


def split_header(text):
    """Return (metadata_dict, body) by splitting on the first '=' separator line."""
    lines = text.split("\n")
    sep_idx = None
    for i, line in enumerate(lines[:10]):
        if line.strip() and set(line.strip()) == {"="}:
            sep_idx = i
            break
    if sep_idx is None:
        return {}, text  # no header found

    meta = {}
    for line in lines[:sep_idx]:
        if ":" in line:
            k, _, v = line.partition(":")
            meta[k.strip()] = v.strip()
    body = "\n".join(lines[sep_idx + 1:]).lstrip("\n")
    return meta, body


def load_flat_index(log_dir):
    """Read flat progress.csv and return {company_name: row} for successful collections."""
    p = os.path.join(log_dir, "progress.csv")
    if not os.path.exists(p):
        return {}
    idx = {}
    with open(p, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row.get("found") != "Y":
                continue
            key = row.get("company_name", "").strip()
            if key:
                idx[key] = row
    return idx


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--flat-dir", required=True,
                        help="Existing flat output dir (e.g. ./transcripts_EC_Q4_2025)")
    parser.add_argument("--log-dir", required=True,
                        help="Flat log dir (progress.csv lives here)")
    parser.add_argument("--root", required=True,
                        help="Greenwood root (e.g. C:/Greenwood/Research/Earnings)")
    parser.add_argument("--db", default=None,
                        help="HealthcareIntel xlsx (auto-detect if omitted)")
    parser.add_argument("--quarter", required=True, choices=["Q1", "Q2", "Q3", "Q4"])
    parser.add_argument("--year", required=True, type=int)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    db_path = find_db_path(args.db)
    if not db_path:
        print("[ERROR] DB not found. Pass --db explicitly.")
        sys.exit(1)

    companies = load_companies(db_path)
    by_ticker = {c["ticker"].upper(): c for c in companies}
    by_name = {c["company_name"]: c for c in companies}

    flat_index = load_flat_index(args.log_dir)
    print(f"[INIT] Flat success entries: {len(flat_index)}")

    # Greenwood period
    period = f"{args.year}FY" if args.quarter == "Q4" else f"{args.year}{args.quarter}"

    flat_dir = Path(args.flat_dir)
    if not flat_dir.exists():
        print(f"[ERROR] Flat dir not found: {flat_dir}")
        sys.exit(1)

    event_tag = f"EC_{args.quarter}_{args.year}"
    moved = 0
    skipped = 0
    unmapped = []

    for company_name, row in flat_index.items():
        ticker = row.get("ticker", "").strip().upper()
        if not ticker or ticker not in by_ticker:
            # Try by company name as fallback
            company = by_name.get(company_name)
            if not company:
                unmapped.append({"company_name": company_name, "ticker": ticker,
                                  "reason": "not_in_db"})
                continue
            ticker = company["ticker"].upper()

        company = by_ticker.get(ticker) or by_name.get(company_name)
        sector = company.get("sector", "") if company else ""

        flat_path = flat_dir / f"{sanitize(company_name)}_{event_tag}.txt"
        if not flat_path.exists():
            unmapped.append({"company_name": company_name, "ticker": ticker,
                              "reason": f"flat_file_missing: {flat_path.name}"})
            continue

        target = make_output_path(ticker, period, sector, "Transcript", ".txt",
                                    args.root)
        meta_target = target.with_suffix(".meta.json")

        if target.exists() and target.stat().st_size > 1024:
            print(f"  [SKIP] {ticker}: greenwood file already exists")
            skipped += 1
            continue

        print(f"  [MOVE] {ticker:10s} {sector:30s} <- {flat_path.name}")
        if args.dry_run:
            moved += 1
            continue

        text = flat_path.read_text(encoding="utf-8")
        meta, body = split_header(text)

        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(body, encoding="utf-8")
        meta_sidecar = {
            "company": meta.get("Company", company_name),
            "ticker": ticker,
            "title": meta.get("Title", ""),
            "source": meta.get("Source", ""),
            "saved": meta.get("Saved", datetime.now().strftime('%Y-%m-%d %H:%M:%S')),
            "period": period,
            "sector": sector,
            "migrated_from": str(flat_path),
        }
        with open(meta_target, "w", encoding="utf-8") as f:
            json.dump(meta_sidecar, f, ensure_ascii=False, indent=2)

        moved += 1

    print(f"\n{'=' * 60}")
    print(f"  Migration {'PLANNED' if args.dry_run else 'COMPLETE'}")
    print(f"  Moved:    {moved}")
    print(f"  Skipped:  {skipped} (already in greenwood)")
    print(f"  Unmapped: {len(unmapped)}")
    if unmapped:
        print(f"\n  Unmapped details:")
        for u in unmapped[:20]:
            print(f"    - {u['company_name']:35s} [{u['ticker']}] {u['reason']}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
