#!/usr/bin/env python3
"""
Rename existing ticker-based Greenwood folders/files to company-name-based.

Converts:
    2025_FY/Biopharma/ABBV/ABBV_2025FY_Transcript.txt
to:
    2025_FY/Biopharma/AbbVie/AbbVie_2025FY_Transcript.txt

Uses HealthcareIntel DB for ticker -> company_name mapping.

Usage:
    python scripts/rename_ticker_to_company.py \\
        --root C:/Greenwood/Research/Earnings \\
        --db C:/Greenwood/Research/Earnings/HealthcareIntel_Database_20260412.xlsx \\
        --period 2025_FY \\
        --dry-run

    python scripts/rename_ticker_to_company.py \\
        --root C:/Greenwood/Research/Earnings \\
        --db C:/Greenwood/Research/Earnings/HealthcareIntel_Database_20260412.xlsx \\
        --period 2025_FY
"""
import argparse
import re
import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.db_loader import load_companies, find_db_path
from scripts.greenwood_adapter import _sanitize_company


def build_ticker_index(companies):
    by_ticker = {}
    for c in companies:
        t = c["ticker"].strip().upper()
        if t:
            by_ticker[t] = c
    return by_ticker


def rename_in_period(period_dir: Path, by_ticker: dict, dry_run: bool) -> dict:
    stats = {"renamed": 0, "already_renamed": 0, "unmapped": 0, "failed": 0}
    period_name = period_dir.name.replace("_", "")  # 2025_FY -> 2025FY

    for sector_dir in sorted(period_dir.iterdir()):
        if not sector_dir.is_dir() or sector_dir.name.startswith("_"):
            continue
        for entry in sorted(sector_dir.iterdir()):
            if not entry.is_dir():
                continue

            folder_name = entry.name
            ticker_upper = folder_name.upper()

            # If folder isn't a ticker we know, skip (already a company name or unmapped)
            if ticker_upper not in by_ticker:
                # Could be already-renamed (contains spaces etc.) — skip silently
                if any(c in folder_name for c in " &.-()"):
                    stats["already_renamed"] += 1
                    continue
                stats["unmapped"] += 1
                print(f"  [SKIP] {sector_dir.name}/{folder_name} — not in DB")
                continue

            company = by_ticker[ticker_upper]
            company_name = company["company_name"]
            new_folder_name = _sanitize_company(company_name)

            if new_folder_name == folder_name:
                # No rename needed
                stats["already_renamed"] += 1
                continue

            new_folder = sector_dir / new_folder_name
            if new_folder.exists():
                print(f"  [SKIP] {sector_dir.name}/{folder_name} -> target exists: {new_folder.name}")
                stats["failed"] += 1
                continue

            print(f"  [RENAME] {sector_dir.name}/{folder_name} -> {new_folder_name}")

            if dry_run:
                stats["renamed"] += 1
                continue

            # Rename files inside first
            for f in entry.iterdir():
                if not f.is_file():
                    continue
                name = f.name
                # Rename pattern: {ticker}_{period}_... -> {company}_{period}_...
                pattern = f"{folder_name}_{period_name}_"
                if name.startswith(pattern):
                    new_name = name.replace(pattern,
                                              f"{new_folder_name}_{period_name}_",
                                              1)
                    f.rename(f.parent / new_name)

            # Then rename the folder
            try:
                entry.rename(new_folder)
                stats["renamed"] += 1
            except Exception as e:
                print(f"    [ERROR] {e}")
                stats["failed"] += 1

    return stats


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", required=True)
    parser.add_argument("--db", default=None)
    parser.add_argument("--period", default="2025_FY",
                        help="Period folder name (2025_FY, 2026_Q1, ...)")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    db_path = find_db_path(args.db)
    if not db_path:
        print("[ERROR] DB not found.")
        sys.exit(1)

    root = Path(args.root)
    period_dir = root / args.period
    if not period_dir.exists():
        print(f"[ERROR] Period directory not found: {period_dir}")
        sys.exit(1)

    print(f"[INIT] Root:   {root}")
    print(f"[INIT] Period: {period_dir.name}")
    print(f"[INIT] DB:     {db_path}")
    print(f"[INIT] Mode:   {'DRY-RUN' if args.dry_run else 'EXECUTE'}")
    print()

    companies = load_companies(db_path)
    by_ticker = build_ticker_index(companies)
    print(f"  DB tickers loaded: {len(by_ticker)}\n")

    stats = rename_in_period(period_dir, by_ticker, args.dry_run)

    print(f"\n{'=' * 60}")
    print(f"  Rename {'PLANNED' if args.dry_run else 'COMPLETE'}")
    print(f"  Renamed:        {stats['renamed']}")
    print(f"  Already OK:     {stats['already_renamed']}")
    print(f"  Unmapped:       {stats['unmapped']}")
    print(f"  Failed:         {stats['failed']}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
