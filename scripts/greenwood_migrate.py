#!/usr/bin/env python3
"""
Greenwood folder migration tool.

Reorganizes local `2025_FY/{old_sector}/{TICKER}/` into the canonical
HealthcareIntel 9-sector taxonomy, reading the master DB for mapping.

Usage (run on LOCAL machine where the files live):
  python scripts/greenwood_migrate.py \\
      --root C:\\Greenwood\\Research\\Earnings \\
      --db C:\\Greenwood\\Research\\Earnings\\HealthcareIntel_Database_20260412.xlsx \\
      --period 2025_FY \\
      --dry-run

  python scripts/greenwood_migrate.py \\
      --root C:\\Greenwood\\Research\\Earnings \\
      --db C:\\Greenwood\\Research\\Earnings\\HealthcareIntel_Database_20260412.xlsx \\
      --period 2025_FY \\
      --backup

Canonical sector mapping targets (9 TIER1 sheets):
  Biopharma, MedTech, Pharma Services, Biologics Tools & Services,
  Healthcare IT, Consumer Health, IVD, Healthcare Services, Dentistry
"""
import argparse
import json
import os
import re
import shutil
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd


TIER1_SHEETS = [
    "1. Biopharma", "2. MedTech", "3. Pharma Services",
    "4. Biologics Tools & Services", "5. Healthcare IT",
    "6. Consumer Health", "7. IVD", "8. Healthcare Services",
    "9. Dentistry",
]


def sanitize_dir_name(name: str) -> str:
    """Convert DB sector name to safe folder name.
    E.g. 'Biologics Tools & Services' -> 'Biologics_Tools_and_Services'
    """
    cleaned = name.replace("&", "and")
    cleaned = re.sub(r"[^\w\s-]", "", cleaned)
    cleaned = re.sub(r"\s+", "_", cleaned.strip())
    return cleaned


def load_db_mapping(db_path: str) -> dict:
    """Load ticker -> {tier1, sub_sector, company} from HealthcareIntel DB."""
    xl = pd.ExcelFile(db_path)
    mapping = {}
    for sheet in TIER1_SHEETS:
        if sheet not in xl.sheet_names:
            continue
        tier1 = sheet.split(". ", 1)[1]
        df = pd.read_excel(xl, sheet, header=1)
        df = df.dropna(subset=["Company", "Ticker"], how="all")
        for _, row in df.iterrows():
            ticker = str(row.get("Ticker", "")).strip()
            company = str(row.get("Company", "")).strip()
            sub = str(row.get("Sub-sector", "")).strip()
            if not ticker or ticker == "nan":
                continue
            mapping[ticker.upper()] = {
                "tier1": tier1,
                "sub_sector": sub if sub and sub != "nan" else "",
                "company": company,
            }
    return mapping


def scan_local(period_dir: Path) -> list:
    """Return list of {old_sector, ticker, path} for every ticker folder."""
    entries = []
    if not period_dir.exists():
        return entries
    for sector_dir in sorted(period_dir.iterdir()):
        if not sector_dir.is_dir():
            continue
        for ticker_dir in sorted(sector_dir.iterdir()):
            if not ticker_dir.is_dir():
                continue
            entries.append({
                "old_sector": sector_dir.name,
                "ticker": ticker_dir.name,
                "path": ticker_dir,
            })
    return entries


def plan_migration(local_entries: list, db_mapping: dict) -> tuple:
    """Build migration plan.

    Returns:
        moves: list of {ticker, from, to, tier1, sub_sector}
        unmapped: list of {ticker, from, reason}
    """
    moves = []
    unmapped = []

    for entry in local_entries:
        ticker = entry["ticker"].upper()
        old_sector = entry["old_sector"]
        src_path = entry["path"]

        if ticker not in db_mapping:
            unmapped.append({
                "ticker": ticker,
                "old_sector": old_sector,
                "from": str(src_path),
                "reason": "ticker_not_in_db",
            })
            continue

        db_entry = db_mapping[ticker]
        new_sector_dir = sanitize_dir_name(db_entry["tier1"])

        if new_sector_dir == old_sector:
            moves.append({
                "ticker": ticker,
                "from": str(src_path),
                "to": str(src_path),
                "tier1": db_entry["tier1"],
                "sub_sector": db_entry["sub_sector"],
                "op": "no_op",
            })
        else:
            target = src_path.parent.parent / new_sector_dir / ticker
            moves.append({
                "ticker": ticker,
                "from": str(src_path),
                "to": str(target),
                "tier1": db_entry["tier1"],
                "sub_sector": db_entry["sub_sector"],
                "op": "move",
            })

    return moves, unmapped


def execute_moves(moves: list, unmapped: list, period_dir: Path,
                   dry_run: bool = False, backup: bool = False) -> dict:
    """Apply migration moves. Returns summary stats."""
    stats = {"moved": 0, "no_op": 0, "failed": 0, "unmapped": len(unmapped)}

    if backup and not dry_run:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_dir = period_dir.parent / f"{period_dir.name}_backup_{ts}"
        print(f"  [BACKUP] Creating backup at {backup_dir}")
        shutil.copytree(period_dir, backup_dir)

    unmapped_dir = period_dir / "_unmapped"
    if unmapped and not dry_run:
        unmapped_dir.mkdir(exist_ok=True)

    for m in moves:
        if m["op"] == "no_op":
            stats["no_op"] += 1
            continue

        src = Path(m["from"])
        dst = Path(m["to"])

        if not src.exists():
            print(f"  [SKIP] {m['ticker']}: source missing {src}")
            stats["failed"] += 1
            continue

        if dry_run:
            print(f"  [DRY] {m['ticker']}: {src.parent.name}/{src.name} -> {dst.parent.name}/{dst.name}")
            stats["moved"] += 1
            continue

        try:
            dst.parent.mkdir(parents=True, exist_ok=True)
            if dst.exists():
                print(f"  [MERGE] {m['ticker']}: target exists, merging files")
                for f in src.iterdir():
                    target_file = dst / f.name
                    if target_file.exists():
                        target_file.unlink()
                    shutil.move(str(f), str(target_file))
                src.rmdir()
            else:
                shutil.move(str(src), str(dst))
            print(f"  [OK] {m['ticker']}: -> {dst.parent.name}/")
            stats["moved"] += 1
        except Exception as e:
            print(f"  [FAIL] {m['ticker']}: {e}")
            stats["failed"] += 1

    for u in unmapped:
        src = Path(u["from"])
        if not src.exists():
            continue
        if dry_run:
            print(f"  [DRY] UNMAPPED {u['ticker']}: -> _unmapped/")
            continue
        try:
            target = unmapped_dir / src.name
            if target.exists():
                print(f"  [SKIP] UNMAPPED {u['ticker']}: already in _unmapped")
                continue
            shutil.move(str(src), str(target))
            print(f"  [OK] UNMAPPED {u['ticker']}: -> _unmapped/")
        except Exception as e:
            print(f"  [FAIL] UNMAPPED {u['ticker']}: {e}")

    # Remove empty old sector dirs
    if not dry_run:
        for sector_dir in period_dir.iterdir():
            if not sector_dir.is_dir():
                continue
            if sector_dir.name == "_unmapped":
                continue
            try:
                if not any(sector_dir.iterdir()):
                    sector_dir.rmdir()
                    print(f"  [CLEANUP] Removed empty: {sector_dir.name}/")
            except Exception:
                pass

    return stats


def write_migration_report(root: Path, period_dir_name: str,
                            moves: list, unmapped: list, stats: dict):
    report = {
        "timestamp": datetime.now().isoformat(),
        "period": period_dir_name,
        "stats": stats,
        "moves": moves,
        "unmapped": unmapped,
    }
    report_path = root / f"migration_report_{period_dir_name}.json"
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    print(f"\n  Migration report: {report_path}")


def main():
    parser = argparse.ArgumentParser(description="Greenwood folder migration")
    parser.add_argument("--root", required=True,
                        help="Root dir (e.g. C:\\Greenwood\\Research\\Earnings)")
    parser.add_argument("--db", required=True,
                        help="HealthcareIntel_Database_*.xlsx")
    parser.add_argument("--period", default="2025_FY",
                        help="Period folder name (2025_FY, 2026_Q1, ...)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print plan without moving files")
    parser.add_argument("--backup", action="store_true",
                        help="Create backup before migration")
    args = parser.parse_args()

    root = Path(args.root)
    period_dir = root / args.period

    if not period_dir.exists():
        print(f"[ERROR] Period directory not found: {period_dir}")
        sys.exit(1)

    if not Path(args.db).exists():
        print(f"[ERROR] DB not found: {args.db}")
        sys.exit(1)

    print(f"[INIT] Root:   {root}")
    print(f"[INIT] Period: {period_dir.name}")
    print(f"[INIT] DB:     {args.db}")
    print(f"[INIT] Mode:   {'DRY-RUN' if args.dry_run else 'EXECUTE'}")
    if args.backup:
        print(f"[INIT] Backup: enabled")
    print()

    db_mapping = load_db_mapping(args.db)
    print(f"  DB companies loaded: {len(db_mapping)}")

    local = scan_local(period_dir)
    print(f"  Local tickers found: {len(local)}")

    moves, unmapped = plan_migration(local, db_mapping)

    print(f"\n[PLAN] Moves: {len(moves)}, Unmapped: {len(unmapped)}")
    print(f"  - Same-sector (no-op): {sum(1 for m in moves if m['op'] == 'no_op')}")
    print(f"  - Cross-sector moves:  {sum(1 for m in moves if m['op'] == 'move')}")

    if unmapped:
        print(f"\n[UNMAPPED] Tickers not in DB:")
        for u in unmapped:
            print(f"  - {u['ticker']} (was in {u['old_sector']})")

    print(f"\n[EXECUTE]")
    stats = execute_moves(moves, unmapped, period_dir,
                           dry_run=args.dry_run, backup=args.backup)

    print(f"\n{'=' * 60}")
    print(f"  Migration {'PLANNED' if args.dry_run else 'COMPLETE'}")
    print(f"  Moved:    {stats['moved']}")
    print(f"  No-op:    {stats['no_op']}")
    print(f"  Failed:   {stats['failed']}")
    print(f"  Unmapped: {stats['unmapped']}")
    print(f"{'=' * 60}")

    if not args.dry_run:
        write_migration_report(root, args.period, moves, unmapped, stats)


if __name__ == "__main__":
    main()
