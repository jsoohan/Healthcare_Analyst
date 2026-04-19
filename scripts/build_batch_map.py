#!/usr/bin/env python3
"""
STEP 1: Build batch map from HealthcareIntel Database.
Reads the master Excel DB and creates data/batch_map.json with
batches grouped by sub-sector slug.
"""
import pandas as pd
import json
import re
from pathlib import Path

from scripts.db_loader import find_header_row

DB_PATH = "HealthcareIntel_Database_20260410.xlsx"
OUTPUT = "data/batch_map.json"

TIER1_SHEETS = [
    "1. Biopharma", "2. MedTech", "3. Pharma Services",
    "4. Biologics Tools & Services", "5. Healthcare IT",
    "6. Consumer Health", "7. IVD", "8. Healthcare Services",
    "9. Dentistry",
]


def slugify(s: str) -> str:
    s = re.sub(r"[^\w\s-]", "", str(s).strip())
    return re.sub(r"[\s\.]+", "_", s).lower()


def load_sheet(xls, sheet: str) -> pd.DataFrame:
    header_row = find_header_row(xls, sheet)
    df = pd.read_excel(xls, sheet_name=sheet, header=header_row)
    df = df.dropna(subset=["Company", "Ticker"], how="all")
    return df


def main():
    xls = pd.ExcelFile(DB_PATH)
    batches = {}

    for sheet in TIER1_SHEETS:
        if sheet not in xls.sheet_names:
            continue
        tier1_name = sheet.split(". ", 1)[1]
        df = load_sheet(xls, sheet)

        for _, row in df.iterrows():
            company = str(row.get("Company", "")).strip()
            ticker = str(row.get("Ticker", "")).strip()
            sub = str(row.get("Sub-sector", "")).strip()

            if not company or company == "nan" or not ticker or ticker == "nan":
                continue
            if not sub or sub == "nan":
                sub = f"{tier1_name}_unclassified"

            slug = slugify(f"{sheet[0]}_{sub}")
            batches.setdefault(slug, {
                "tier1": tier1_name,
                "sub_sector": sub,
                "companies": [],
            })
            batches[slug]["companies"].append({
                "company": company,
                "ticker": ticker,
                "exchange": str(row.get("Exchange", "")).strip(),
                "mkt_cap": str(row.get("Mkt Cap (USD)", "")).strip(),
                "focus_notes": str(row.get("Focus / Notes", "")).strip(),
                "is_new": str(row.get("NEW", "")).strip() == "★",
            })

    summary = {
        "total_batches": len(batches),
        "total_companies": sum(len(b["companies"]) for b in batches.values()),
        "by_tier1": {},
    }
    for slug, b in batches.items():
        summary["by_tier1"].setdefault(b["tier1"], {"batches": 0, "companies": 0})
        summary["by_tier1"][b["tier1"]]["batches"] += 1
        summary["by_tier1"][b["tier1"]]["companies"] += len(b["companies"])

    Path("data").mkdir(exist_ok=True)
    with open(OUTPUT, "w", encoding="utf-8") as f:
        json.dump({"summary": summary, "batches": batches}, f,
                  ensure_ascii=False, indent=2)
    print(f"Wrote {OUTPUT}")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
