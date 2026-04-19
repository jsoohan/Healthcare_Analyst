#!/usr/bin/env python3
"""
Shared DB loader for HealthcareIntel pipeline.
Single source of truth: reads HealthcareIntel_Database_*.xlsx
and provides a unified company list to all Phase 0/1/2 scripts.
"""
import csv
import glob
import re
from pathlib import Path

import pandas as pd

TIER1_SHEETS = [
    "1. Biopharma", "2. MedTech", "3. Pharma Services",
    "4. Biologics Tools & Services", "5. Healthcare IT",
    "6. Consumer Health", "7. IVD", "8. Healthcare Services",
    "9. Dentistry",
]

US_EXCHANGES = {"NYSE", "NASDAQ", "Nasdaq", "AMEX", "OTC"}


def find_header_row(xls, sheet, max_scan=6):
    """Detect the row index that contains 'Company' and 'Ticker' column headers.

    Real HealthcareIntel DB has 3 intro rows before the header; test fixtures
    have 1. This handles both by scanning.
    """
    preview = pd.read_excel(xls, sheet_name=sheet, header=None, nrows=max_scan)
    for idx, row in preview.iterrows():
        values = [str(v).strip() for v in row.values]
        if "Company" in values and "Ticker" in values:
            return idx
    return 1


def find_db_path(hint=None):
    """Auto-detect the HealthcareIntel DB xlsx in CWD or given path."""
    if hint and Path(hint).exists():
        return hint
    candidates = sorted(glob.glob("HealthcareIntel_Database_*.xlsx"), reverse=True)
    if candidates:
        return candidates[0]
    return None


def load_companies(xlsx_path=None, sector_filter=None):
    """Load all companies from the 9 TIER1 sheets.

    Returns list of dicts with keys:
        company_name, ticker, exchange, sector, sub_sector,
        mkt_cap, focus_notes, is_new, search_term
    """
    if xlsx_path is None:
        xlsx_path = find_db_path()
    if xlsx_path is None:
        raise FileNotFoundError("HealthcareIntel_Database_*.xlsx not found")

    xls = pd.ExcelFile(xlsx_path)
    companies = []

    for sheet in TIER1_SHEETS:
        if sheet not in xls.sheet_names:
            continue
        tier1_name = sheet.split(". ", 1)[1]

        if sector_filter and tier1_name.lower() != sector_filter.lower():
            continue

        header_row = find_header_row(xls, sheet)
        df = pd.read_excel(xls, sheet_name=sheet, header=header_row)
        df = df.dropna(subset=["Company", "Ticker"], how="all")

        for _, row in df.iterrows():
            company = str(row.get("Company", "")).strip()
            ticker = str(row.get("Ticker", "")).strip()
            sub = str(row.get("Sub-sector", "")).strip()
            exchange = str(row.get("Exchange", "")).strip()

            if not company or company == "nan" or not ticker or ticker == "nan":
                continue
            if not sub or sub == "nan":
                sub = f"{tier1_name}_unclassified"

            if exchange in US_EXCHANGES and ticker.isalpha():
                search_term = ticker
            else:
                search_term = company

            companies.append({
                "company_name": company,
                "ticker": ticker,
                "exchange": exchange,
                "sector": tier1_name,
                "sub_sector": sub,
                "mkt_cap": str(row.get("Mkt Cap (USD)", "")).strip(),
                "focus_notes": str(row.get("Focus / Notes", "")).strip(),
                "is_new": str(row.get("NEW", "")).strip() == "\u2605",
                "search_term": search_term,
            })

    return companies


def to_input_csv(companies, csv_path):
    """Export to input_companies.csv (Format A) for legacy compatibility."""
    fields = ["Company Name", "Exchange", "Ticker", "Sector"]
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for c in companies:
            w.writerow({
                "Company Name": c["company_name"],
                "Exchange": c["exchange"],
                "Ticker": c["ticker"],
                "Sector": c["sector"],
            })
    return csv_path


def sanitize(name):
    """Shared sanitize — identical to Phase 0 collection tools."""
    return re.sub(r'[\\/:*?"<>|]', '_', name).strip()
