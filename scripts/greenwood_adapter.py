#!/usr/bin/env python3
"""
Greenwood folder adapter.
Provides source file discovery for the hierarchical Greenwood layout:

  {base_dir}/
    2025_FY/
      {sector}/
        {TICKER}/
          {TICKER}_2025FY_Transcript.txt
          {TICKER}_2025FY_EarningsRelease.htm  (original)
          {TICKER}_2025FY_EarningsRelease.txt  (extracted)
          {TICKER}_2025FY_Presentation_{name}.pdf

Supports periods: 2025FY, 2026Q1, 2026Q2, 2026Q3, 2026FY, ...
"""
import os
import re
from pathlib import Path
from typing import Dict, List, Optional

MIN_TRANSCRIPT_SIZE = 1024
MIN_IR_SIZE = 5000
MIN_RELEASE_SIZE = 512

IR_EXTENSIONS = [".pdf", ".pptx", ".ppt", ".xlsx"]


def quarter_to_period(quarter_env: str) -> str:
    """Convert Phase 1 QUARTER env (Q4_2025) to Greenwood period tag.

    Q4_2025 -> 2025FY (Q4 earnings = full year report)
    Q1_2026 -> 2026Q1
    Q2_2026 -> 2026Q2
    Q3_2026 -> 2026Q3
    """
    m = re.match(r"(Q[1-4])_(\d{4})", quarter_env.strip(), re.IGNORECASE)
    if not m:
        return quarter_env
    q = m.group(1).upper()
    year = m.group(2)
    if q == "Q4":
        return f"{year}FY"
    return f"{year}{q}"


def period_dir_name(period: str) -> str:
    """Greenwood uses underscore in folder names: 2025_FY, 2026_Q1."""
    m = re.match(r"(\d{4})(FY|Q[1-4])", period)
    if m:
        return f"{m.group(1)}_{m.group(2)}"
    return period


def discover_sources(ticker: str, period: str, base_dir: str) -> Dict:
    """Find all source files for a ticker in the Greenwood structure.

    Args:
        ticker: Stock ticker (e.g. 'ABBV')
        period: Period tag (e.g. '2025FY', '2026Q1')
        base_dir: Root of Greenwood folder (contains 2025_FY/, 2026_Q1/, ...)

    Returns:
        {
            "transcript": path | None,
            "earnings_release": path | None,      # prefer .txt over .htm
            "earnings_release_html": path | None, # original HTML if present
            "ir_presentation": path | None,
            "filings": list[path],                # currently empty, for future use
            "sector_found": str | None,           # which sector folder ticker was in
        }
    """
    base = Path(base_dir)
    period_dir = base / period_dir_name(period)

    result = {
        "transcript": None,
        "earnings_release": None,
        "earnings_release_html": None,
        "ir_presentation": None,
        "filings": [],
        "sector_found": None,
    }

    if not period_dir.exists():
        return result

    ticker_dir = _find_ticker_dir(period_dir, ticker)
    if ticker_dir is None:
        return result

    result["sector_found"] = ticker_dir.parent.name

    # Use the actual directory name (preserves case for case-insensitive match)
    actual_ticker = ticker_dir.name
    prefix = f"{actual_ticker}_{period}_"

    transcript_path = ticker_dir / f"{prefix}Transcript.txt"
    if transcript_path.exists() and transcript_path.stat().st_size >= MIN_TRANSCRIPT_SIZE:
        result["transcript"] = str(transcript_path)

    release_txt = ticker_dir / f"{prefix}EarningsRelease.txt"
    release_htm = ticker_dir / f"{prefix}EarningsRelease.htm"
    if release_txt.exists() and release_txt.stat().st_size >= MIN_RELEASE_SIZE:
        result["earnings_release"] = str(release_txt)
    if release_htm.exists():
        result["earnings_release_html"] = str(release_htm)

    for p in sorted(ticker_dir.iterdir()):
        if not p.is_file():
            continue
        name = p.name
        if not name.startswith(f"{prefix}Presentation"):
            continue
        if p.suffix.lower() not in IR_EXTENSIONS:
            continue
        if p.stat().st_size < MIN_IR_SIZE:
            continue
        result["ir_presentation"] = str(p)
        break

    return result


def _find_ticker_dir(period_dir: Path, ticker: str) -> Optional[Path]:
    """Search all sector subdirs for a matching ticker folder (case-sensitive)."""
    if not period_dir.exists():
        return None
    for sector_dir in period_dir.iterdir():
        if not sector_dir.is_dir():
            continue
        candidate = sector_dir / ticker
        if candidate.exists() and candidate.is_dir():
            return candidate
    # Case-insensitive fallback
    upper_ticker = ticker.upper()
    for sector_dir in period_dir.iterdir():
        if not sector_dir.is_dir():
            continue
        for child in sector_dir.iterdir():
            if child.is_dir() and child.name.upper() == upper_ticker:
                return child
    return None


def list_all_tickers(base_dir: str, period: str) -> List[Dict]:
    """List every {sector, ticker} pair present under the given period."""
    base = Path(base_dir) / period_dir_name(period)
    entries = []
    if not base.exists():
        return entries
    for sector_dir in sorted(base.iterdir()):
        if not sector_dir.is_dir():
            continue
        for ticker_dir in sorted(sector_dir.iterdir()):
            if not ticker_dir.is_dir():
                continue
            entries.append({
                "sector": sector_dir.name,
                "ticker": ticker_dir.name,
                "path": str(ticker_dir),
            })
    return entries


def sanitize_sector_name(name: str) -> str:
    """Convert DB tier1 value (e.g. '1. Biologics Tools & Services')
    to safe folder name (e.g. 'Biologics_Tools_and_Services').
    Shared with greenwood_migrate.py sanitize_dir_name.
    """
    if not name:
        return ""
    cleaned = re.sub(r"^\s*Sector\s+\d+\s*[:\.\-]?\s*", "", name, flags=re.IGNORECASE)
    cleaned = re.sub(r"^\s*\d+\s*[\.\-]?\s*", "", cleaned)
    cleaned = cleaned.replace("&", "and")
    cleaned = re.sub(r"[^\w\s-]", "", cleaned)
    cleaned = re.sub(r"\s+", "_", cleaned.strip())
    return cleaned


def make_output_path(ticker: str, period: str, sector: str, file_type: str,
                      ext: str, output_root: str) -> Path:
    """Compute the Greenwood-style output path for a collected file.

    Args:
        ticker: e.g. 'ABBV'
        period: e.g. '2025FY' or '2026Q1' (no underscore)
        sector: DB tier1 value, e.g. 'Biopharma' or '1. Biopharma'
        file_type: 'Transcript', 'EarningsRelease', 'Presentation'
        ext: '.txt', '.pdf', '.pptx', etc. (leading dot)
        output_root: base dir, e.g. 'C:/Greenwood/Research/Earnings'

    Returns:
        Path: {output_root}/{period_dir}/{sector_dir}/{ticker}/{ticker}_{period}_{type}{ext}
    """
    sector_dir = sanitize_sector_name(sector) if sector else "_unmapped"
    ticker_dir = Path(output_root) / period_dir_name(period) / sector_dir / ticker
    filename = f"{ticker}_{period}_{file_type}{ext}"
    return ticker_dir / filename


def check_sources_bundle(source: Dict) -> str:
    """Classify a discover_sources() result as READY / PARTIAL / SKIP."""
    has_transcript = source.get("transcript") is not None
    has_release = source.get("earnings_release") is not None
    has_ir = source.get("ir_presentation") is not None

    if has_transcript and (has_release or has_ir):
        return "READY"
    if has_transcript or has_release or has_ir:
        return "PARTIAL"
    return "SKIP"
