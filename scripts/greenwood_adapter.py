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


def discover_sources(ticker: str, period: str, base_dir: str,
                      company_name: str = None) -> Dict:
    """Find all source files for a company in the Greenwood structure.

    Tries company_name first (new canonical layout) then ticker (legacy layout).

    Args:
        ticker: Stock ticker (e.g. 'ABBV') — used as legacy fallback.
        period: Period tag (e.g. '2025FY', '2026Q1')
        base_dir: Root of Greenwood folder (contains 2025_FY/, 2026_Q1/, ...)
        company_name: Canonical company name (e.g. 'AbbVie'). When provided,
                       tried first.

    Returns:
        {
            "transcript": path | None,
            "earnings_release": path | None,      # prefer .txt over .htm
            "earnings_release_html": path | None, # original HTML if present
            "ir_presentation": path | None,
            "filings": list[path],                # currently empty, for future use
            "sector_found": str | None,           # which sector folder it was in
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

    # Build list of candidate folder names in priority order
    candidates = []
    if company_name:
        candidates.append(_sanitize_company(company_name))
    if ticker:
        candidates.append(ticker)

    target_dir = None
    for name in candidates:
        target_dir = _find_company_dir(period_dir, name)
        if target_dir is not None:
            break

    if target_dir is None:
        return result

    result["sector_found"] = target_dir.parent.name
    actual_name = target_dir.name
    prefix = f"{actual_name}_{period}_"

    transcript_path = target_dir / f"{prefix}Transcript.txt"
    if transcript_path.exists() and transcript_path.stat().st_size >= MIN_TRANSCRIPT_SIZE:
        result["transcript"] = str(transcript_path)

    release_txt = target_dir / f"{prefix}EarningsRelease.txt"
    release_htm = target_dir / f"{prefix}EarningsRelease.htm"
    if release_txt.exists() and release_txt.stat().st_size >= MIN_RELEASE_SIZE:
        result["earnings_release"] = str(release_txt)
    if release_htm.exists():
        result["earnings_release_html"] = str(release_htm)

    for p in sorted(target_dir.iterdir()):
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


def _find_company_dir(period_dir: Path, folder_name: str) -> Optional[Path]:
    """Search all sector subdirs for a matching folder (case-sensitive first)."""
    if not period_dir.exists():
        return None
    for sector_dir in period_dir.iterdir():
        if not sector_dir.is_dir():
            continue
        candidate = sector_dir / folder_name
        if candidate.exists() and candidate.is_dir():
            return candidate
    # Case-insensitive fallback
    target = folder_name.upper()
    for sector_dir in period_dir.iterdir():
        if not sector_dir.is_dir():
            continue
        for child in sector_dir.iterdir():
            if child.is_dir() and child.name.upper() == target:
                return child
    return None


# Backward-compat alias (tests/callers may still use old name)
_find_ticker_dir = _find_company_dir


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


def _sanitize_company(name: str) -> str:
    """Company name -> safe folder/file name. Keeps spaces, ampersand, digits.
    Only replaces path-hostile chars (identical to Phase 0 collector sanitize)."""
    import re as _re
    return _re.sub(r'[\\/:*?"<>|]', '_', name).strip()


def make_output_path(company_name: str, period: str, sector: str,
                      file_type: str, ext: str, output_root: str,
                      ticker: str = None) -> Path:
    """Compute the Greenwood-style output path for a collected file.

    Files are named by company name (not ticker) to match the user's
    original Phase 0 naming convention.

    Args:
        company_name: e.g. 'AbbVie' or '10x Genomics'
        period: e.g. '2025FY' or '2026Q1' (no underscore)
        sector: DB tier1 value, e.g. 'Biopharma' or '1. Biopharma'
        file_type: 'Transcript', 'EarningsRelease', 'Presentation'
        ext: '.txt', '.pdf', '.pptx', etc. (leading dot)
        output_root: base dir, e.g. 'C:/Greenwood/Research/Earnings'
        ticker: reserved for legacy callers; ignored.

    Returns:
        Path: {output_root}/{period_dir}/{sector_dir}/{Company}/{Company}_{period}_{type}{ext}
    """
    sector_dir = sanitize_sector_name(sector) if sector else "_unmapped"
    company_dir = _sanitize_company(company_name)
    folder = Path(output_root) / period_dir_name(period) / sector_dir / company_dir
    filename = f"{company_dir}_{period}_{file_type}{ext}"
    return folder / filename


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
