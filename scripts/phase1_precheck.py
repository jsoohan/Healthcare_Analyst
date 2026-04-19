#!/usr/bin/env python3
"""
Phase 1 Precheck: 각 배치의 기업별 소스 파일 존재 여부 확인.
Phase 0 수집 도구(collect_transcripts_earnings.py, collect_ir_presentations.py)의
파일명 규칙에 맞춰 매칭.
"""
import json
import re
import os
import sys
from pathlib import Path
from typing import List, Optional

# Allow direct execution: python scripts/phase1_precheck.py
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# === 경로 설정 (환경에 맞게 조정) ===
BATCH_MAP = "data/batch_map.json"
PHASE1_DIR = Path("data/phase1")

QUARTER = os.getenv("QUARTER", "Q4_2025")  # 예: Q4_2025
TRANSCRIPT_DIR = Path(os.getenv("TRANSCRIPT_DIR", f"./transcripts_EC_{QUARTER}"))
IR_DIR = Path(os.getenv("IR_DIR", "./ir_presentations"))
FILING_DIR = Path(os.getenv("FILING_DIR", "./filings"))


# === Phase 0 도구와 동일한 sanitize 규칙 ===
def sanitize(name: str) -> str:
    """collect_transcripts_earnings.py / collect_ir_presentations.py의 sanitize와 동일"""
    return re.sub(r'[\\/:*?"<>|]', '_', name).strip()


# === 회사명 변형 생성 (fuzzy match용) ===
def name_variants(company: str) -> List[str]:
    """회사명의 가능한 변형들 (수집 도구가 약간 다른 이름을 쓸 경우 대비)"""
    base = company.strip()
    variants = set()
    variants.add(base)
    variants.add(sanitize(base))

    # 공백 정규화
    variants.add(re.sub(r"\s+", " ", base))
    variants.add(re.sub(r"\s+", "_", base))
    variants.add(re.sub(r"\s+", "", base))

    # & ↔ and
    for v in list(variants):
        variants.add(v.replace("&", "and"))
        variants.add(v.replace(" & ", " "))
        variants.add(v.replace("&", ""))

    # 접미사 제거 (Inc., Corp., Ltd., plc, Co., Ltd)
    for v in list(variants):
        stripped = re.sub(
            r"[\s,]+(Inc\.?|Corp\.?|Corporation|Ltd\.?|Limited|plc|PLC|Co\.?|SA|NV|AG|GmbH|Holdings|Group)$",
            "", v, flags=re.IGNORECASE
        )
        variants.add(stripped)

    # 소문자
    variants.update({v.lower() for v in list(variants)})

    return [v for v in variants if v and len(v) >= 2]


# === 파일 탐색 ===
def find_transcript(company: str, ticker: str) -> Optional[str]:
    """Transcript 파일 탐색.
    Phase 0 규칙: {sanitize(company_name)}_EC_{QUARTER}.txt
    """
    if not TRANSCRIPT_DIR.exists():
        return None

    suffix = f"_EC_{QUARTER}.txt"

    # 1단계: exact sanitize match
    exact = TRANSCRIPT_DIR / f"{sanitize(company)}{suffix}"
    if exact.exists() and exact.stat().st_size > 1024:
        return str(exact)

    # 2단계: variant match
    for v in name_variants(company):
        candidate = TRANSCRIPT_DIR / f"{sanitize(v)}{suffix}"
        if candidate.exists() and candidate.stat().st_size > 1024:
            return str(candidate)

    # 3단계: case-insensitive glob (전체 순회)
    for p in TRANSCRIPT_DIR.iterdir():
        if not p.name.endswith(suffix):
            continue
        if p.stat().st_size < 1024:
            continue
        stem_lower = p.name[:-len(suffix)].lower()
        for v in name_variants(company):
            if sanitize(v).lower() == stem_lower:
                return str(p)

    # 4단계: ticker 포함 파일 (수집 도구가 가끔 ticker로 저장한 경우)
    if ticker:
        for p in TRANSCRIPT_DIR.glob(f"*{ticker}*{suffix}"):
            if p.stat().st_size > 1024:
                return str(p)

    return None


def find_ir(company: str, ticker: str) -> Optional[str]:
    """IR 파일 탐색.
    Phase 0 규칙: {sanitize(company_name)}_{QUARTER}.{pdf|pptx|ppt|xlsx}
    (transcript와 달리 'EC_' 접두어 없음!)
    """
    if not IR_DIR.exists():
        return None

    extensions = [".pdf", ".pptx", ".ppt", ".xlsx"]
    suffix_base = f"_{QUARTER}"

    # 1단계: exact sanitize match
    for ext in extensions:
        exact = IR_DIR / f"{sanitize(company)}{suffix_base}{ext}"
        if exact.exists() and exact.stat().st_size > 5000:
            return str(exact)

    # 2단계: variant match
    for v in name_variants(company):
        for ext in extensions:
            candidate = IR_DIR / f"{sanitize(v)}{suffix_base}{ext}"
            if candidate.exists() and candidate.stat().st_size > 5000:
                return str(candidate)

    # 3단계: case-insensitive 전체 순회
    for p in IR_DIR.iterdir():
        if p.is_dir():  # _temp_download/ 등 skip
            continue
        if p.suffix.lower() not in extensions:
            continue
        if p.stat().st_size < 5000:
            continue
        # "{name}_{QUARTER}.ext" 패턴에서 name 추출
        m = re.match(rf"^(.+){re.escape(suffix_base)}{re.escape(p.suffix)}$",
                     p.name, re.IGNORECASE)
        if not m:
            continue
        name_part = m.group(1).lower()
        for v in name_variants(company):
            if sanitize(v).lower() == name_part:
                return str(p)

    # 4단계: ticker 포함 파일
    if ticker:
        for p in IR_DIR.glob(f"*{ticker}*{suffix_base}.*"):
            if p.suffix.lower() in extensions and p.stat().st_size > 5000:
                return str(p)

    return None


def find_filings(company: str, ticker: str) -> List[str]:
    """Filings는 여러 파일 가능. 별도 수집 도구가 어떤 규칙을 쓰든 최대한 잡음."""
    if not FILING_DIR.exists():
        return []

    found = set()
    valid_exts = [".pdf", ".json", ".xml", ".html", ".txt"]

    # Ticker 기반
    if ticker:
        for pattern in [f"{ticker}*", f"{ticker.lower()}*", f"{ticker.upper()}*"]:
            for p in FILING_DIR.glob(pattern):
                if p.is_file() and p.suffix.lower() in valid_exts and p.stat().st_size > 0:
                    found.add(str(p))

    # Company name 기반 (ticker로 못 찾은 경우)
    if not found:
        for v in name_variants(company)[:5]:
            if len(v) < 4:
                continue
            for p in FILING_DIR.glob(f"*{sanitize(v)}*"):
                if p.is_file() and p.suffix.lower() in valid_exts and p.stat().st_size > 0:
                    found.add(str(p))

    return sorted(found)


def check_sources(ticker: str, company: str) -> dict:
    return {
        "transcript": find_transcript(company, ticker),
        "ir": find_ir(company, ticker),
        "filings": find_filings(company, ticker),
    }


# === Greenwood mode: use hierarchical {period}/{sector}/{TICKER}/ layout ===

def check_sources_greenwood(ticker: str, company: str, source_root: str) -> dict:
    """Discover sources in the Greenwood folder layout."""
    from scripts.greenwood_adapter import discover_sources, quarter_to_period

    period = quarter_to_period(QUARTER)
    src = discover_sources(ticker, period, source_root)

    return {
        "transcript": src["transcript"],
        "ir": src["ir_presentation"],
        "earnings_release": src["earnings_release"],
        "filings": src["filings"],
    }


def main(batch_slug=None, source_mode="marketscreener", source_root=None):
    source_mode = source_mode or "marketscreener"

    data = json.load(open(BATCH_MAP, encoding="utf-8"))
    batches = data["batches"]
    targets = [batch_slug] if batch_slug else list(batches.keys())

    print(f"[config] QUARTER={QUARTER}")
    print(f"[config] SOURCE_MODE={source_mode}")
    if source_mode == "greenwood":
        print(f"[config] SOURCE_ROOT={source_root}")
    else:
        print(f"[config] TRANSCRIPT_DIR={TRANSCRIPT_DIR} (exists={TRANSCRIPT_DIR.exists()})")
        print(f"[config] IR_DIR={IR_DIR} (exists={IR_DIR.exists()})")
        print(f"[config] FILING_DIR={FILING_DIR} (exists={FILING_DIR.exists()})")
    print()

    if source_mode == "greenwood" and not source_root:
        print("[ERROR] --source-root required for greenwood mode")
        return

    grand_total = {"READY": 0, "SKIP": 0}

    for slug in targets:
        if slug not in batches:
            print(f"[SKIP] {slug}: not in batch map")
            continue
        b = batches[slug]
        out_dir = PHASE1_DIR / slug
        out_dir.mkdir(parents=True, exist_ok=True)
        manifest = {
            "batch_slug": slug,
            "tier1": b["tier1"],
            "sub_sector": b["sub_sector"],
            "quarter": QUARTER,
            "source_mode": source_mode,
            "companies": [],
        }
        for c in b["companies"]:
            if source_mode == "greenwood":
                src = check_sources_greenwood(c["ticker"], c["company"], source_root)
                has_any = (src["transcript"] or src["ir"]
                           or src.get("earnings_release") or src["filings"])
            else:
                src = check_sources(c["ticker"], c["company"])
                has_any = src["transcript"] or src["ir"] or src["filings"]
            status = "READY" if has_any else "SKIP"
            manifest["companies"].append({**c, "sources": src, "status": status})
            grand_total[status] += 1

        with open(out_dir / "_precheck.json", "w", encoding="utf-8") as f:
            json.dump(manifest, f, ensure_ascii=False, indent=2)
        ready = sum(1 for c in manifest["companies"] if c["status"] == "READY")
        total = len(manifest["companies"])
        print(f"[{slug}] {ready}/{total} READY  ({b['sub_sector']})")

    print(f"\n=== TOTAL: {grand_total['READY']} READY, {grand_total['SKIP']} SKIP ===")


def _cli():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("batch_slug", nargs="?", default=None)
    parser.add_argument("--source-mode", default="marketscreener",
                        choices=["marketscreener", "greenwood"],
                        help="Layout convention for source files")
    parser.add_argument("--source-root", default=None,
                        help="Root directory for greenwood mode "
                             "(e.g. C:/Greenwood/Research/Earnings)")
    args = parser.parse_args()
    main(batch_slug=args.batch_slug,
         source_mode=args.source_mode,
         source_root=args.source_root)


if __name__ == "__main__":
    _cli()
