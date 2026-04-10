#!/usr/bin/env python3
"""
Quality Gate: Phase 1 & Phase 2 출력물 품질 검증.
실행지시서 v4 섹션 2.8 / 3.5 기준.
"""
import json
import re
import sys
from pathlib import Path

BATCH_MAP = "data/batch_map.json"
PHASE1_DIR = Path("data/phase1")
PHASE2_DIR = Path("data/phase2")

# ========================================================
# 1. Phase 1 Validation
# ========================================================

PHASE1_REQUIRED_FIELDS = ["ticker", "company", "tier1", "sub_sector", "sources_used"]


def validate_phase1_company(data: dict) -> list:
    """Phase 1 기업별 JSON 검증. 에러 목록 반환 (빈 리스트 = 통과)."""
    errors = []

    for field in PHASE1_REQUIRED_FIELDS:
        if field not in data or not data[field]:
            errors.append(f"missing or empty field: {field}")

    # sources_used에 실제 경로가 있는지
    sources = data.get("sources_used", [])
    if isinstance(sources, list) and len(sources) == 0:
        errors.append("sources_used is empty")

    # key_products 또는 financials 중 하나 이상 비-null
    has_products = False
    products = data.get("key_products", [])
    if isinstance(products, list) and len(products) > 0:
        has_products = True

    has_financials = False
    financials = data.get("financials", {})
    if isinstance(financials, dict):
        for section in financials.values():
            if isinstance(section, dict):
                for metric_key, metric_val in section.items():
                    if isinstance(metric_val, dict):
                        for k, v in metric_val.items():
                            if k != "source" and v is not None and v != "N/A":
                                has_financials = True
                                break
                    elif metric_key != "source" and metric_val is not None and metric_val != "N/A":
                        has_financials = True
                    if has_financials:
                        break
            if has_financials:
                break

    if not has_products and not has_financials:
        errors.append("neither key_products nor financials has non-null data")

    return errors


def validate_phase1_batch(batch_slug: str) -> dict:
    """Phase 1 배치 전체 검증."""
    batch_dir = PHASE1_DIR / batch_slug
    result = {"batch_slug": batch_slug, "companies": {}, "pass": 0, "warn": 0, "fail": 0}

    if not batch_dir.exists():
        result["fail"] += 1
        result["error"] = f"directory not found: {batch_dir}"
        return result

    for p in sorted(batch_dir.glob("*.json")):
        if p.name.startswith("_"):
            continue
        try:
            data = json.load(open(p, encoding="utf-8"))
            errors = validate_phase1_company(data)
            ticker = data.get("ticker", p.stem)
            result["companies"][ticker] = errors
            if errors:
                result["fail"] += 1
            else:
                result["pass"] += 1
        except Exception as e:
            result["companies"][p.stem] = [f"JSON load error: {e}"]
            result["fail"] += 1

    return result


# ========================================================
# 2. Phase 2 Validation
# ========================================================

def validate_phase2_review(filepath: Path) -> list:
    """Phase 2 _review.json 검증. 에러 목록 반환."""
    errors = []

    try:
        data = json.load(open(filepath, encoding="utf-8"))
    except Exception as e:
        return [f"JSON load error: {e}"]

    # sector_dynamics 3개 이상
    dynamics = data.get("sector_dynamics", [])
    if not isinstance(dynamics, list) or len(dynamics) < 3:
        errors.append(f"sector_dynamics: expected >= 3, got {len(dynamics) if isinstance(dynamics, list) else 0}")

    # exec_summary_input.one_liner 비어있지 않음
    exec_summary = data.get("exec_summary_input", {})
    one_liner = exec_summary.get("one_liner", "") if isinstance(exec_summary, dict) else ""
    if not one_liner or not one_liner.strip():
        errors.append("exec_summary_input.one_liner is empty")

    # companies dict 존재
    companies = data.get("companies", {})
    if not isinstance(companies, dict) or len(companies) == 0:
        errors.append("companies dict is empty")

    # enriched_highlights 각 항목에 숫자 포함
    for ticker, company_data in companies.items():
        if not isinstance(company_data, dict):
            errors.append(f"{ticker}: company data is not a dict")
            continue
        highlights = company_data.get("enriched_highlights", [])
        if not isinstance(highlights, list) or len(highlights) == 0:
            errors.append(f"{ticker}: enriched_highlights is empty")
            continue
        for i, bullet in enumerate(highlights):
            if isinstance(bullet, str) and not re.search(r"\d", bullet):
                errors.append(f"{ticker}: enriched_highlights[{i}] has no numeric value")

    return errors


def check_phase2_completeness(batch_map_path: str) -> dict:
    """Phase 2 출력물 완결성 확인 — batch_map의 모든 배치에 _review.json 존재하는지."""
    try:
        data = json.load(open(batch_map_path, encoding="utf-8"))
    except Exception as e:
        return {"error": str(e)}

    batches = data.get("batches", {})
    total = len(batches)
    found = 0
    missing = []

    for slug in batches:
        review_path = PHASE2_DIR / f"{slug}_review.json"
        if review_path.exists():
            found += 1
        else:
            missing.append(slug)

    return {
        "total_batches": total,
        "reviews_found": found,
        "reviews_missing": len(missing),
        "missing_slugs": missing,
        "coverage_pct": round(found / total * 100, 1) if total > 0 else 0,
    }


# ========================================================
# 3. Main
# ========================================================

def main(phase: str = None):
    if phase is None:
        phase = sys.argv[1] if len(sys.argv) > 1 else "all"

    total_pass = 0
    total_warn = 0
    total_fail = 0

    # Phase 1 validation
    if phase in ("all", "phase1", "1"):
        print("=== Phase 1 Quality Gate ===\n")
        if PHASE1_DIR.exists():
            for batch_dir in sorted(PHASE1_DIR.iterdir()):
                if not batch_dir.is_dir():
                    continue
                result = validate_phase1_batch(batch_dir.name)
                status = "PASS" if result["fail"] == 0 else "FAIL"
                total_pass += result["pass"]
                total_fail += result["fail"]
                company_count = result["pass"] + result["fail"]
                if company_count > 0:
                    print(f"  [{status}] {batch_dir.name}: {result['pass']}/{company_count} companies passed")
                    if result["fail"] > 0:
                        for ticker, errors in result["companies"].items():
                            if errors:
                                print(f"        {ticker}: {'; '.join(errors)}")
        else:
            print("  Phase 1 directory not found. Skipping.\n")

        print(f"\n  Phase 1 Total: {total_pass} PASS, {total_fail} FAIL\n")

    # Phase 2 validation
    p2_pass = 0
    p2_warn = 0
    p2_fail = 0

    if phase in ("all", "phase2", "2"):
        print("=== Phase 2 Quality Gate ===\n")

        # Completeness check
        if Path(BATCH_MAP).exists():
            completeness = check_phase2_completeness(BATCH_MAP)
            if "error" not in completeness:
                print(f"  Coverage: {completeness['reviews_found']}/{completeness['total_batches']} "
                      f"({completeness['coverage_pct']}%)")
                if completeness["missing_slugs"]:
                    print(f"  Missing: {', '.join(completeness['missing_slugs'][:10])}"
                          f"{'...' if len(completeness['missing_slugs']) > 10 else ''}")
                print()

        # Per-review validation
        if PHASE2_DIR.exists():
            for review_path in sorted(PHASE2_DIR.glob("*_review.json")):
                errors = validate_phase2_review(review_path)
                if not errors:
                    p2_pass += 1
                    print(f"  [PASS] {review_path.name}")
                else:
                    has_critical = any("empty" in e or "missing" in e or "not a dict" in e for e in errors)
                    if has_critical:
                        p2_fail += 1
                        print(f"  [FAIL] {review_path.name}")
                    else:
                        p2_warn += 1
                        print(f"  [WARN] {review_path.name}")
                    for e in errors:
                        print(f"        {e}")
        else:
            print("  Phase 2 directory not found. Skipping.\n")

        print(f"\n  Phase 2 Total: {p2_pass} PASS, {p2_warn} WARN, {p2_fail} FAIL\n")

    # Grand total
    grand_pass = total_pass + p2_pass
    grand_fail = total_fail + p2_fail
    print(f"=== GRAND TOTAL: {grand_pass} PASS, {p2_warn} WARN, {grand_fail} FAIL ===")

    return grand_fail == 0


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
