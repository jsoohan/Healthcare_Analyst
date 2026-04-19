#!/usr/bin/env python3
"""
Quality Gate: Phase 1 & Phase 2 출력물 품질 검증.
실행지시서 v4 섹션 2.8 / 3.5 기준.

LLM-as-judge 모드:
  python scripts/quality_gate.py --judge --batch 1_oncology
  JUDGE_MODEL=claude-haiku-4-5 python scripts/quality_gate.py --judge
"""
import argparse
import json
import os
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
# 3. LLM-as-Judge (Adaptive cross-family)
# ========================================================

JUDGE_PROMPT = """당신은 healthcare equity research 보고서 품질 심사관입니다.
아래 Phase 2 분석 출력물을 4가지 기준으로 0~10점 채점해주세요.

## 채점 기준
1. **actionability** (0-10): 투자자 관점에서 실행 가능한 시사점이 있는가? 매수/매도 판단에 도움되는 구체적 insight가 있는가?
2. **factual_grounding** (0-10): 제시된 수치/주장이 Phase 1 데이터에서 도출 가능한가? 환각(hallucination) 없는가?
3. **korean_quality** (0-10): 한국어 품질 — 자연스러운 문장, 회사명/제품명 영어 보존, 재무용어 일관성
4. **so_what** (0-10): 각 enriched_highlights에 "이것이 왜 중요한가" 해석이 동반되는가? 단순 수치 나열이 아닌가?

## 출력 포맷 (JSON만, 설명 없이)
```json
{{
  "actionability": 8,
  "factual_grounding": 7,
  "korean_quality": 9,
  "so_what": 7,
  "overall": 7.75,
  "strengths": ["강점 1", "강점 2"],
  "weaknesses": ["약점 1"],
  "critical_issues": []
}}
```

## Phase 1 입력 데이터 (Reference)
{phase1_json}

## Phase 2 분석 출력 (평가 대상)
{phase2_json}
"""


def select_judge_model(llm_used):
    """Adaptive cross-family judge: opposite family from the model that produced the output."""
    override = os.getenv("JUDGE_MODEL")
    if override:
        return override, "gemini" if "gemini" in override.lower() else "anthropic"

    if llm_used and "gemini" in llm_used.lower():
        return "claude-haiku-4-5", "anthropic"
    else:
        return "gemini-2.5-flash", "gemini"


def call_judge(provider, model, prompt):
    if provider == "gemini":
        from google import genai
        from google.genai import types
        client = genai.Client()
        response = client.models.generate_content(
            model=model,
            contents=prompt,
            config=types.GenerateContentConfig(
                temperature=0.2,
                max_output_tokens=2000,
                response_mime_type="application/json",
            ),
        )
        return response.text
    elif provider == "anthropic":
        import anthropic
        client = anthropic.Anthropic()
        response = client.messages.create(
            model=model,
            max_tokens=2000,
            temperature=0.2,
            messages=[{"role": "user", "content": prompt}],
        )
        return response.content[0].text
    else:
        raise ValueError(f"Unknown judge provider: {provider}")


def extract_judge_json(text):
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```\s*$", "", text)
    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1:
        raise ValueError("No JSON in judge response")
    return json.loads(text[start:end + 1])


def judge_batch(batch_slug, phase1_dir=None, phase2_dir=None):
    """Run LLM-as-judge on a single batch's Phase 2 output."""
    p1_dir = Path(phase1_dir) if phase1_dir else PHASE1_DIR
    p2_dir = Path(phase2_dir) if phase2_dir else PHASE2_DIR

    review_path = p2_dir / f"{batch_slug}_review.json"
    if not review_path.exists():
        return {"error": f"Review not found: {review_path}"}

    review = json.load(open(review_path, encoding="utf-8"))
    llm_used = review.get("llm_used", "")

    phase1_data = []
    batch_dir = p1_dir / batch_slug
    if batch_dir.exists():
        for p in sorted(batch_dir.glob("*.json")):
            if p.name.startswith("_"):
                continue
            try:
                phase1_data.append(json.load(open(p, encoding="utf-8")))
            except Exception:
                pass

    model, provider = select_judge_model(llm_used)
    prompt = JUDGE_PROMPT.format(
        phase1_json=json.dumps(phase1_data, ensure_ascii=False, indent=1)[:8000],
        phase2_json=json.dumps(review, ensure_ascii=False, indent=1)[:12000],
    )

    print(f"  Judging {batch_slug} with {model}...")
    try:
        response = call_judge(provider, model, prompt)
        scores = extract_judge_json(response)
        scores["judge_model"] = model
        scores["batch_slug"] = batch_slug
        return scores
    except Exception as e:
        return {"error": str(e), "judge_model": model, "batch_slug": batch_slug}


def judge_regression(batch_slug, golden_path, phase2_dir=None):
    """Compare current output against golden sample via judge scores."""
    p2_dir = Path(phase2_dir) if phase2_dir else PHASE2_DIR
    review_path = p2_dir / f"{batch_slug}_review.json"

    if not review_path.exists():
        return {"error": f"Review not found: {review_path}"}
    if not Path(golden_path).exists():
        return {"error": f"Golden sample not found: {golden_path}"}

    current_scores = judge_batch(batch_slug, phase2_dir=phase2_dir)
    if "error" in current_scores:
        return current_scores

    golden = json.load(open(golden_path, encoding="utf-8"))
    golden_overall = golden.get("overall", 0)
    current_overall = current_scores.get("overall", 0)

    result = {
        "golden_overall": golden_overall,
        "current_overall": current_overall,
        "delta": round(current_overall - golden_overall, 2),
        "regression": current_overall < golden_overall - 0.5,
        "scores": current_scores,
    }
    return result


# ========================================================
# 4. Main
# ========================================================

def main(phase: str = None, judge_batch_slug: str = None,
         golden_path: str = None):
    parser = argparse.ArgumentParser(description="Quality Gate")
    parser.add_argument("phase_arg", nargs="?", default=None,
                        help="all, phase1, phase2, 1, 2")
    parser.add_argument("--judge", action="store_true",
                        help="Run LLM-as-judge on Phase 2 outputs")
    parser.add_argument("--batch", default=None,
                        help="Specific batch slug for --judge")
    parser.add_argument("--regression", action="store_true",
                        help="Regression test against golden sample")
    parser.add_argument("--golden", default=None,
                        help="Path to golden sample JSON")

    if phase is not None:
        args = argparse.Namespace(
            phase_arg=phase, judge=judge_batch_slug is not None,
            batch=judge_batch_slug, regression=golden_path is not None,
            golden=golden_path)
    else:
        args = parser.parse_args()

    if args.judge:
        return _run_judge(args)
    if args.regression:
        return _run_regression(args)

    selected_phase = args.phase_arg or phase or "all"
    return _run_structural(selected_phase)

    selected_phase = args.phase_arg or phase or "all"
    return _run_structural(selected_phase)


def _run_structural(phase):
    total_pass = 0
    total_fail = 0

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

    p2_pass = 0
    p2_warn = 0
    p2_fail = 0

    if phase in ("all", "phase2", "2"):
        print("=== Phase 2 Quality Gate ===\n")
        if Path(BATCH_MAP).exists():
            completeness = check_phase2_completeness(BATCH_MAP)
            if "error" not in completeness:
                print(f"  Coverage: {completeness['reviews_found']}/{completeness['total_batches']} "
                      f"({completeness['coverage_pct']}%)")
                if completeness["missing_slugs"]:
                    print(f"  Missing: {', '.join(completeness['missing_slugs'][:10])}"
                          f"{'...' if len(completeness['missing_slugs']) > 10 else ''}")
                print()

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

    grand_pass = total_pass + p2_pass
    grand_fail = total_fail + p2_fail
    print(f"=== GRAND TOTAL: {grand_pass} PASS, {p2_warn} WARN, {grand_fail} FAIL ===")
    return grand_fail == 0


def _run_judge(args):
    print("=== LLM-as-Judge Quality Assessment ===\n")

    if args.batch:
        slugs = [args.batch]
    elif PHASE2_DIR.exists():
        slugs = [p.stem.replace("_review", "")
                 for p in sorted(PHASE2_DIR.glob("*_review.json"))]
    else:
        print("  No Phase 2 outputs found.")
        return False

    all_scores = []
    for slug in slugs:
        result = judge_batch(slug)
        if "error" in result:
            print(f"  [ERROR] {slug}: {result['error']}")
        else:
            overall = result.get("overall", 0)
            status = "PASS" if overall >= 7 else "WARN" if overall >= 5 else "FAIL"
            print(f"  [{status}] {slug}: overall={overall:.1f} "
                  f"(action={result.get('actionability', 0)}, "
                  f"facts={result.get('factual_grounding', 0)}, "
                  f"kr={result.get('korean_quality', 0)}, "
                  f"sowhat={result.get('so_what', 0)}) "
                  f"[{result.get('judge_model', '?')}]")
            if result.get("critical_issues"):
                for issue in result["critical_issues"]:
                    print(f"        ! {issue}")
            all_scores.append(result)

    if all_scores:
        avg = sum(s.get("overall", 0) for s in all_scores) / len(all_scores)
        print(f"\n  Average overall: {avg:.1f} ({len(all_scores)} batches)")
        return avg >= 6
    return False


def _run_regression(args):
    print("=== Regression Test (vs Golden Sample) ===\n")
    if not args.golden:
        print("  --golden path required for regression mode")
        return False

    batch_slug = args.batch
    if not batch_slug:
        batch_slug = Path(args.golden).stem.replace("_review", "")

    result = judge_regression(batch_slug, args.golden)
    if "error" in result:
        print(f"  [ERROR] {result['error']}")
        return False

    delta = result["delta"]
    status = "PASS" if not result["regression"] else "FAIL"
    print(f"  [{status}] {batch_slug}")
    print(f"    Golden:  {result['golden_overall']:.1f}")
    print(f"    Current: {result['current_overall']:.1f}")
    print(f"    Delta:   {delta:+.1f}")
    if result["regression"]:
        print(f"    ! Regression detected (>{0.5} point drop)")
    return not result["regression"]


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
