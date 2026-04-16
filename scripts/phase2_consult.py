#!/usr/bin/env python3
"""
Phase 2: Sector Specialist Review (Multi-LLM).
- Primary: Gemini 2.5 Pro (google-genai SDK)
- Fallback: Claude Sonnet 4.6 (anthropic SDK)
"""
import json
import os
import re
import sys
import time
from pathlib import Path
from datetime import datetime, timezone

BATCH_MAP = "data/batch_map.json"
PHASE1_DIR = Path("data/phase1")
PHASE2_DIR = Path("data/phase2")
LOGS_DIR = Path("data/logs")

PRIMARY_PROVIDER = os.getenv("LLM_PROVIDER", "gemini").lower()
PRIMARY_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-pro")
FALLBACK_MODEL = os.getenv("CLAUDE_MODEL", "claude-sonnet-4-6")

# ========================================================
# 1. Embedded Sector Personas
# ========================================================

BASE_PERSONA = """당신은 HealthcareIntel의 {tier1} 섹터 전문 애널리스트입니다.
커버리지: 전 세계 상장 {tier1} 기업 (시가총액 $100M+).

## 분석 프레임워크
1. **구조적 vs 일시적** 구분: 매출/마진 변동이 구조적 변화인지 일시적 요인인지 반드시 구분
2. **경쟁 역학**: 단일 기업이 아닌 서브섹터 내 competitive positioning으로 분석
3. **투자자 관점**: PE/VC/기관투자자에게 actionable한 시사점 제시
4. **확신도 표시**: 불확실할 때 confidence level(high/medium/low) 명시
5. **So What**: 모든 데이터 포인트에 "이것이 왜 중요한가" 해석 동반

## 소스 규칙
- 1차 소스(Transcript/IR/Filing)의 수치를 최우선으로 취급
- 수치가 의심스러우면 "확인 필요" 플래그
- Phase 1 데이터에 없는 수치를 배경지식으로 채우지 말 것
- 확인 불가 항목은 null 또는 "N/A"로

## 출력 스타일
- **한국어 기본**, 회사명/제품명/재무용어는 영어 원문 유지
- 숫자를 먼저 제시하고 해석을 이어서
- 동의하지 말고 반대 의견도 솔직하게
- "내 현재 conviction은 ~이다. 근거는 ~" 형식으로 의견 표명

## {tier1} 섹터 핵심 관점
{sector_lens}
"""

SECTOR_LENSES = {
    "Biopharma": """
- LOE(Loss of Exclusivity) 사이클과 파이프라인 대체 능력이 장기 성장의 핵심
- Top 5 제품 매출 집중도와 차세대 파이프라인(Phase 3+) 품질
- ADC, bispecific, GLP-1, cell/gene therapy 등 구조적 트렌드에서의 포지셔닝
- M&A: 대형 제약사의 LOE 방어 vs 바이오텍의 플랫폼 차별성
- BIOSECURE Act, 약가 협상(IRA), 중국 bio 공급망 변화의 영향
""",
    "MedTech": """
- Installed base와 consumables/service revenue의 recurring 구조
- 지역 믹스(미국/유럽/중국/신흥시장)와 regulatory pathway (510k, CE, NMPA)
- 병원 capex 사이클, GLP-1의 bariatric/cardiac 디바이스 수요 영향
- AI/로보틱스 통합, cross-selling을 통한 organic growth 지속 가능성
- 중국 VBP(volume-based procurement)와 현지 경쟁 심화
""",
    "Pharma Services": """
- Backlog, book-to-bill, 가동률이 선행 지표 — 수주→매출 전환 시차 2-3년
- CDMO: ADC/GLP-1 전용 라인, BIOSECURE Act 수혜 구조 (한국/EU vs 중국)
- CRO: 바이오텍 펀딩 환경에 민감, 대형 제약사 vs 바이오텍 믹스
- Customer concentration 리스크 (top 10 고객이 매출의 X%)
""",
    "Biologics Tools & Services": """
- Bioprocessing 사이클(post-COVID 재고 조정 후 회복 국면)
- Consumables vs Instruments 믹스가 마진 안정성의 열쇠
- 싱글유즈, 세포유전자치료용 원료, 바이오 인프라 CapEx 사이클
- 중국 시장 노출도와 지정학 리스크
""",
    "Healthcare IT": """
- ARR, NRR(Net Revenue Retention), churn이 SaaS 품질의 핵심 지표
- AI 매출 실체화 비중: "파일럿" vs "프로덕션" 구분 필수
- EHR, prior auth, RCM 등 어드민 자동화의 실제 비용 절감 효과
- Subscription vs Professional services 믹스, 대형 계약 lumpiness
""",
    "Consumer Health": """
- 브랜드별 M/S, 가격 결정력, 프리미엄화 트렌드
- GLP-1의 식욕 억제/체중 관리 OTC 시장 잠식 (구조적 피해주 vs 무풍지대)
- 채널 믹스: 오프라인 drugstore, 이커머스, DTC
- 지역 특수성: 일본 OTC, 중국 TCM, 인도 Ayurveda
""",
    "IVD": """
- Test volume, installed base, reimbursement status의 3요소
- LDT→IVD 전환(VALID Act) 수혜/피해 기업
- Liquid biopsy, MRD, companion diagnostics 성장 영역
- Routine clinical 대비 specialty dx의 ASP와 마진 구조
""",
    "Healthcare Services": """
- Same-store growth, MLR(Managed Care), admissions, 병상 가동률
- 인력난, labor cost 압박이 마진에 미치는 영향
- 정책 리스크: Medicare Advantage 개편, 약가 협상, 가치기반 지불(VBP)
- 아시아 확장(중국/인도 고급 병원) vs 미국 포화 시장
""",
    "Dentistry": """
- 신규 섹터 — 주의 깊게 접근. 참조 thesis 없음
- Implant, clear aligner, digital dentistry (CAD/CAM, intraoral scanner)
- 지역 믹스: 한국/일본/중국 implant 수요, 북미 DSO 확산
- OEM vs 디지털 워크플로우 통합 플레이어
""",
}


def build_system_prompt(tier1: str) -> str:
    sector_lens = SECTOR_LENSES.get(
        tier1,
        "- (이 섹터에 대한 특수 관점 없음. 일반 프레임워크 적용)"
    )
    return BASE_PERSONA.format(tier1=tier1, sector_lens=sector_lens.strip())


# ========================================================
# 2. Task Prompt Builder
# ========================================================

TASK_TEMPLATE = """아래는 **{sub_sector}** 서브섹터 소속 기업들의 Phase 1 구조화 데이터입니다.
전문가로서 아래 7가지 태스크를 수행해주세요.

## 태스크
1. **DATA VALIDATION**: 의심스러운 수치 식별 → `confidence_flags`에 기록
2. **REVENUE DRIVER 분석** (기업별): 핵심 제품/사업부 매출 변동의 원인 (구조적 vs 일시적) + 향후 전망
3. **EVENT 전략 맥락** (기업별): M&A/Pipeline/규제 이벤트의 "왜 지금" + "경쟁사 영향"
4. **HIGHLIGHTS** (기업별): Phase 1 raw를 4-5개 전략적 불릿으로 재작성 (각 불릿에 수치 1개 이상)
5. **SECTOR DYNAMICS**: 이 sub-sector에서 진행 중인 3-5개 구조적 트렌드
6. **CROSS-COMPANY POSITIONING**: Top performers, Notable movers, positioning map
7. **EXEC SUMMARY INPUT**: 이 sub-sector의 한 줄 요약 + 핵심 수치 + So What

## 출력 포맷 (엄격히 준수)
```json
{{{{
  "sub_sector": "{sub_sector}",
  "tier1": "{tier1}",
  "reviewed_at": "ISO 8601",
  "llm_used": "...",
  "sector_dynamics": [
    {{{{"trend": "", "evidence": "", "structural_or_cyclical": "", "investment_implication": ""}}}}
  ],
  "cross_company_positioning": {{{{
    "top_performers": [{{{{"ticker": "", "reason": ""}}}}],
    "notable_movers": [{{{{"ticker": "", "direction": "+/-", "reason": ""}}}}],
    "positioning_map_summary": ""
  }}}},
  "companies": {{{{
    "<TICKER>": {{{{
      "validated_financials_notes": "",
      "enriched_highlights": ["bullet1", "bullet2", "bullet3", "bullet4", "bullet5"],
      "revenue_drivers": [
        {{{{"product": "", "yoy_change": "", "cause": "", "structural": true, "outlook": ""}}}}
      ],
      "events_enriched": [
        {{{{"type": "", "target": "", "strategic_context": "", "competitive_impact": ""}}}}
      ],
      "enriched_sector_kpis": {{{{}}}},
      "key_risk": "",
      "investor_implication": ""
    }}}}
  }}}},
  "exec_summary_input": {{{{
    "one_liner": "",
    "key_number": "",
    "so_what": ""
  }}}},
  "confidence_flags": [
    {{{{"field": "", "reason": "", "level": "high|medium|low"}}}}
  ]
}}}}
```

## 규칙
- JSON **외부**에 설명 문장 금지
- markdown fence(```json)는 있어도 없어도 됨 (파서가 처리)
- Phase 1 데이터에 없는 수치를 생성하지 말 것
- 한국어 기본, 회사명/제품명은 영어

## Phase 1 데이터
{phase1_json}
"""


def build_task_prompt(batch_data: dict) -> str:
    return TASK_TEMPLATE.format(
        sub_sector=batch_data["sub_sector"],
        tier1=batch_data["tier1"],
        phase1_json=json.dumps(batch_data["companies"], ensure_ascii=False, indent=2),
    )


# ========================================================
# 3. LLM Provider Adapters
# ========================================================

def call_gemini(system: str, user: str, model: str) -> str:
    """
    Google Gen AI SDK (new unified SDK, google-genai package).
    Docs: https://googleapis.github.io/python-genai/
    """
    from google import genai
    from google.genai import types

    client = genai.Client()

    response = client.models.generate_content(
        model=model,
        contents=user,
        config=types.GenerateContentConfig(
            system_instruction=system,
            temperature=0.3,
            max_output_tokens=16000,
            response_mime_type="application/json",
        ),
    )
    return response.text


def call_anthropic(system: str, user: str, model: str) -> str:
    """Anthropic Claude API (anthropic package)."""
    import anthropic
    client = anthropic.Anthropic()
    response = client.messages.create(
        model=model,
        max_tokens=16000,
        temperature=0.3,
        system=system,
        messages=[{"role": "user", "content": user}],
    )
    return response.content[0].text


def call_llm(provider: str, system: str, user: str) -> tuple:
    if provider == "gemini":
        return call_gemini(system, user, PRIMARY_MODEL), PRIMARY_MODEL
    elif provider == "anthropic":
        return call_anthropic(system, user, FALLBACK_MODEL), FALLBACK_MODEL
    else:
        raise ValueError(f"Unknown provider: {provider}")


# ========================================================
# 4. JSON Parsing (robust)
# ========================================================

def extract_json(text: str) -> dict:
    """응답에서 JSON 추출. markdown fence 제거, 앞뒤 설명 제거."""
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```\s*$", "", text)
    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1:
        raise ValueError("No JSON object found in response")
    return json.loads(text[start:end + 1])


# ========================================================
# 5. Phase 1 데이터 로드
# ========================================================

def load_batch_data(batch_slug: str) -> dict:
    batch_dir = PHASE1_DIR / batch_slug
    if not batch_dir.exists():
        raise FileNotFoundError(f"Phase 1 directory not found: {batch_dir}")

    precheck = json.load(open(batch_dir / "_precheck.json", encoding="utf-8"))

    company_jsons = []
    for p in sorted(batch_dir.glob("*.json")):
        if p.name.startswith("_"):
            continue
        try:
            company_jsons.append(json.load(open(p, encoding="utf-8")))
        except Exception as e:
            print(f"  [WARN] Failed to load {p}: {e}")

    return {
        "batch_slug": batch_slug,
        "tier1": precheck["tier1"],
        "sub_sector": precheck["sub_sector"],
        "companies": company_jsons,
    }


# ========================================================
# 6. Main orchestration
# ========================================================

def consult_batch(batch_slug: str, use_fallback: bool = False) -> dict:
    batch_data = load_batch_data(batch_slug)
    if not batch_data["companies"]:
        raise ValueError(f"No company JSONs in batch {batch_slug}")

    system = build_system_prompt(batch_data["tier1"])
    user = build_task_prompt(batch_data)

    provider = "anthropic" if use_fallback else PRIMARY_PROVIDER
    print(f"  -> Calling {provider} for {batch_slug} ({len(batch_data['companies'])} companies)")

    start = time.time()
    response_text, model_name = call_llm(provider, system, user)
    elapsed = time.time() - start

    result = extract_json(response_text)
    result["llm_used"] = model_name
    result["reviewed_at"] = datetime.now(timezone.utc).isoformat()
    result["elapsed_seconds"] = round(elapsed, 1)
    return result


def save_result(batch_slug: str, result: dict):
    PHASE2_DIR.mkdir(parents=True, exist_ok=True)
    out_path = PHASE2_DIR / f"{batch_slug}_review.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print(f"  -> Saved: {out_path}")


def log_error(batch_slug: str, error: Exception, provider: str):
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    with open(LOGS_DIR / "phase2_errors.jsonl", "a", encoding="utf-8") as f:
        f.write(json.dumps({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "batch_slug": batch_slug,
            "provider": provider,
            "error": str(error),
            "error_type": type(error).__name__,
        }, ensure_ascii=False) + "\n")


def process_batch(batch_slug: str):
    """배치 처리: primary 실패 시 자동 fallback."""
    print(f"[{batch_slug}]")
    try:
        result = consult_batch(batch_slug, use_fallback=False)
        save_result(batch_slug, result)
        return "success_primary"
    except Exception as e:
        print(f"  x Primary ({PRIMARY_PROVIDER}) failed: {e}")
        log_error(batch_slug, e, PRIMARY_PROVIDER)

        print(f"  -> Retrying with Claude Sonnet fallback...")
        try:
            result = consult_batch(batch_slug, use_fallback=True)
            save_result(batch_slug, result)
            return "success_fallback"
        except Exception as e2:
            print(f"  x Fallback also failed: {e2}")
            log_error(batch_slug, e2, "anthropic")
            return "failed"


def main():
    if len(sys.argv) > 1:
        targets = sys.argv[1:]
    else:
        data = json.load(open(BATCH_MAP, encoding="utf-8"))
        targets = list(data["batches"].keys())

    stats = {"success_primary": 0, "success_fallback": 0, "failed": 0}
    for slug in targets:
        if not (PHASE1_DIR / slug).exists():
            print(f"[SKIP] {slug}: no Phase 1 data")
            continue
        result = process_batch(slug)
        stats[result] += 1
        time.sleep(2)  # rate limit 방어

    print("\n=== Phase 2 Summary ===")
    print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()
