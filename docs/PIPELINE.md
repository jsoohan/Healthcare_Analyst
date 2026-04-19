# HealthcareIntel Pipeline — Operator Guide

전체 파이프라인 운영 가이드. Phase 0 (수집) → Phase 1 (구조화) → Phase 2 (LLM 분석) → 품질 검증.

---

## 중요: 로컬 vs 서버 실행

이 프로젝트의 스크립트는 **실행 환경 요구사항이 서로 다릅니다**. 잘못된 환경에서 실행하면 실패하므로 반드시 아래 표를 확인하세요.

| 단계 | 스크립트 | 실행 위치 | 이유 |
|------|---------|----------|------|
| **Phase 0a** | `build_ir_url_map.py` | **로컬 PC** | Chrome 브라우저 + Google 검색 (captcha 대응) |
| **Phase 0b** | `collect_transcripts_earnings.py` | **로컬 PC** | MarketScreener 수동 로그인 필요 (첫 실행), 쿠키 기반 세션 |
| **Phase 0c** | `collect_ir_presentations.py` | **로컬 PC** | Chrome + 파일 다운로드, Google 검색 |
| Phase 1a | `build_batch_map.py` | 로컬 or 서버 | Excel 읽기만 (파이썬 기본) |
| Phase 1b | `phase1_precheck.py` | **수집 파일이 있는 곳** | 파일 시스템 스캔 필요 |
| Phase 1c | (수동 Claude Code 세션) | Claude Code | LLM이 원본 파일 읽고 JSON 생성 |
| Phase 2 | `phase2_consult.py` | 로컬 or 서버 | LLM API 호출만 (key만 있으면 OK) |
| Quality Gate | `quality_gate.py` | Phase 2 출력이 있는 곳 | JSON 검증 + (judge 모드는 LLM 호출) |

**핵심**: Phase 0은 브라우저 자동화이므로 **데스크톱 환경(Windows/macOS/Linux with display)** 에서 실행해야 합니다. CI, Docker, 서버리스, 또는 Claude Code 웹 환경에서는 동작하지 않습니다.

---

## 로컬 환경 준비

### 1. 시스템 요구사항
- **OS**: Windows 10+, macOS 13+, 또는 Linux with display (X11/Wayland)
- **브라우저**: Google Chrome 120+ (자동 설치 — `webdriver-manager`가 ChromeDriver 다운로드)
- **Python**: 3.11+

### 2. 저장소 복제
```bash
git clone https://github.com/jsoohan/Healthcare_Analyst.git
cd Healthcare_Analyst
git checkout claude/healthcare-intel-phase-1-2-UCVLT
```

### 3. 의존성 설치
```bash
# 가상환경 권장
python -m venv .venv
# Windows PowerShell
.\.venv\Scripts\Activate.ps1
# macOS/Linux
source .venv/bin/activate

pip install -r requirements.txt
```

### 4. 환경변수 (Phase 2/Judge 용)
```powershell
# Windows PowerShell
$env:GEMINI_API_KEY = "..."
$env:ANTHROPIC_API_KEY = "..."

# macOS/Linux
export GEMINI_API_KEY="..."
export ANTHROPIC_API_KEY="..."
```

### 5. 마스터 DB 배치
`HealthcareIntel_Database_YYYYMMDD.xlsx` 파일을 프로젝트 루트에 둡니다.
스크립트가 가장 최근 파일을 자동 감지합니다.

---

## 전체 실행 흐름

### 단계 0a: IR URL 사전 매핑 (1회 실행, ~2-3시간)

**왜 필요한가**: 기업별 IR 페이지 URL을 미리 찾아두면 IR Presentation 수집 시 Google 검색 의존도가 낮아져 수집률이 60-70% → 80-90%+로 상승.

```bash
# 전체 기업 (headless 모드)
python scripts/build_ir_url_map.py

# 특정 섹터만 (테스트)
python scripts/build_ir_url_map.py --sector Biopharma --limit 20

# 기존 URL 재검증 (IR 페이지가 바뀌었을 수 있음)
python scripts/build_ir_url_map.py --verify

# 브라우저 창 띄우기 (디버그)
python scripts/build_ir_url_map.py --no-headless --limit 5
```

**출력**: `data/ir_url_map.json` — 회사별 IR URL + 발견 방법(domain_pattern/google_search/marketscreener)

**주의**:
- Google captcha 발생 시 헤드리스가 아닌 창을 띄워 수동 해결
- 50개마다 자동 체크포인트 저장 → 중단 후 재실행하면 이어서 진행
- `.gitignore`에 포함되므로 커밋되지 않음

---

### 단계 0b: Earnings Call Transcript 수집

**왜 필요한가**: MarketScreener가 가장 일관된 형식의 earnings call transcript를 제공. Phase 1이 이 파일들을 기반으로 회사별 JSON을 생성.

```bash
# 대화형 (분기 입력 프롬프트)
python scripts/collect_transcripts_earnings.py

# 비대화형
python scripts/collect_transcripts_earnings.py --quarter Q4 --year 2025

# 섹터 + 제한 (테스트)
python scripts/collect_transcripts_earnings.py --quarter Q4 --year 2025 --sector Biopharma --limit 5
```

**첫 실행 시 로그인 절차**:
1. 스크립트 실행 후 Chrome 창이 자동으로 열림
2. MarketScreener 로그인 페이지가 표시됨
3. **수동으로 로그인** (아이디/비밀번호 입력)
4. 콘솔에 "Press Enter >>> " 표시되면 Enter 입력
5. 쿠키가 `logs_EC_Q4_2025/ms_cookies.json`에 저장됨 → 다음부터는 자동 로그인

**출력**: `./transcripts_EC_Q4_2025/{company}_EC_Q4_2025.txt`
- 5줄 메타데이터 헤더 + `===` 구분선 + 본문
- `logs_EC_Q4_2025/progress.csv`에 진행 상황 기록 (중단 후 재시작 시 이어서 진행)

**수집률 개선 팁**:
- 20개마다 브라우저 자동 재시작 (메모리 누수 방지)
- 실패 시 `progress.csv`의 `note` 컬럼 확인 (`no_earnings_Q4_2025` = 해당 분기 transcript 미존재, `search_failed` = 회사명 매칭 실패 등)

---

### 단계 0c: IR Presentation 수집

**왜 필요한가**: Earnings call은 말로만 전달되므로 숫자/차트는 IR presentation PDF에만 존재. Phase 1의 재무 데이터 정확성에 필수.

```bash
python scripts/collect_ir_presentations.py --quarter Q4 --year 2025

# 섹터 제한
python scripts/collect_ir_presentations.py --quarter Q4 --year 2025 --sector Biopharma

# Headless + 테스트
python scripts/collect_ir_presentations.py --quarter Q4 --year 2025 --headless --limit 10
```

**수집 전략 (4단계 자동 fallback)**:
1. **Step 0** (NEW): `data/ir_url_map.json`의 IR URL로 직접 진입
2. **Step 1**: Google 검색으로 직접 PDF 발견
3. **Step 2**: Google 검색으로 IR 페이지 발견 후 내부 크롤링
4. **Step 3**: Ticker 기반 재검색

**출력**: `./ir_presentations/{company}_Q4_2025.{pdf|pptx}`
- `./logs/ir_progress.csv`에 진행 기록
- 30KB 미만 파일은 자동 거부 (HTML 오탐 방지)

---

### 단계 1a: 배치 맵 생성

Phase 0와 독립적으로 실행 가능 (Excel DB만 필요).

```bash
python scripts/build_batch_map.py
```

**출력**: `data/batch_map.json` — 9개 Tier1 × 서브섹터별 배치 (~70개)

---

### 단계 1b: Phase 1 Precheck

수집된 파일이 Phase 1에 쓸 수 있는 상태인지 검증.

```bash
# Windows PowerShell
$env:QUARTER = "Q4_2025"
python scripts/phase1_precheck.py

# macOS/Linux
QUARTER=Q4_2025 python scripts/phase1_precheck.py
```

**출력**: `data/phase1/{slug}/_precheck.json` — READY/SKIP 상태 + 소스 파일 경로

---

### 단계 1c: Phase 1 JSON 생성 (Claude Code 세션)

Phase 1 수치 구조화는 자동화 스크립트가 아닌 **Claude Code 세션에서 수동 실행**.

각 배치마다 Claude Code에게 다음과 같이 지시:
```
"data/phase1/1_1_oncology/_precheck.json을 읽어.
status=READY인 기업들만 sources 필드에 기재된 파일들을 읽고
Phase 1 스키마에 맞는 JSON을 data/phase1/1_1_oncology/{ticker}.json으로 저장해줘.
Transcript 파일은 === 구분선 이후부터 본문으로 파싱할 것.
소스에 없는 수치는 반드시 null 또는 'N/A'로 표기하고 절대 추정하지 마."
```

---

### 단계 2: Phase 2 LLM 섹터 분석

서버/로컬 어디서든 가능 (API key만 있으면 OK).

```bash
# 전체 배치
python scripts/phase2_consult.py

# 특정 배치만
python scripts/phase2_consult.py 1_1_oncology 2_2_surgical_systems
```

**출력**: `data/phase2/{slug}_review.json`
**비용**: ~$8-10 (Gemini 2.5 Pro 전량) / ~$15 (Sonnet fallback 다수)

---

### 단계 3: 품질 검증

#### 3a. 구조 검증 (즉시, 무료)
```bash
python scripts/quality_gate.py                # 전체 Phase 1 + 2
python scripts/quality_gate.py phase2         # Phase 2만
```

#### 3b. LLM-as-judge (품질 점수 채점, ~$0.40/회)
```bash
# 전체 배치 채점
python scripts/quality_gate.py --judge

# 특정 배치만
python scripts/quality_gate.py --judge --batch 1_1_oncology

# Judge 모델 고정 (비용 통제)
JUDGE_MODEL=gemini-2.5-flash python scripts/quality_gate.py --judge
JUDGE_MODEL=claude-haiku-4-5 python scripts/quality_gate.py --judge
```

**Adaptive cross-family judge** (기본):
- `gemini-2.5-pro` 출력 → `claude-haiku-4-5`로 채점
- `claude-sonnet-4-6` 출력 → `gemini-2.5-flash`로 채점

채점 기준: actionability / factual_grounding / korean_quality / so_what (각 0-10점)

#### 3c. 회귀 테스트 (golden 대비)
```bash
python scripts/quality_gate.py --regression \
  --batch 1_1_oncology \
  --golden tests/golden/1_1_oncology_review.json
```
점수가 0.5점 이상 하락하면 FAIL.

---

## 흔한 이슈와 해결

### MarketScreener 로그인 실패
- **증상**: `[LOGIN] Failed` 반복
- **원인**: 쿠키 만료 또는 IP 차단
- **해결**: `logs_EC_*/ms_cookies.json` 삭제 후 재실행, 수동 재로그인

### Chrome 드라이버 버전 불일치
- **증상**: `SessionNotCreatedException: This version of ChromeDriver...`
- **원인**: Chrome 업데이트됐는데 WebDriver 캐시가 오래됨
- **해결**: `~/.wdm/drivers/` 폴더 삭제 → `webdriver-manager`가 재다운로드

### Google Captcha
- **증상**: IR 수집 중 "not_found" 반복 발생
- **해결**: `--no-headless`로 창 띄우고 수동으로 captcha 해결, 쿠키 저장됨

### `ModuleNotFoundError: scripts.db_loader`
- **원인**: 프로젝트 루트가 아닌 `scripts/` 내부에서 실행
- **해결**: 프로젝트 루트에서 `python scripts/collect_...py` 실행

### 파일 경로 문자 이슈 (Windows)
- `:`, `?`, `*` 등이 회사명에 있으면 sanitize 함수가 `_`로 치환
- Phase 1의 파일 매칭도 동일 규칙이므로 정합성 유지

---

## 테스트 실행

개발 환경에서 mock 기반 단위 테스트:
```bash
pytest tests/ -v
```

Phase 0 스크립트는 Selenium mock으로 테스트되므로 실 브라우저 없이 검증.
실제 수집 smoke test는 `--limit 2`로 소규모 실행 권장.

---

## 체크리스트

### Phase 0 시작 전
- [ ] 로컬 PC (Windows/macOS/Linux desktop)
- [ ] Chrome 설치됨
- [ ] Python 3.11+ 설치
- [ ] `pip install -r requirements.txt`
- [ ] `HealthcareIntel_Database_*.xlsx` 루트에 위치
- [ ] MarketScreener 계정 준비

### Phase 0 완료 후
- [ ] `./transcripts_EC_Q4_2025/` 에 수집 파일 존재
- [ ] `./ir_presentations/` 에 PDF 존재
- [ ] `data/ir_url_map.json` 생성됨
- [ ] `logs_EC_*/progress.csv`, `logs/ir_progress.csv` 확인

### Phase 2 시작 전
- [ ] `GEMINI_API_KEY`, `ANTHROPIC_API_KEY` 설정
- [ ] `data/phase1/{slug}/{ticker}.json` 파일들 생성됨
- [ ] 품질 게이트 Phase 1 통과

### 최종 검증
- [ ] 구조 게이트 PASS
- [ ] LLM-as-judge 평균 점수 ≥ 6
- [ ] 회귀 테스트 통과 (golden 존재 시)
