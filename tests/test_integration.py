"""Integration test: end-to-end pipeline with synthetic data."""
import json
from pathlib import Path
from unittest.mock import patch

import pytest

from tests.conftest import create_sample_excel


def test_full_pipeline(tmp_path):
    """
    E2E: Excel → batch_map → precheck → (mock LLM) phase2 → quality gate.
    Validates that the data contracts between all scripts are correct.
    """
    # =============================================
    # STEP 1: Build batch map from sample Excel
    # =============================================
    excel_path = create_sample_excel(tmp_path / "HealthcareIntel_Database.xlsx")
    batch_map_path = tmp_path / "data" / "batch_map.json"
    (tmp_path / "data").mkdir(parents=True, exist_ok=True)

    import scripts.build_batch_map as build_mod

    original_db = build_mod.DB_PATH
    original_output = build_mod.OUTPUT
    try:
        build_mod.DB_PATH = str(excel_path)
        build_mod.OUTPUT = str(batch_map_path)
        build_mod.main()
    finally:
        build_mod.DB_PATH = original_db
        build_mod.OUTPUT = original_output

    assert batch_map_path.exists()
    batch_map = json.load(open(batch_map_path, encoding="utf-8"))
    assert batch_map["summary"]["total_companies"] == 5

    # =============================================
    # STEP 2: Create source files for precheck
    # =============================================
    transcript_dir = tmp_path / "transcripts_EC_Q4_2025"
    transcript_dir.mkdir()
    ir_dir = tmp_path / "ir_presentations"
    ir_dir.mkdir()
    filing_dir = tmp_path / "filings"
    filing_dir.mkdir()

    # Create transcript files for some companies (> 1KB)
    header = (
        "Company : {company}\n"
        "Title   : Q4 2025 Earnings Call Transcript\n"
        "Source  : https://example.com\n"
        "Saved   : 2026-01-28 14:23:15\n"
        "=" * 80 + "\n\n"
    )
    transcript_body = "Operator: Welcome to the earnings call. " * 30  # > 1KB

    for company in ["TestPharma Inc.", "RoboSurg Ltd."]:
        content = header.format(company=company) + transcript_body
        (transcript_dir / f"{company}_EC_Q4_2025.txt").write_text(content, encoding="utf-8")

    # Create IR file for one company (> 5KB)
    (ir_dir / "TestPharma Inc._Q4_2025.pdf").write_bytes(b"%PDF-1.4 " + b"x" * 5200)

    # =============================================
    # STEP 2a: Run precheck
    # =============================================
    import scripts.phase1_precheck as precheck_mod

    original_vals = {
        "BATCH_MAP": precheck_mod.BATCH_MAP,
        "PHASE1_DIR": precheck_mod.PHASE1_DIR,
        "TRANSCRIPT_DIR": precheck_mod.TRANSCRIPT_DIR,
        "IR_DIR": precheck_mod.IR_DIR,
        "FILING_DIR": precheck_mod.FILING_DIR,
        "QUARTER": precheck_mod.QUARTER,
    }
    try:
        precheck_mod.BATCH_MAP = str(batch_map_path)
        precheck_mod.PHASE1_DIR = Path(tmp_path / "data" / "phase1")
        precheck_mod.TRANSCRIPT_DIR = transcript_dir
        precheck_mod.IR_DIR = ir_dir
        precheck_mod.FILING_DIR = filing_dir
        precheck_mod.QUARTER = "Q4_2025"
        precheck_mod.main()
    finally:
        for k, v in original_vals.items():
            setattr(precheck_mod, k, v)

    # Verify precheck output
    phase1_dir = tmp_path / "data" / "phase1"
    precheck_files = list(phase1_dir.rglob("_precheck.json"))
    assert len(precheck_files) > 0

    # Check that at least some companies are READY
    total_ready = 0
    for pf in precheck_files:
        data = json.load(open(pf, encoding="utf-8"))
        for c in data["companies"]:
            if c["status"] == "READY":
                total_ready += 1
    assert total_ready >= 2  # At least TestPharma and RoboSurg

    # =============================================
    # STEP 2b: Create Phase 1 company JSONs
    # (In real pipeline this is done manually by Claude Code)
    # =============================================
    for pf in precheck_files:
        precheck_data = json.load(open(pf, encoding="utf-8"))
        batch_dir = pf.parent
        for c in precheck_data["companies"]:
            if c["status"] != "READY":
                continue
            company_json = {
                "ticker": c["ticker"],
                "company": c["company"],
                "tier1": precheck_data["tier1"],
                "sub_sector": precheck_data["sub_sector"],
                "exchange": c.get("exchange", ""),
                "mkt_cap": c.get("mkt_cap", ""),
                "fiscal_quarter_reported": "Q4_2025",
                "fiscal_quarter_mapped": "FY2025 Q4",
                "financials": {
                    "quarterly": {
                        "revenue": {"2025-Q4": "$1.0B", "source": "transcript"},
                    },
                },
                "key_products": [
                    {"name": "Product A", "category": "Test", "revenue_q": "$500M",
                     "revenue_yoy": "+10%", "note": "Test note", "source": "transcript"},
                ],
                "events": [],
                "guidance": {"fy_guidance_current": "$4B", "raised_lowered_maintained": "maintained",
                             "quote": "We maintain guidance", "source": "transcript"},
                "management_quotes": [],
                "sector_specific_raw": {},
                "sources_used": [str(s) for s in [c["sources"].get("transcript"),
                                                   c["sources"].get("ir")] if s],
                "sources_missing": [],
            }
            with open(batch_dir / f"{c['ticker']}.json", "w", encoding="utf-8") as f:
                json.dump(company_json, f, ensure_ascii=False, indent=2)

    # =============================================
    # STEP 3: Run Phase 2 with mocked LLM
    # =============================================
    import scripts.phase2_consult as phase2_mod

    # Build a valid mock response
    mock_review = {
        "sub_sector": "test",
        "tier1": "test",
        "sector_dynamics": [
            {"trend": "Trend 1", "evidence": "ev1", "structural_or_cyclical": "structural",
             "investment_implication": "imp1"},
            {"trend": "Trend 2", "evidence": "ev2", "structural_or_cyclical": "cyclical",
             "investment_implication": "imp2"},
            {"trend": "Trend 3", "evidence": "ev3", "structural_or_cyclical": "structural",
             "investment_implication": "imp3"},
        ],
        "cross_company_positioning": {
            "top_performers": [{"ticker": "TEST", "reason": "Strong growth"}],
            "notable_movers": [],
            "positioning_map_summary": "Test summary",
        },
        "companies": {},
        "exec_summary_input": {
            "one_liner": "Test sector shows 15% growth driven by innovation",
            "key_number": "$1.0B revenue",
            "so_what": "Sector remains attractive for investment",
        },
        "confidence_flags": [],
    }

    # Add company entries for each READY company
    for pf in precheck_files:
        precheck_data = json.load(open(pf, encoding="utf-8"))
        for c in precheck_data["companies"]:
            if c["status"] == "READY":
                mock_review["companies"][c["ticker"]] = {
                    "validated_financials_notes": "Validated OK",
                    "enriched_highlights": [
                        "Revenue grew 10% YoY to $1.0B",
                        "Product A contributed $500M in Q4 2025",
                        "Operating margin expanded 200bps to 25%",
                        "Guidance maintained at $4B for FY2026",
                        "Pipeline advancement with 3 Phase 3 readouts expected",
                    ],
                    "revenue_drivers": [
                        {"product": "Product A", "yoy_change": "+10%",
                         "cause": "Market expansion", "structural": True,
                         "outlook": "Continued growth expected"},
                    ],
                    "events_enriched": [],
                    "enriched_sector_kpis": {},
                    "key_risk": "Competition from generics",
                    "investor_implication": "Attractive risk/reward",
                }

    mock_response_text = json.dumps(mock_review, ensure_ascii=False)

    phase2_dir = tmp_path / "data" / "phase2"
    logs_dir = tmp_path / "data" / "logs"

    original_p2 = {
        "BATCH_MAP": phase2_mod.BATCH_MAP,
        "PHASE1_DIR": phase2_mod.PHASE1_DIR,
        "PHASE2_DIR": phase2_mod.PHASE2_DIR,
        "LOGS_DIR": phase2_mod.LOGS_DIR,
    }
    try:
        phase2_mod.BATCH_MAP = str(batch_map_path)
        phase2_mod.PHASE1_DIR = phase1_dir
        phase2_mod.PHASE2_DIR = phase2_dir
        phase2_mod.LOGS_DIR = logs_dir

        with patch("scripts.phase2_consult.call_llm") as mock_llm:
            mock_llm.return_value = (mock_response_text, "gemini-2.5-pro")
            # Process only batches that have phase1 data
            for slug in batch_map["batches"]:
                if (phase1_dir / slug).exists():
                    # Check if there are company JSONs (not just _precheck)
                    company_files = [f for f in (phase1_dir / slug).glob("*.json")
                                     if not f.name.startswith("_")]
                    if company_files:
                        phase2_mod.process_batch(slug)
    finally:
        for k, v in original_p2.items():
            setattr(phase2_mod, k, v)

    # Verify Phase 2 output
    review_files = list(phase2_dir.glob("*_review.json"))
    assert len(review_files) >= 1

    # =============================================
    # STEP 4: Run quality gate
    # =============================================
    import scripts.quality_gate as qg_mod

    original_qg = {
        "PHASE1_DIR": qg_mod.PHASE1_DIR,
        "PHASE2_DIR": qg_mod.PHASE2_DIR,
        "BATCH_MAP": qg_mod.BATCH_MAP,
    }
    try:
        qg_mod.PHASE1_DIR = phase1_dir
        qg_mod.PHASE2_DIR = phase2_dir
        qg_mod.BATCH_MAP = str(batch_map_path)
        result = qg_mod.main(phase="all")
    finally:
        for k, v in original_qg.items():
            setattr(qg_mod, k, v)

    assert result is True
