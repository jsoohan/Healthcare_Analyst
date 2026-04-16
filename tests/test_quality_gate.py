"""Tests for scripts/quality_gate.py."""
import json
from pathlib import Path

import pytest

from scripts.quality_gate import (
    validate_phase1_company,
    validate_phase1_batch,
    validate_phase2_review,
    check_phase2_completeness,
    main,
    PHASE1_REQUIRED_FIELDS,
)


# ========================================================
# Phase 1 Validation
# ========================================================

class TestValidatePhase1Company:
    def test_pass(self, sample_phase1_company):
        errors = validate_phase1_company(sample_phase1_company)
        assert errors == []

    def test_missing_ticker(self, sample_phase1_company):
        data = dict(sample_phase1_company)
        del data["ticker"]
        errors = validate_phase1_company(data)
        assert any("ticker" in e for e in errors)

    def test_missing_sources_used(self, sample_phase1_company):
        data = dict(sample_phase1_company)
        data["sources_used"] = []
        errors = validate_phase1_company(data)
        assert any("sources_used" in e for e in errors)

    def test_empty_company(self, sample_phase1_company):
        data = dict(sample_phase1_company)
        data["company"] = ""
        errors = validate_phase1_company(data)
        assert any("company" in e for e in errors)

    def test_no_products_or_financials(self):
        data = {
            "ticker": "TEST",
            "company": "Test Corp",
            "tier1": "Biopharma",
            "sub_sector": "Oncology",
            "sources_used": ["transcript.txt"],
            "key_products": [],
            "financials": {
                "annual": {"revenue": {"source": "N/A"}},
            },
        }
        errors = validate_phase1_company(data)
        assert any("neither key_products nor financials" in e for e in errors)

    def test_has_financials_passes(self):
        data = {
            "ticker": "TEST",
            "company": "Test Corp",
            "tier1": "Biopharma",
            "sub_sector": "Oncology",
            "sources_used": ["transcript.txt"],
            "key_products": [],
            "financials": {
                "quarterly": {"revenue": {"2025-Q4": "$8.2B", "source": "transcript"}},
            },
        }
        errors = validate_phase1_company(data)
        assert not any("neither" in e for e in errors)


class TestValidatePhase1Batch:
    def test_pass(self, project_dir, monkeypatch):
        monkeypatch.setattr("scripts.quality_gate.PHASE1_DIR",
                            project_dir / "data" / "phase1")
        result = validate_phase1_batch("1_oncology")
        assert result["pass"] >= 1
        assert result["fail"] == 0

    def test_missing_dir(self, tmp_path, monkeypatch):
        monkeypatch.setattr("scripts.quality_gate.PHASE1_DIR", tmp_path)
        result = validate_phase1_batch("nonexistent")
        assert "error" in result


# ========================================================
# Phase 2 Validation
# ========================================================

class TestValidatePhase2Review:
    def test_pass(self, tmp_path, sample_phase2_response):
        review_path = tmp_path / "test_review.json"
        with open(review_path, "w", encoding="utf-8") as f:
            json.dump(sample_phase2_response, f, ensure_ascii=False)

        errors = validate_phase2_review(review_path)
        assert errors == []

    def test_missing_sector_dynamics(self, tmp_path, sample_phase2_response):
        data = dict(sample_phase2_response)
        data["sector_dynamics"] = [{"trend": "only one"}]
        review_path = tmp_path / "test_review.json"
        with open(review_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)

        errors = validate_phase2_review(review_path)
        assert any("sector_dynamics" in e for e in errors)

    def test_empty_exec_summary(self, tmp_path, sample_phase2_response):
        data = dict(sample_phase2_response)
        data["exec_summary_input"] = {"one_liner": "", "key_number": "", "so_what": ""}
        review_path = tmp_path / "test_review.json"
        with open(review_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)

        errors = validate_phase2_review(review_path)
        assert any("one_liner" in e for e in errors)

    def test_empty_companies(self, tmp_path, sample_phase2_response):
        data = dict(sample_phase2_response)
        data["companies"] = {}
        review_path = tmp_path / "test_review.json"
        with open(review_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)

        errors = validate_phase2_review(review_path)
        assert any("companies" in e for e in errors)

    def test_highlight_without_number(self, tmp_path, sample_phase2_response):
        data = dict(sample_phase2_response)
        # Replace all highlights with text that has no numbers
        for ticker in data["companies"]:
            data["companies"][ticker]["enriched_highlights"] = [
                "No numeric value here",
                "Still no numbers at all",
            ]
        review_path = tmp_path / "test_review.json"
        with open(review_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)

        errors = validate_phase2_review(review_path)
        assert any("no numeric value" in e for e in errors)

    def test_invalid_json_file(self, tmp_path):
        review_path = tmp_path / "bad_review.json"
        review_path.write_text("not json at all")

        errors = validate_phase2_review(review_path)
        assert any("JSON load error" in e for e in errors)


# ========================================================
# Phase 2 Completeness
# ========================================================

class TestCheckPhase2Completeness:
    def test_full_coverage(self, project_dir, monkeypatch, sample_phase2_response):
        monkeypatch.setattr("scripts.quality_gate.PHASE2_DIR",
                            project_dir / "data" / "phase2")

        # Create review files for all batches
        phase2_dir = project_dir / "data" / "phase2"
        for slug in ["1_oncology", "2_surgical_systems"]:
            with open(phase2_dir / f"{slug}_review.json", "w", encoding="utf-8") as f:
                json.dump(sample_phase2_response, f, ensure_ascii=False)

        result = check_phase2_completeness(
            str(project_dir / "data" / "batch_map.json"))
        assert result["coverage_pct"] == 100.0
        assert result["reviews_missing"] == 0

    def test_partial_coverage(self, project_dir, monkeypatch, sample_phase2_response):
        monkeypatch.setattr("scripts.quality_gate.PHASE2_DIR",
                            project_dir / "data" / "phase2")

        # Only create one review file
        phase2_dir = project_dir / "data" / "phase2"
        with open(phase2_dir / "1_oncology_review.json", "w", encoding="utf-8") as f:
            json.dump(sample_phase2_response, f, ensure_ascii=False)

        result = check_phase2_completeness(
            str(project_dir / "data" / "batch_map.json"))
        assert result["coverage_pct"] == 50.0
        assert result["reviews_missing"] == 1
        assert "2_surgical_systems" in result["missing_slugs"]

    def test_missing_batch_map(self, tmp_path):
        result = check_phase2_completeness(str(tmp_path / "nonexistent.json"))
        assert "error" in result


# ========================================================
# Main
# ========================================================

class TestMain:
    def test_returns_true_on_pass(self, project_dir, monkeypatch, sample_phase2_response):
        monkeypatch.setattr("scripts.quality_gate.PHASE1_DIR",
                            project_dir / "data" / "phase1")
        monkeypatch.setattr("scripts.quality_gate.PHASE2_DIR",
                            project_dir / "data" / "phase2")
        monkeypatch.setattr("scripts.quality_gate.BATCH_MAP",
                            str(project_dir / "data" / "batch_map.json"))

        # Create review file
        phase2_dir = project_dir / "data" / "phase2"
        with open(phase2_dir / "1_oncology_review.json", "w", encoding="utf-8") as f:
            json.dump(sample_phase2_response, f, ensure_ascii=False)

        result = main(phase="all")
        assert result is True
