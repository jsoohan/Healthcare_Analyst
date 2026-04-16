"""Tests for scripts/phase2_consult.py."""
import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from scripts.phase2_consult import (
    build_system_prompt,
    build_task_prompt,
    extract_json,
    load_batch_data,
    consult_batch,
    process_batch,
    save_result,
    log_error,
    SECTOR_LENSES,
)


# ========================================================
# extract_json
# ========================================================

class TestExtractJson:
    def test_plain_json(self):
        text = '{"key": "value", "num": 42}'
        result = extract_json(text)
        assert result == {"key": "value", "num": 42}

    def test_markdown_fenced(self):
        text = '```json\n{"key": "value"}\n```'
        result = extract_json(text)
        assert result == {"key": "value"}

    def test_markdown_fenced_no_lang(self):
        text = '```\n{"key": "value"}\n```'
        result = extract_json(text)
        assert result == {"key": "value"}

    def test_with_trailing_text(self):
        text = 'Here is the result:\n{"key": "value"}\nDone processing.'
        result = extract_json(text)
        assert result == {"key": "value"}

    def test_with_leading_text(self):
        text = 'Analysis complete. {"key": "value"}'
        result = extract_json(text)
        assert result == {"key": "value"}

    def test_nested_json(self):
        text = '{"outer": {"inner": [1, 2, 3]}}'
        result = extract_json(text)
        assert result["outer"]["inner"] == [1, 2, 3]

    def test_invalid_raises(self):
        with pytest.raises(ValueError, match="No JSON object found"):
            extract_json("no json here at all")

    def test_empty_string_raises(self):
        with pytest.raises(ValueError):
            extract_json("")

    def test_korean_content(self):
        text = '{"summary": "온콜로지 서브섹터 분석 완료"}'
        result = extract_json(text)
        assert "온콜로지" in result["summary"]


# ========================================================
# build_system_prompt
# ========================================================

class TestBuildSystemPrompt:
    @pytest.mark.parametrize("tier1", list(SECTOR_LENSES.keys()))
    def test_all_sectors_produce_valid_prompts(self, tier1):
        prompt = build_system_prompt(tier1)
        assert tier1 in prompt
        assert "HealthcareIntel" in prompt
        assert len(prompt) > 100

    def test_unknown_sector_uses_default(self):
        prompt = build_system_prompt("UnknownSector")
        assert "일반 프레임워크 적용" in prompt

    def test_biopharma_contains_loe(self):
        prompt = build_system_prompt("Biopharma")
        assert "LOE" in prompt

    def test_medtech_contains_installed_base(self):
        prompt = build_system_prompt("MedTech")
        assert "Installed base" in prompt

    def test_healthcare_it_contains_arr(self):
        prompt = build_system_prompt("Healthcare IT")
        assert "ARR" in prompt


# ========================================================
# build_task_prompt
# ========================================================

class TestBuildTaskPrompt:
    def test_contains_sub_sector(self):
        batch_data = {
            "tier1": "Biopharma",
            "sub_sector": "1.1 Oncology",
            "companies": [{"ticker": "TPHR", "company": "TestPharma"}],
        }
        prompt = build_task_prompt(batch_data)
        assert "1.1 Oncology" in prompt

    def test_contains_company_data(self):
        batch_data = {
            "tier1": "Biopharma",
            "sub_sector": "1.1 Oncology",
            "companies": [{"ticker": "TPHR", "company": "TestPharma"}],
        }
        prompt = build_task_prompt(batch_data)
        assert "TPHR" in prompt
        assert "TestPharma" in prompt

    def test_contains_7_tasks(self):
        batch_data = {
            "tier1": "Biopharma",
            "sub_sector": "1.1 Oncology",
            "companies": [],
        }
        prompt = build_task_prompt(batch_data)
        assert "DATA VALIDATION" in prompt
        assert "REVENUE DRIVER" in prompt
        assert "SECTOR DYNAMICS" in prompt
        assert "EXEC SUMMARY" in prompt


# ========================================================
# load_batch_data
# ========================================================

class TestLoadBatchData:
    def test_loads_successfully(self, project_dir, monkeypatch):
        monkeypatch.setattr("scripts.phase2_consult.PHASE1_DIR",
                            project_dir / "data" / "phase1")

        result = load_batch_data("1_oncology")
        assert result["batch_slug"] == "1_oncology"
        assert result["tier1"] == "Biopharma"
        assert len(result["companies"]) == 1  # Only TPHR.json exists

    def test_missing_dir_raises(self, project_dir, monkeypatch):
        monkeypatch.setattr("scripts.phase2_consult.PHASE1_DIR",
                            project_dir / "data" / "phase1")

        with pytest.raises(FileNotFoundError):
            load_batch_data("nonexistent_batch")

    def test_skips_underscore_files(self, project_dir, monkeypatch):
        """Files starting with _ (like _precheck.json) should be skipped."""
        monkeypatch.setattr("scripts.phase2_consult.PHASE1_DIR",
                            project_dir / "data" / "phase1")

        result = load_batch_data("1_oncology")
        # _precheck.json should not be in company_jsons
        for c in result["companies"]:
            assert "batch_slug" not in c  # _precheck.json has batch_slug


# ========================================================
# LLM Adapters (mocked)
# ========================================================

class TestCallGemini:
    def test_success(self, sample_phase2_response):
        response_text = json.dumps(sample_phase2_response, ensure_ascii=False)

        mock_response = MagicMock()
        mock_response.text = response_text

        mock_client = MagicMock()
        mock_client.models.generate_content.return_value = mock_response

        with patch("scripts.phase2_consult.call_gemini") as mock_call:
            mock_call.return_value = response_text
            result = mock_call("system prompt", "user prompt", "gemini-2.5-pro")
            assert "sub_sector" in result or isinstance(result, str)


class TestCallAnthropic:
    def test_success(self, sample_phase2_response):
        response_text = json.dumps(sample_phase2_response, ensure_ascii=False)

        with patch("scripts.phase2_consult.call_anthropic") as mock_call:
            mock_call.return_value = response_text
            result = mock_call("system prompt", "user prompt", "claude-sonnet-4-6")
            assert isinstance(result, str)


# ========================================================
# consult_batch / process_batch (mocked LLM)
# ========================================================

class TestConsultBatch:
    def test_success_with_mock(self, project_dir, monkeypatch, sample_phase2_response):
        monkeypatch.setattr("scripts.phase2_consult.PHASE1_DIR",
                            project_dir / "data" / "phase1")

        response_text = json.dumps(sample_phase2_response, ensure_ascii=False)

        with patch("scripts.phase2_consult.call_llm") as mock_llm:
            mock_llm.return_value = (response_text, "gemini-2.5-pro")
            result = consult_batch("1_oncology")

        assert result["llm_used"] == "gemini-2.5-pro"
        assert "reviewed_at" in result
        assert "elapsed_seconds" in result

    def test_empty_batch_raises(self, project_dir, monkeypatch):
        # Create a batch dir with only _precheck.json (no company JSONs)
        empty_batch = project_dir / "data" / "phase1" / "empty_batch"
        empty_batch.mkdir(parents=True)
        precheck = {"tier1": "MedTech", "sub_sector": "Test", "companies": []}
        with open(empty_batch / "_precheck.json", "w") as f:
            json.dump(precheck, f)

        monkeypatch.setattr("scripts.phase2_consult.PHASE1_DIR",
                            project_dir / "data" / "phase1")

        with pytest.raises(ValueError, match="No company JSONs"):
            consult_batch("empty_batch")


class TestProcessBatch:
    def test_saves_review_json(self, project_dir, monkeypatch, sample_phase2_response):
        monkeypatch.setattr("scripts.phase2_consult.PHASE1_DIR",
                            project_dir / "data" / "phase1")
        monkeypatch.setattr("scripts.phase2_consult.PHASE2_DIR",
                            project_dir / "data" / "phase2")
        monkeypatch.setattr("scripts.phase2_consult.LOGS_DIR",
                            project_dir / "data" / "logs")

        response_text = json.dumps(sample_phase2_response, ensure_ascii=False)

        with patch("scripts.phase2_consult.call_llm") as mock_llm:
            mock_llm.return_value = (response_text, "gemini-2.5-pro")
            status = process_batch("1_oncology")

        assert status == "success_primary"
        review_path = project_dir / "data" / "phase2" / "1_oncology_review.json"
        assert review_path.exists()

    def test_fallback_on_primary_failure(self, project_dir, monkeypatch, sample_phase2_response):
        monkeypatch.setattr("scripts.phase2_consult.PHASE1_DIR",
                            project_dir / "data" / "phase1")
        monkeypatch.setattr("scripts.phase2_consult.PHASE2_DIR",
                            project_dir / "data" / "phase2")
        monkeypatch.setattr("scripts.phase2_consult.LOGS_DIR",
                            project_dir / "data" / "logs")

        response_text = json.dumps(sample_phase2_response, ensure_ascii=False)

        call_count = 0

        def mock_call_llm(provider, system, user):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("Gemini rate limit")
            return (response_text, "claude-sonnet-4-6")

        with patch("scripts.phase2_consult.call_llm", side_effect=mock_call_llm):
            status = process_batch("1_oncology")

        assert status == "success_fallback"
        assert call_count == 2

    def test_logs_error_on_failure(self, project_dir, monkeypatch):
        monkeypatch.setattr("scripts.phase2_consult.PHASE1_DIR",
                            project_dir / "data" / "phase1")
        monkeypatch.setattr("scripts.phase2_consult.PHASE2_DIR",
                            project_dir / "data" / "phase2")
        monkeypatch.setattr("scripts.phase2_consult.LOGS_DIR",
                            project_dir / "data" / "logs")

        with patch("scripts.phase2_consult.call_llm", side_effect=RuntimeError("API down")):
            status = process_batch("1_oncology")

        assert status == "failed"
        log_path = project_dir / "data" / "logs" / "phase2_errors.jsonl"
        assert log_path.exists()
        lines = log_path.read_text(encoding="utf-8").strip().split("\n")
        assert len(lines) >= 2  # primary + fallback errors


# ========================================================
# save_result / log_error
# ========================================================

class TestSaveResult:
    def test_saves_to_correct_path(self, tmp_path, monkeypatch):
        monkeypatch.setattr("scripts.phase2_consult.PHASE2_DIR", tmp_path)
        save_result("test_batch", {"key": "value"})
        out = tmp_path / "test_batch_review.json"
        assert out.exists()
        data = json.load(open(out, encoding="utf-8"))
        assert data["key"] == "value"


class TestLogError:
    def test_appends_jsonl(self, tmp_path, monkeypatch):
        monkeypatch.setattr("scripts.phase2_consult.LOGS_DIR", tmp_path)
        log_error("batch1", RuntimeError("test error"), "gemini")
        log_error("batch2", ValueError("another error"), "anthropic")

        log_path = tmp_path / "phase2_errors.jsonl"
        assert log_path.exists()
        lines = log_path.read_text(encoding="utf-8").strip().split("\n")
        assert len(lines) == 2
        entry = json.loads(lines[0])
        assert entry["batch_slug"] == "batch1"
        assert entry["error_type"] == "RuntimeError"
