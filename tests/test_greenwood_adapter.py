"""Tests for scripts/greenwood_adapter.py."""
from pathlib import Path

import pytest

from scripts.greenwood_adapter import (
    quarter_to_period,
    period_dir_name,
    discover_sources,
    list_all_tickers,
    check_sources_bundle,
    sanitize_sector_name,
    make_output_path,
    MIN_TRANSCRIPT_SIZE,
    MIN_IR_SIZE,
    MIN_RELEASE_SIZE,
)


class TestPeriodConversion:
    def test_q4_to_fy(self):
        assert quarter_to_period("Q4_2025") == "2025FY"

    def test_q1_retains_q(self):
        assert quarter_to_period("Q1_2026") == "2026Q1"

    def test_q2_retains_q(self):
        assert quarter_to_period("Q2_2026") == "2026Q2"

    def test_already_period(self):
        assert quarter_to_period("2025FY") == "2025FY"

    def test_period_dir_fy(self):
        assert period_dir_name("2025FY") == "2025_FY"

    def test_period_dir_quarterly(self):
        assert period_dir_name("2026Q1") == "2026_Q1"


@pytest.fixture
def greenwood_tree(tmp_path):
    """Build a fake Greenwood folder tree with a few tickers."""
    base = tmp_path / "Earnings"
    fy_dir = base / "2025_FY"

    biopharma = fy_dir / "Biopharma"
    biopharma.mkdir(parents=True)

    abbv = biopharma / "ABBV"
    abbv.mkdir()
    (abbv / "ABBV_2025FY_Transcript.txt").write_text(
        "Operator\n" + ("Transcript body. " * 200), encoding="utf-8")
    (abbv / "ABBV_2025FY_EarningsRelease.txt").write_text(
        "Q4 2025 earnings release " * 50, encoding="utf-8")
    (abbv / "ABBV_2025FY_EarningsRelease.htm").write_text(
        "<html><body>AbbVie Q4 2025 release</body></html>", encoding="utf-8")
    (abbv / "ABBV_2025FY_Presentation.pdf").write_bytes(b"%PDF-1.4 " + b"x" * 6000)

    alny = biopharma / "ALNY"
    alny.mkdir()
    (alny / "ALNY_2025FY_Transcript.txt").write_text(
        "Operator\n" + ("Transcript body. " * 200), encoding="utf-8")

    medtech = fy_dir / "MedTech"
    medtech.mkdir()
    isrg = medtech / "ISRG"
    isrg.mkdir()
    (isrg / "ISRG_2025FY_Transcript.txt").write_text(
        "Operator\n" + ("Transcript body. " * 200), encoding="utf-8")
    (isrg / "ISRG_2025FY_Presentation.pdf").write_bytes(b"%PDF-1.4 " + b"x" * 6000)

    return base


class TestDiscoverSources:
    def test_full_bundle(self, greenwood_tree):
        result = discover_sources("ABBV", "2025FY", str(greenwood_tree))
        assert result["transcript"] is not None
        assert result["earnings_release"] is not None
        assert result["earnings_release_html"] is not None
        assert result["ir_presentation"] is not None
        assert result["sector_found"] == "Biopharma"

    def test_partial_transcript_only(self, greenwood_tree):
        result = discover_sources("ALNY", "2025FY", str(greenwood_tree))
        assert result["transcript"] is not None
        assert result["earnings_release"] is None
        assert result["ir_presentation"] is None
        assert result["sector_found"] == "Biopharma"

    def test_transcript_plus_ir(self, greenwood_tree):
        result = discover_sources("ISRG", "2025FY", str(greenwood_tree))
        assert result["transcript"] is not None
        assert result["ir_presentation"] is not None
        assert result["sector_found"] == "MedTech"

    def test_missing_ticker(self, greenwood_tree):
        result = discover_sources("XXXX", "2025FY", str(greenwood_tree))
        assert result["transcript"] is None
        assert result["sector_found"] is None

    def test_missing_period(self, greenwood_tree):
        result = discover_sources("ABBV", "2026Q1", str(greenwood_tree))
        assert result["transcript"] is None

    def test_case_insensitive_fallback(self, greenwood_tree):
        result = discover_sources("abbv", "2025FY", str(greenwood_tree))
        assert result["transcript"] is not None

    def test_company_name_preferred_over_ticker(self, tmp_path):
        """New canonical layout uses company name; ticker folder is legacy fallback."""
        base = tmp_path / "Earnings"
        fy = base / "2025_FY" / "Biopharma"

        # Company-name folder (new)
        company_dir = fy / "AbbVie"
        company_dir.mkdir(parents=True)
        (company_dir / "AbbVie_2025FY_Transcript.txt").write_text(
            "Operator\n" + ("Transcript body. " * 200), encoding="utf-8")

        # Ticker folder (legacy) — also present but should NOT be picked first
        ticker_dir = fy / "ABBV"
        ticker_dir.mkdir()
        (ticker_dir / "ABBV_2025FY_Transcript.txt").write_text(
            "LEGACY should not be chosen " * 200, encoding="utf-8")

        result = discover_sources(
            "ABBV", "2025FY", str(base), company_name="AbbVie")
        assert "AbbVie" in result["transcript"]
        assert "ABBV_2025FY" not in result["transcript"]

    def test_ticker_fallback_when_no_company_name(self, greenwood_tree):
        """If only ticker folder exists, discover_sources still works."""
        result = discover_sources(
            "ABBV", "2025FY", str(greenwood_tree), company_name="AbbVie")
        # No AbbVie folder exists in fixture, should fall back to ABBV
        assert result["transcript"] is not None


class TestListAllTickers:
    def test_lists_all(self, greenwood_tree):
        entries = list_all_tickers(str(greenwood_tree), "2025FY")
        tickers = [e["ticker"] for e in entries]
        assert "ABBV" in tickers
        assert "ALNY" in tickers
        assert "ISRG" in tickers
        assert len(entries) == 3

    def test_empty_period(self, greenwood_tree):
        entries = list_all_tickers(str(greenwood_tree), "2026Q1")
        assert entries == []


class TestSanitizeSectorName:
    def test_basic(self):
        assert sanitize_sector_name("Biopharma") == "Biopharma"

    def test_strips_number_prefix(self):
        assert sanitize_sector_name("1. Biopharma") == "Biopharma"
        assert sanitize_sector_name("9 Dentistry") == "Dentistry"

    def test_ampersand_to_and(self):
        assert sanitize_sector_name("4. Biologics Tools & Services") == "Biologics_Tools_and_Services"

    def test_empty(self):
        assert sanitize_sector_name("") == ""


class TestMakeOutputPath:
    def test_transcript_q4_by_company_name(self, tmp_path):
        p = make_output_path("AbbVie", "2025FY", "1. Biopharma",
                              "Transcript", ".txt", str(tmp_path))
        expected = (tmp_path / "2025_FY" / "Biopharma" / "AbbVie"
                    / "AbbVie_2025FY_Transcript.txt")
        assert p == expected

    def test_ir_presentation_pdf(self, tmp_path):
        p = make_output_path("10x Genomics", "2025FY",
                              "4. Biologics Tools & Services",
                              "Presentation", ".pdf", str(tmp_path))
        expected = (tmp_path / "2025_FY" / "Biologics_Tools_and_Services"
                    / "10x Genomics"
                    / "10x Genomics_2025FY_Presentation.pdf")
        assert p == expected

    def test_quarterly_period(self, tmp_path):
        p = make_output_path("AbbVie", "2026Q1", "Biopharma",
                              "Transcript", ".txt", str(tmp_path))
        expected = (tmp_path / "2026_Q1" / "Biopharma" / "AbbVie"
                    / "AbbVie_2026Q1_Transcript.txt")
        assert p == expected

    def test_empty_sector_uses_unmapped(self, tmp_path):
        p = make_output_path("UnknownCo", "2025FY", "",
                              "Transcript", ".txt", str(tmp_path))
        assert "_unmapped" in str(p)

    def test_company_with_special_chars(self, tmp_path):
        """Johnson & Johnson should keep ampersand (only path-hostile chars stripped)."""
        p = make_output_path("Johnson & Johnson", "2025FY", "Biopharma",
                              "Transcript", ".txt", str(tmp_path))
        assert "Johnson & Johnson" in str(p)


class TestCheckSourcesBundle:
    def test_ready_when_transcript_and_release(self):
        src = {"transcript": "/x.txt", "earnings_release": "/y.txt",
               "ir_presentation": None, "filings": []}
        assert check_sources_bundle(src) == "READY"

    def test_ready_when_transcript_and_ir(self):
        src = {"transcript": "/x.txt", "earnings_release": None,
               "ir_presentation": "/y.pdf", "filings": []}
        assert check_sources_bundle(src) == "READY"

    def test_partial_when_release_only(self):
        src = {"transcript": None, "earnings_release": "/y.txt",
               "ir_presentation": None, "filings": []}
        assert check_sources_bundle(src) == "PARTIAL"

    def test_partial_when_transcript_only(self):
        src = {"transcript": "/x.txt", "earnings_release": None,
               "ir_presentation": None, "filings": []}
        assert check_sources_bundle(src) == "PARTIAL"

    def test_skip_when_nothing(self):
        src = {"transcript": None, "earnings_release": None,
               "ir_presentation": None, "filings": []}
        assert check_sources_bundle(src) == "SKIP"
