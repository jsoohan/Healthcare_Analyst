"""Tests for scripts/collect_transcripts_earnings.py — pure functions + HTML parsing."""
import os
from pathlib import Path
from unittest.mock import MagicMock

import pytest


class TestFileUtils:
    def test_sanitize_shared(self):
        from scripts.collect_transcripts_earnings import sanitize
        assert sanitize('A/B:C') == "A_B_C"

    def test_save_transcript_writes_header(self, tmp_path):
        from scripts.collect_transcripts_earnings import save_transcript
        path = save_transcript(
            str(tmp_path), "TestCo Inc.", "EC_Q4_2025",
            "Q4 2025 Earnings Call Transcript",
            "https://example.com/transcript",
            "Operator: Good afternoon...\nCEO: Thank you..."
        )
        assert os.path.exists(path)
        content = Path(path).read_text(encoding="utf-8")
        assert content.startswith("Company : TestCo Inc.")
        assert "Title   : Q4 2025 Earnings Call Transcript" in content
        assert "Source  : https://example.com/transcript" in content
        assert "=" * 80 in content
        assert "Operator: Good afternoon" in content

    def test_validate_file_pass(self, tmp_path):
        from scripts.collect_transcripts_earnings import validate_file
        p = tmp_path / "t.txt"
        body = "The revenue growth was strong. Earnings guidance raised. " * 100
        p.write_text(body, encoding="utf-8")
        assert validate_file(str(p)) == "PASS"

    def test_validate_file_fail_small(self, tmp_path):
        from scripts.collect_transcripts_earnings import validate_file
        p = tmp_path / "t.txt"
        p.write_text("tiny", encoding="utf-8")
        assert validate_file(str(p)) == "FAIL"

    def test_validate_file_missing(self, tmp_path):
        from scripts.collect_transcripts_earnings import validate_file
        assert validate_file(str(tmp_path / "nonexistent.txt")) == "FAIL"

    def test_validate_file_no_keywords(self, tmp_path):
        from scripts.collect_transcripts_earnings import validate_file
        p = tmp_path / "t.txt"
        p.write_text("random text without any relevant terms. " * 200, encoding="utf-8")
        assert validate_file(str(p)) == "WARN_no_kw"

    def test_already_collected_true(self, tmp_path):
        from scripts.collect_transcripts_earnings import already_collected
        p = tmp_path / "TestCo_EC_Q4_2025.txt"
        p.write_text("x" * 2000, encoding="utf-8")
        assert already_collected(str(tmp_path), "TestCo", "EC_Q4_2025") is True

    def test_already_collected_false_too_small(self, tmp_path):
        from scripts.collect_transcripts_earnings import already_collected
        p = tmp_path / "TestCo_EC_Q4_2025.txt"
        p.write_text("tiny", encoding="utf-8")
        assert already_collected(str(tmp_path), "TestCo", "EC_Q4_2025") is False


class TestProgressTracking:
    def test_load_progress_empty(self, tmp_path):
        from scripts.collect_transcripts_earnings import load_progress
        assert load_progress(str(tmp_path)) == set()

    def test_append_and_load(self, tmp_path):
        from scripts.collect_transcripts_earnings import append_progress, load_progress
        row = {
            "ticker": "TPHR", "company_name": "TestPharma",
            "sector": "Biopharma", "found": "Y", "file_size_kb": 12.0,
            "event_title": "Q4 2025 Earnings Call", "event_date": "2026-01-28",
            "url": "https://marketscreener.com/x", "note": "PASS",
            "collected_at": "2026-04-19",
        }
        append_progress(str(tmp_path), row)
        assert "TPHR" in load_progress(str(tmp_path))


class TestFindEarningsTranscript:
    def test_direct_match_scored_higher(self):
        from scripts.collect_transcripts_earnings import find_earnings_transcript
        html = '''
        <html><body>
          <a href="/news/q4-2025-earnings">Q4 2025 Earnings Call Transcript</a>
          <a href="/news/q3-2025-earnings">Q3 2025 Earnings Call Transcript</a>
          <a href="/news/jpm-healthcare">J.P. Morgan Healthcare Conference</a>
          <a href="/news/results">Full Year Results 2025</a>
        </body></html>
        '''
        driver = MagicMock()
        driver.page_source = html

        results = find_earnings_transcript(driver, "Q4", 2025)
        assert len(results) >= 1
        assert results[0]["score"] == 10
        assert "q4" in results[0]["title"].lower()

    def test_excludes_jpm_conference(self):
        from scripts.collect_transcripts_earnings import find_earnings_transcript
        html = '''
        <html><body>
          <a href="/news/jpm-healthcare">J.P. Morgan Healthcare Conference 2025</a>
        </body></html>
        '''
        driver = MagicMock()
        driver.page_source = html

        results = find_earnings_transcript(driver, "Q4", 2025)
        assert all("j.p. morgan" not in r["title"].lower() for r in results)

    def test_year_only_match_scored_lower(self):
        from scripts.collect_transcripts_earnings import find_earnings_transcript
        html = '''
        <html><body>
          <a href="/news/ec1">Quarterly Results 2025</a>
        </body></html>
        '''
        driver = MagicMock()
        driver.page_source = html
        results = find_earnings_transcript(driver, "Q4", 2025)
        if results:
            assert results[0]["score"] == 5

    def test_no_match_returns_empty(self):
        from scripts.collect_transcripts_earnings import find_earnings_transcript
        driver = MagicMock()
        driver.page_source = '<html><body><a href="/a">Unrelated News</a></body></html>'
        results = find_earnings_transcript(driver, "Q4", 2025)
        assert results == []


class TestIsLoggedIn:
    def test_detects_login_page(self):
        from scripts.collect_transcripts_earnings import is_logged_in
        driver = MagicMock()
        driver.current_url = "https://www.marketscreener.com/login/"
        driver.page_source = "<html></html>"
        assert is_logged_in(driver) is False

    def test_detects_logged_in(self):
        from scripts.collect_transcripts_earnings import is_logged_in
        driver = MagicMock()
        driver.current_url = "https://www.marketscreener.com/quote/stock/TEST/"
        driver.page_source = '<header><div id="user_data_modal">User</div></header>'
        assert is_logged_in(driver) is True

    def test_detects_logged_out_via_header(self):
        from scripts.collect_transcripts_earnings import is_logged_in
        driver = MagicMock()
        driver.current_url = "https://www.marketscreener.com/"
        driver.page_source = '<header><a href="/login/">Login</a></header>'
        assert is_logged_in(driver) is False


class TestLoadCompaniesFromCsv:
    def test_format_a(self, tmp_path):
        from scripts.collect_transcripts_earnings import load_companies_from_csv
        csv_path = tmp_path / "input.csv"
        csv_path.write_text(
            "Company Name,Exchange,Ticker,Sector\n"
            "AbbVie,NYSE,ABBV,Biopharma\n"
            "Astellas,TSE,4503,Biopharma\n",
            encoding="utf-8"
        )
        companies = load_companies_from_csv(str(csv_path))
        assert len(companies) == 2
        abbv = [c for c in companies if c["ticker"] == "ABBV"][0]
        assert abbv["search_term"] == "ABBV"
        astellas = [c for c in companies if c["ticker"] == "4503"][0]
        assert astellas["search_term"] == "Astellas"

    def test_format_b(self, tmp_path):
        from scripts.collect_transcripts_earnings import load_companies_from_csv
        csv_path = tmp_path / "input.csv"
        csv_path.write_text(
            "ticker,company_name,search_term,sector\n"
            "ABBV,AbbVie,ABBV,Biopharma\n",
            encoding="utf-8"
        )
        companies = load_companies_from_csv(str(csv_path))
        assert len(companies) == 1
        assert companies[0]["search_term"] == "ABBV"
