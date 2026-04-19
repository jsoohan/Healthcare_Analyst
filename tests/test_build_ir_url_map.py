"""Tests for scripts/build_ir_url_map.py — pure functions + incremental execution."""
import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


class TestUrlUtils:
    def test_is_blocked(self):
        from scripts.build_ir_url_map import is_blocked
        assert is_blocked("https://google.com/search") is True
        assert is_blocked("https://www.morningstar.com/stock") is True
        assert is_blocked("https://ir.pfizer.com/") is False
        assert is_blocked("https://investors.abbvie.com/") is False

    def test_extract_google_url(self):
        from scripts.build_ir_url_map import extract_google_url
        assert extract_google_url("/url?q=https%3A//ir.example.com/page&sa=U") == "https://ir.example.com/page"
        assert extract_google_url("https://direct.com/page") == "https://direct.com/page"
        assert extract_google_url("/search?q=foo") is None


class TestLoadSaveMap:
    def test_load_existing_map(self, tmp_path):
        from scripts.build_ir_url_map import load_existing_map
        path = tmp_path / "ir_url_map.json"
        data = {"TEST": {"company": "TestCo", "ir_url": "https://ir.test.com"}}
        with open(path, "w") as f:
            json.dump(data, f)

        result = load_existing_map(str(path))
        assert result["TEST"]["ir_url"] == "https://ir.test.com"

    def test_load_missing_returns_empty(self, tmp_path):
        from scripts.build_ir_url_map import load_existing_map
        result = load_existing_map(str(tmp_path / "nonexistent.json"))
        assert result == {}

    def test_save_map(self, tmp_path):
        from scripts.build_ir_url_map import save_map
        data = {"ABBV": {"company": "AbbVie", "ir_url": "https://ir.abbvie.com"}}
        path = tmp_path / "subdir" / "ir_url_map.json"
        save_map(data, str(path))

        assert path.exists()
        loaded = json.load(open(path))
        assert loaded["ABBV"]["ir_url"] == "https://ir.abbvie.com"


class TestVerifyIrUrl:
    def test_verify_success(self):
        from scripts.build_ir_url_map import verify_ir_url
        driver = MagicMock()
        driver.get = MagicMock()
        driver.page_source = """
        <html><body>
        <h1>Investor Relations</h1>
        <p>Latest earnings results and quarterly presentation</p>
        <a href="/sec-filings">SEC Filings</a>
        </body></html>
        """
        assert verify_ir_url(driver, "https://ir.example.com") is True

    def test_verify_fail_no_ir_keywords(self):
        from scripts.build_ir_url_map import verify_ir_url
        driver = MagicMock()
        driver.get = MagicMock()
        driver.page_source = "<html><body><p>Random marketing page</p></body></html>"
        assert verify_ir_url(driver, "https://example.com") is False


class TestDiscoverViaGoogle:
    def test_finds_ir_domain(self):
        from scripts.build_ir_url_map import discover_via_google

        driver = MagicMock()
        driver.get = MagicMock()
        driver.page_source = '''
        <html><body>
          <a href="/url?q=https%3A//ir.abbvie.com/investor-relations&sa=U">AbbVie Investor Relations</a>
          <a href="/url?q=https%3A//morningstar.com/abbv&sa=U">AbbVie on Morningstar</a>
        </body></html>
        '''

        url, method = discover_via_google(driver, "AbbVie Inc", "ABBV")
        assert url is not None
        assert "abbvie" in url.lower()
        assert method == "google_search"

    def test_skips_blocked_domains(self):
        from scripts.build_ir_url_map import discover_via_google

        driver = MagicMock()
        driver.get = MagicMock()
        driver.page_source = '''
        <html><body>
          <a href="/url?q=https%3A//morningstar.com/investor/abbv&sa=U">Morningstar</a>
          <a href="/url?q=https%3A//seekingalpha.com/abbv/investor&sa=U">Seeking Alpha</a>
        </body></html>
        '''

        url, method = discover_via_google(driver, "AbbVie Inc", "ABBV")
        assert url is None


class TestIncremental:
    """Test that re-running skips already-mapped companies."""

    def test_new_companies_are_targeted(self, tmp_path):
        from scripts.build_ir_url_map import load_existing_map

        existing = {
            "ABBV": {"company": "AbbVie", "ir_url": "https://ir.abbvie.com",
                     "verified": True},
        }
        map_path = tmp_path / "ir_url_map.json"
        with open(map_path, "w") as f:
            json.dump(existing, f)

        ir_map = load_existing_map(str(map_path))
        companies = [
            {"ticker": "ABBV", "company_name": "AbbVie"},
            {"ticker": "PFE", "company_name": "Pfizer"},
        ]

        new_targets = [c for c in companies if c["ticker"] not in ir_map
                       or not ir_map[c["ticker"]].get("ir_url")]
        assert len(new_targets) == 1
        assert new_targets[0]["ticker"] == "PFE"

    def test_failed_companies_retried(self, tmp_path):
        from scripts.build_ir_url_map import load_existing_map

        existing = {
            "ABBV": {"company": "AbbVie", "ir_url": None, "verified": False},
        }
        map_path = tmp_path / "ir_url_map.json"
        with open(map_path, "w") as f:
            json.dump(existing, f)

        ir_map = load_existing_map(str(map_path))
        companies = [{"ticker": "ABBV", "company_name": "AbbVie"}]
        targets = [c for c in companies if c["ticker"] not in ir_map
                   or not ir_map[c["ticker"]].get("ir_url")]
        assert len(targets) == 1  # Retried
