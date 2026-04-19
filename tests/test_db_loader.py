"""Tests for scripts/db_loader.py."""
import json
from pathlib import Path

import pytest

from scripts.db_loader import (
    load_companies,
    to_input_csv,
    sanitize,
    find_db_path,
    TIER1_SHEETS,
    US_EXCHANGES,
)
from tests.conftest import create_sample_excel


class TestSanitize:
    def test_basic(self):
        assert sanitize("Pfizer Inc.") == "Pfizer Inc."

    def test_replaces_special_chars(self):
        assert sanitize('A/B:C*D?"E') == "A_B_C_D__E"

    def test_preserves_ampersand(self):
        assert sanitize("Johnson & Johnson") == "Johnson & Johnson"


class TestLoadCompanies:
    def test_loads_all_sheets(self, tmp_path):
        excel_path = create_sample_excel(tmp_path / "test.xlsx")
        companies = load_companies(str(excel_path))
        assert len(companies) == 5  # 3 biopharma + 2 medtech

    def test_keys_present(self, tmp_path):
        excel_path = create_sample_excel(tmp_path / "test.xlsx")
        companies = load_companies(str(excel_path))
        for c in companies:
            for key in ["company_name", "ticker", "exchange", "sector",
                        "sub_sector", "mkt_cap", "focus_notes", "is_new",
                        "search_term"]:
                assert key in c, f"missing key: {key}"

    def test_sector_filter(self, tmp_path):
        excel_path = create_sample_excel(tmp_path / "test.xlsx")
        biopharma = load_companies(str(excel_path), sector_filter="Biopharma")
        assert len(biopharma) == 3
        for c in biopharma:
            assert c["sector"] == "Biopharma"

    def test_search_term_uses_company_name(self, tmp_path):
        """All companies use company name for MarketScreener search."""
        excel_path = create_sample_excel(tmp_path / "test.xlsx")
        companies = load_companies(str(excel_path))
        tphr = [c for c in companies if c["ticker"] == "TPHR"][0]
        assert tphr["search_term"] == "TestPharma Inc."

    def test_search_term_non_us(self, tmp_path):
        import openpyxl
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "1. Biopharma"
        ws.append(["HealthcareIntel"])
        ws.append(["Company", "Ticker", "Exchange", "Sub-sector",
                   "Mkt Cap (USD)", "Focus / Notes", "NEW"])
        ws.append(["Astellas Pharma", "4503", "TSE", "Oncology",
                   "$30B", "Japan biopharma", ""])
        xlsx_path = tmp_path / "test.xlsx"
        wb.save(xlsx_path)

        companies = load_companies(str(xlsx_path))
        assert companies[0]["search_term"] == "Astellas Pharma"

    def test_missing_file_raises(self):
        with pytest.raises(FileNotFoundError):
            load_companies(str(Path("/nonexistent/path.xlsx")))

    def test_is_new_detection(self, tmp_path):
        excel_path = create_sample_excel(tmp_path / "test.xlsx")
        companies = load_companies(str(excel_path))
        bgen = [c for c in companies if c["ticker"] == "BGEN"][0]
        assert bgen["is_new"] is True

    def test_skips_empty_rows(self, tmp_path):
        import openpyxl
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "1. Biopharma"
        ws.append(["HealthcareIntel"])
        ws.append(["Company", "Ticker", "Exchange", "Sub-sector",
                   "Mkt Cap (USD)", "Focus / Notes", "NEW"])
        ws.append(["ValidCo", "VC", "NYSE", "Onco", "$10B", "", ""])
        ws.append([None, None, None, None, None, None, None])
        ws.append(["", "", "", "", "", "", ""])
        xlsx_path = tmp_path / "test.xlsx"
        wb.save(xlsx_path)

        companies = load_companies(str(xlsx_path))
        assert len(companies) == 1
        assert companies[0]["company_name"] == "ValidCo"


class TestToInputCsv:
    def test_writes_format_a(self, tmp_path):
        excel_path = create_sample_excel(tmp_path / "test.xlsx")
        companies = load_companies(str(excel_path))
        csv_path = tmp_path / "input.csv"
        to_input_csv(companies, csv_path)

        assert csv_path.exists()
        import csv as csvmod
        with open(csv_path, encoding="utf-8") as f:
            rows = list(csvmod.DictReader(f))
        assert len(rows) == 5
        assert "Company Name" in rows[0]
        assert "Exchange" in rows[0]
        assert "Ticker" in rows[0]
        assert "Sector" in rows[0]


class TestFindDbPath:
    def test_returns_hint_if_exists(self, tmp_path):
        db = tmp_path / "HealthcareIntel_Database_20260101.xlsx"
        db.write_bytes(b"")
        assert find_db_path(str(db)) == str(db)

    def test_returns_none_when_absent(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        assert find_db_path() is None

    def test_auto_detect_latest(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        (tmp_path / "HealthcareIntel_Database_20260101.xlsx").write_bytes(b"")
        (tmp_path / "HealthcareIntel_Database_20260410.xlsx").write_bytes(b"")
        result = find_db_path()
        assert "20260410" in result


class TestConstants:
    def test_tier1_sheets_has_9(self):
        assert len(TIER1_SHEETS) == 9

    def test_us_exchanges(self):
        assert "NYSE" in US_EXCHANGES
        assert "NASDAQ" in US_EXCHANGES
