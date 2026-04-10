"""Tests for scripts/build_batch_map.py."""
import json
from pathlib import Path

import pytest

from scripts.build_batch_map import slugify, load_sheet, main
from tests.conftest import create_sample_excel


class TestSlugify:
    def test_basic(self):
        assert slugify("1. Biopharma") == "1_biopharma"

    def test_special_chars(self):
        # & is removed by [^\w\s-] regex, adjacent spaces collapsed by [\s.]+ → single _
        assert slugify("Biologics Tools & Services") == "biologics_tools_services"

    def test_dots_replaced(self):
        # . is removed by [^\w\s-], leaving "22 Surgical Systems"
        assert slugify("2.2 Surgical Systems") == "22_surgical_systems"

    def test_trailing_spaces(self):
        assert slugify("  MedTech  ") == "medtech"

    def test_empty_string(self):
        assert slugify("") == ""

    def test_numeric_prefix(self):
        result = slugify("1_Oncology")
        assert "oncology" in result


class TestLoadSheet:
    def test_filters_nan_rows(self, tmp_path):
        excel_path = create_sample_excel(tmp_path / "test.xlsx")
        import pandas as pd
        xls = pd.ExcelFile(excel_path)
        df = load_sheet(xls, "1. Biopharma")
        assert len(df) == 3  # 3 valid rows
        assert all(df["Ticker"].notna())

    def test_column_names(self, tmp_path):
        excel_path = create_sample_excel(tmp_path / "test.xlsx")
        import pandas as pd
        xls = pd.ExcelFile(excel_path)
        df = load_sheet(xls, "1. Biopharma")
        assert "Company" in df.columns
        assert "Ticker" in df.columns
        assert "Sub-sector" in df.columns


class TestMain:
    def test_creates_batch_map(self, tmp_path, monkeypatch):
        excel_path = create_sample_excel(tmp_path / "test.xlsx")
        output_path = tmp_path / "data" / "batch_map.json"
        (tmp_path / "data").mkdir(parents=True, exist_ok=True)

        monkeypatch.setattr("scripts.build_batch_map.DB_PATH", str(excel_path))
        monkeypatch.setattr("scripts.build_batch_map.OUTPUT", str(output_path))

        main()

        assert output_path.exists()
        data = json.load(open(output_path, encoding="utf-8"))
        assert "summary" in data
        assert "batches" in data

    def test_summary_counts(self, tmp_path, monkeypatch):
        excel_path = create_sample_excel(tmp_path / "test.xlsx")
        output_path = tmp_path / "data" / "batch_map.json"
        (tmp_path / "data").mkdir(parents=True, exist_ok=True)

        monkeypatch.setattr("scripts.build_batch_map.DB_PATH", str(excel_path))
        monkeypatch.setattr("scripts.build_batch_map.OUTPUT", str(output_path))

        main()

        data = json.load(open(output_path, encoding="utf-8"))
        summary = data["summary"]
        assert summary["total_companies"] == 5  # 3 biopharma + 2 medtech
        assert "Biopharma" in summary["by_tier1"]
        assert "MedTech" in summary["by_tier1"]

    def test_handles_missing_sheet(self, tmp_path, monkeypatch):
        """Sheets not in Excel are silently skipped."""
        excel_path = create_sample_excel(tmp_path / "test.xlsx")
        output_path = tmp_path / "data" / "batch_map.json"
        (tmp_path / "data").mkdir(parents=True, exist_ok=True)

        monkeypatch.setattr("scripts.build_batch_map.DB_PATH", str(excel_path))
        monkeypatch.setattr("scripts.build_batch_map.OUTPUT", str(output_path))

        main()

        data = json.load(open(output_path, encoding="utf-8"))
        # Only 2 sheets exist (Biopharma, MedTech), others are skipped
        tier1_names = set()
        for b in data["batches"].values():
            tier1_names.add(b["tier1"])
        assert tier1_names == {"Biopharma", "MedTech"}

    def test_company_fields(self, tmp_path, monkeypatch):
        excel_path = create_sample_excel(tmp_path / "test.xlsx")
        output_path = tmp_path / "data" / "batch_map.json"
        (tmp_path / "data").mkdir(parents=True, exist_ok=True)

        monkeypatch.setattr("scripts.build_batch_map.DB_PATH", str(excel_path))
        monkeypatch.setattr("scripts.build_batch_map.OUTPUT", str(output_path))

        main()

        data = json.load(open(output_path, encoding="utf-8"))
        # Find any company and check fields
        first_batch = next(iter(data["batches"].values()))
        company = first_batch["companies"][0]
        assert "company" in company
        assert "ticker" in company
        assert "exchange" in company
        assert "mkt_cap" in company
        assert "is_new" in company

    def test_is_new_star_detection(self, tmp_path, monkeypatch):
        excel_path = create_sample_excel(tmp_path / "test.xlsx")
        output_path = tmp_path / "data" / "batch_map.json"
        (tmp_path / "data").mkdir(parents=True, exist_ok=True)

        monkeypatch.setattr("scripts.build_batch_map.DB_PATH", str(excel_path))
        monkeypatch.setattr("scripts.build_batch_map.OUTPUT", str(output_path))

        main()

        data = json.load(open(output_path, encoding="utf-8"))
        # BioGen Corp. has NEW=★
        all_companies = []
        for b in data["batches"].values():
            all_companies.extend(b["companies"])

        bgen = [c for c in all_companies if c["ticker"] == "BGEN"]
        assert len(bgen) == 1
        assert bgen[0]["is_new"] is True

        tphr = [c for c in all_companies if c["ticker"] == "TPHR"]
        assert len(tphr) == 1
        assert tphr[0]["is_new"] is False
