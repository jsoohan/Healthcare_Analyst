"""Shared fixtures for HealthcareIntel tests."""
import json
import shutil
from pathlib import Path

import pytest

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def fixtures_dir():
    return FIXTURES_DIR


@pytest.fixture
def sample_batch_map():
    with open(FIXTURES_DIR / "sample_batch_map.json", encoding="utf-8") as f:
        return json.load(f)


@pytest.fixture
def sample_precheck():
    with open(FIXTURES_DIR / "sample_precheck.json", encoding="utf-8") as f:
        return json.load(f)


@pytest.fixture
def sample_phase1_company():
    with open(FIXTURES_DIR / "sample_phase1_company.json", encoding="utf-8") as f:
        return json.load(f)


@pytest.fixture
def sample_phase2_response():
    with open(FIXTURES_DIR / "sample_phase2_response.json", encoding="utf-8") as f:
        return json.load(f)


@pytest.fixture
def sample_transcript_text():
    with open(FIXTURES_DIR / "sample_transcript.txt", encoding="utf-8") as f:
        return f.read()


@pytest.fixture
def project_dir(tmp_path, sample_batch_map, sample_precheck, sample_phase1_company):
    """Create a temporary project directory mimicking the real layout."""
    # data directories
    data_dir = tmp_path / "data"
    (data_dir / "phase1" / "1_oncology").mkdir(parents=True)
    (data_dir / "phase2").mkdir(parents=True)
    (data_dir / "logs").mkdir(parents=True)

    # batch_map.json
    with open(data_dir / "batch_map.json", "w", encoding="utf-8") as f:
        json.dump(sample_batch_map, f, ensure_ascii=False, indent=2)

    # precheck
    with open(data_dir / "phase1" / "1_oncology" / "_precheck.json", "w", encoding="utf-8") as f:
        json.dump(sample_precheck, f, ensure_ascii=False, indent=2)

    # company JSON
    with open(data_dir / "phase1" / "1_oncology" / "TPHR.json", "w", encoding="utf-8") as f:
        json.dump(sample_phase1_company, f, ensure_ascii=False, indent=2)

    # transcript directory
    transcript_dir = tmp_path / "transcripts_EC_Q4_2025"
    transcript_dir.mkdir()
    shutil.copy(FIXTURES_DIR / "sample_transcript.txt",
                transcript_dir / "TestPharma Inc._EC_Q4_2025.txt")

    # Create a small transcript (under 1KB threshold) for edge case testing
    small_file = transcript_dir / "SmallFile_EC_Q4_2025.txt"
    small_file.write_text("too small", encoding="utf-8")

    # IR directory
    ir_dir = tmp_path / "ir_presentations"
    ir_dir.mkdir()
    # Create a fake PDF (just needs to be > 5000 bytes)
    fake_ir = ir_dir / "TestPharma Inc._Q4_2025.pdf"
    fake_ir.write_bytes(b"%PDF-1.4 fake content " + b"x" * 5100)

    # Filings directory
    filings_dir = tmp_path / "filings"
    filings_dir.mkdir()
    fake_filing = filings_dir / "TPHR_10K_2025.txt"
    fake_filing.write_text("SEC Filing content for TPHR", encoding="utf-8")

    return tmp_path


def create_sample_excel(path: Path):
    """Create a minimal Excel file matching the HealthcareIntel DB format."""
    import openpyxl
    wb = openpyxl.Workbook()

    # Sheet: 1. Biopharma
    ws1 = wb.active
    ws1.title = "1. Biopharma"
    # Header row (row 1 is title, row 2 is header per load_sheet with header=1)
    ws1.append(["HealthcareIntel Database - Biopharma"])
    ws1.append(["Company", "Ticker", "Exchange", "Sub-sector", "Mkt Cap (USD)", "Focus / Notes", "NEW"])
    ws1.append(["TestPharma Inc.", "TPHR", "NASDAQ", "Oncology", "$50B", "Oncology pipeline leader", ""])
    ws1.append(["BioGen Corp.", "BGEN", "NYSE", "Oncology", "$30B", "ADC platform", "\u2605"])
    ws1.append(["ImmunoCo", "IMCO", "NASDAQ", "Immunology", "$20B", "Autoimmune focus", ""])

    # Sheet: 2. MedTech
    ws2 = wb.create_sheet("2. MedTech")
    ws2.append(["HealthcareIntel Database - MedTech"])
    ws2.append(["Company", "Ticker", "Exchange", "Sub-sector", "Mkt Cap (USD)", "Focus / Notes", "NEW"])
    ws2.append(["RoboSurg Ltd.", "RSRG", "NASDAQ", "Surgical Systems", "$80B", "Surgical robotics", ""])
    ws2.append(["MedDevice & Co.", "MDVC", "NYSE", "Orthopedics", "$15B", "Orthopedic implants", ""])

    wb.save(path)
    return path
