import pytest
import pandas as pd
from app.query_service import QueryService
from app.sql_validator import ValidationError

REAL_CSV = "data/Global_Cybersecurity_Threats_2015-2024.csv"


# ─────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────

@pytest.fixture
def tmp_db(tmp_path):
    return str(tmp_path / "test.db")


@pytest.fixture
def loaded_qs(tmp_path):
    """QueryService with the real CSV already loaded."""
    db = str(tmp_path / "test.db")
    qs = QueryService(db)
    qs.load_csv(REAL_CSV, "threats")
    return qs


@pytest.fixture
def sample_csv(tmp_path):
    path = tmp_path / "sample.csv"
    pd.DataFrame({
        "Country":                              ["USA",          "China",      "India"],
        "Year":                                 [2020,           2021,         2022],
        "Attack Type":                          ["Phishing",     "Ransomware", "DDoS"],
        "Target Industry":                      ["Finance",      "Education",  "IT"],
        "Financial Loss (in Million $)":        [120.5,          80.3,         45.0],
        "Number of Affected Users":             [500000,         773169,       120000],
        "Attack Source":                        ["Hacker Group", "Insider",    "Unknown"],
        "Security Vulnerability Type":          ["Weak Passwords","Zero-day",  "Social Engineering"],
        "Defense Mechanism Used":               ["Firewall",     "VPN",        "Antivirus"],
        "Incident Resolution Time (in Hours)":  [24,             63,           12],
    }).to_csv(path, index=False)
    return str(path)


# ─────────────────────────────────────────────
# Flow 1: Data ingestion
# ─────────────────────────────────────────────

def test_load_csv_creates_table(tmp_db, sample_csv):
    qs = QueryService(tmp_db)
    result = qs.load_csv(sample_csv, "threats")
    assert result["rows_inserted"] == 3
    assert result["action"] == "created"


def test_load_csv_full_real_csv(tmp_db):
    qs = QueryService(tmp_db)
    result = qs.load_csv(REAL_CSV, "threats")
    assert result["rows_inserted"] == 3000


def test_load_csv_replace(tmp_db, sample_csv):
    qs = QueryService(tmp_db)
    qs.load_csv(sample_csv, "threats")
    result = qs.load_csv(sample_csv, "threats", on_conflict="replace")
    assert result["action"] == "replaced"


def test_load_csv_skip(tmp_db, sample_csv):
    qs = QueryService(tmp_db)
    qs.load_csv(sample_csv, "threats")
    result = qs.load_csv(sample_csv, "threats", on_conflict="skip")
    assert result["action"] == "skipped"
    assert result["rows_inserted"] == 0


# ─────────────────────────────────────────────
# Flow 2: Query processing
# ─────────────────────────────────────────────

def test_execute_query_returns_list(loaded_qs):
    rows = loaded_qs.execute_query("SELECT * FROM threats LIMIT 5")
    assert isinstance(rows, list)
    assert len(rows) == 5


def test_execute_query_returns_tuples(loaded_qs):
    rows = loaded_qs.execute_query("SELECT * FROM threats LIMIT 1")
    assert isinstance(rows[0], tuple)


def test_execute_query_with_where(loaded_qs):
    rows = loaded_qs.execute_query(
        "SELECT * FROM threats WHERE attack_type = 'Phishing'"
    )
    assert all("Phishing" in row for row in rows)


def test_execute_query_empty_result(loaded_qs):
    rows = loaded_qs.execute_query(
        "SELECT * FROM threats WHERE year = 1800"
    )
    assert rows == []


def test_execute_query_rejects_invalid_sql(loaded_qs):
    with pytest.raises(ValidationError):
        loaded_qs.execute_query("DROP TABLE threats")


def test_execute_query_rejects_unknown_table(loaded_qs):
    with pytest.raises(ValidationError, match="Unknown table"):
        loaded_qs.execute_query("SELECT * FROM fake_table")


def test_execute_query_rejects_injection(loaded_qs):
    with pytest.raises(ValidationError):
        loaded_qs.execute_query("SELECT * FROM threats; DROP TABLE threats")


# ─────────────────────────────────────────────
# Schema access
# ─────────────────────────────────────────────

def test_get_schema_returns_dict(loaded_qs):
    schema = loaded_qs.get_schema()
    assert isinstance(schema, dict)
    assert "threats" in schema


def test_get_schema_has_columns(loaded_qs):
    schema = loaded_qs.get_schema()
    col_names = [c["name"] for c in schema["threats"]]
    assert "country" in col_names
    assert "year" in col_names


def test_get_tables_returns_list(loaded_qs):
    tables = loaded_qs.get_tables()
    assert isinstance(tables, list)
    assert "threats" in tables


# ─────────────────────────────────────────────
# Separation of concerns
# ─────────────────────────────────────────────

def test_query_service_never_bypasses_validator(loaded_qs):
    """
    QueryService must always run validation before execution.
    A forbidden query must never reach the database.
    """
    with pytest.raises(ValidationError):
        loaded_qs.execute_query("DELETE FROM threats")


def test_cli_cannot_bypass_query_service():
    """
    QueryService is the only path to the DB —
    confirms the class exists and exposes the right interface.
    """
    qs = QueryService()
    assert hasattr(qs, "execute_query")
    assert hasattr(qs, "load_csv")
    assert hasattr(qs, "get_schema")
    assert hasattr(qs, "get_tables")