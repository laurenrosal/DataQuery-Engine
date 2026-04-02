import pytest
import pandas as pas
from database.database import getconnection
from database.schema_manager import SchemaManager
from database.csv_loader import CSVLoader

REAL_CSV = "data/Global_Cybersecurity_Threats_2015-2024.csv"


@pytest.fixture
def tmp_db(tmp_path):
    return str(tmp_path / "test.db")

@pytest.fixture
def colors_csv(tmp_path):
    csv_path = tmp_path / "colors.csv"

    sample_data = pas.DataFrame({
        "Color Name": ["Red", "Blue", "Green"],
        "Hex Code":   ["#FF0000", "#0000FF", "#00FF00"],
        "R": [255, 0, 0], 
        "G": [0, 0, 255], 
        "B": [0, 255, 0],
    })

    sample_data.to_csv(csv_path, index=False)
    return str(csv_path)

@pytest.fixture
def cyber_csv(tmp_path):
    csv_path = tmp_path / "cyber.csv"
    
    sample_data = pas.DataFrame({
        "Country": ["USA", "China", "India"],
        "Year": [2020, 2021, 2022],
        "Attack Type": ["Phishing", "Ransomware", "DDoS"],
        "Target Industry": ["Finance", "Education", "IT"],
        "Financial Loss (in Million $)": [120.5, 80.3, 45.0],
        "Number of Affected Users": [500000, 773169, 120000],
        "Attack Source": ["Hacker Group", "Insider", "Unknown"],
        "Security Vulnerability Type": ["Weak Passwords", "Zero-day", "Social Engineering"],
        "Defense Mechanism Used": ["Firewall", "VPN", "Antivirus"],
        "Incident Resolution Time (in Hours)": [24, 63, 12],
    })

    sample_data.to_csv(csv_path, index=False)
    return str(csv_path)

def test_can_loads_colors_csv(tmp_db, colors_csv):
    result = CSVLoader(tmp_db).load(colors_csv, "colors")
    
    assert result["rows_inserted"] == 3
    assert result["action"] == "created"


def test_can_loads_cybersecurity_csv(tmp_db, cyber_csv):
    result = CSVLoader(tmp_db).load(cyber_csv, "threats")

    assert result["rows_inserted"] == 3
    assert result["action"] == "created"


def test_loads_real_csv_3000_rows(tmp_db):
    result = CSVLoader(tmp_db).load(REAL_CSV, "threats")
    assert result["rows_inserted"] == 3000


def test_can_two_different_csvs_in_same_db(tmp_db, colors_csv, cyber_csv):
    loader = CSVLoader(tmp_db)
    loader.load(colors_csv, "colors")
    loader.load(cyber_csv, "threats")

    tables = SchemaManager(tmp_db).get_tables()

    assert "colors" in tables
    assert "threats" in tables


def test_column_names_normalized(tmp_db, colors_csv):
    CSVLoader(tmp_db).load(colors_csv, "colors")

    cols = [c["name"] for c in SchemaManager(tmp_db).get_table_schema("colors")]

    assert "color_name" in cols
    assert "hex_code" in cols


def test_id_column_is_added_automatically(tmp_db, colors_csv):
    CSVLoader(tmp_db).load(colors_csv, "colors")

    schema = SchemaManager(tmp_db).get_table_schema("colors")
    assert any(c["name"] == "id" for c in schema)


def test_on_conflict_skip_does_nothing(tmp_db, colors_csv):
    loader = CSVLoader(tmp_db)
    loader.load(colors_csv, "colors")

    result = loader.load(colors_csv, "colors", on_conflict="skip")

    assert result["action"] == "skipped"
    assert result["rows_inserted"] == 0


def test_on_conflict_replace_reloads_rows(tmp_db, colors_csv):
    loader = CSVLoader(tmp_db)
    loader.load(colors_csv, "colors")

    result = loader.load(colors_csv, "colors", on_conflict="replace")
    assert result["action"] == "replaced"

    with getconnection(tmp_db) as conn:
        count = conn.execute("SELECT COUNT(*) FROM colors").fetchone()[0]

    assert count == 3


def test_on_conflict_append_adds_more_rows(tmp_db, colors_csv):
    loader = CSVLoader(tmp_db)
    loader.load(colors_csv, "colors")
    loader.load(colors_csv, "colors", on_conflict="append")

    with getconnection(tmp_db) as conn:
        count = conn.execute("SELECT COUNT(*) FROM colors").fetchone()[0]

    assert count == 6


def test_schema_mismatch_raises_value_error(tmp_db, colors_csv, cyber_csv):
    loader = CSVLoader(tmp_db)
    loader.load(colors_csv, "colors")

    with pytest.raises(ValueError, match="mismatch"):
        loader.load(cyber_csv, "colors", on_conflict="append")


def test_invalid_on_conflict_raises_error(tmp_db, colors_csv):
    loader = CSVLoader(tmp_db)
    loader.load(colors_csv, "colors")

    with pytest.raises(ValueError):
        loader.load(colors_csv, "colors", on_conflict="invalid")


def test_csv_loader_does_not_use_to_sql_():
    import inspect
    from database import csv_loader 

    source_code = inspect.getsource(csv_loader)
    assert "to_sql" not in source_code