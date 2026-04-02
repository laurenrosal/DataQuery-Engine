import pytest
import pandas as pd
from database.database import getconnection
from database.schema_manager import SchemaManager
from database.csv_loader import CSVLoader

REAL_CSV = "data/Global_Cybersecurity_Threats_2015-2024.csv"


@pytest.fixture
def tmp_db(tmp_path):
    return str(tmp_path / "test.db")


@pytest.fixture
def loaded_db(tmp_path):
    db_path = str(tmp_path / "test.db")
    CSVLoader(db_path).load(REAL_CSV, "threats")
    return db_path


def test_new_database_starts_empty(tmp_db):
    sm = SchemaManager(tmp_db)
    assert sm.get_tables() == []


def test_table_shows_up_after_loading_csv(loaded_db):
    sm = SchemaManager(loaded_db)
    tables = sm.get_tables()

    assert "threats" in tables


def test_table_exists_returns_true_for_real_table(loaded_db):
    sm = SchemaManager(loaded_db)
    assert sm.table_exists("threats") is True


def test_table_exists_returns_false_for_missing_table(loaded_db):
    sm = SchemaManager(loaded_db)
    assert sm.table_exists("nonexistent") is False


def test_get_table_schema_returns_expected_columns(loaded_db):
    sm = SchemaManager(loaded_db)
    columns = [col["name"] for col in sm.get_table_schema("threats")]

    for expected in ["id", "year", "country", "attack_type"]:
        assert expected in columns


def test_get_table_schema_returns_expected_types(loaded_db):
    sm = SchemaManager(loaded_db)
    column_types = {
        col["name"]: col["type"]
        for col in sm.get_table_schema("threats")
    }

    assert column_types["year"] == "INTEGER"
    assert column_types["country"] == "TEXT"


def test_get_all_schemas_includes_threats_table(loaded_db):
    sm = SchemaManager(loaded_db)
    all_schemas = sm.get_all_schemas()

    assert "threats" in all_schemas
    assert len(all_schemas["threats"]) > 0


def test_infere_sql_type_returns_expected_values(tmp_db):
    sm = SchemaManager(tmp_db)

    assert sm.infere_sql_type("int64") == "INTEGER"
    assert sm.infere_sql_type("float64") == "REAL"
    assert sm.infere_sql_type("object") == "TEXT"
    assert sm.infere_sql_type("unknown") == "TEXT"


def test_create_table_adds_id_column(tmp_db):
    sm = SchemaManager(tmp_db)
    df = pd.DataFrame({
        "name": ["a"],
        "value": [1]
    })

    with getconnection(tmp_db) as conn:
        sm.create_table("tbl", df, conn)

    schema = sm.get_table_schema("tbl")
    assert any(col["name"] == "id" for col in schema)


def test_schemas_match_when_dataframe_is_same(tmp_db):
    sm = SchemaManager(tmp_db)
    df = pd.DataFrame({
        "country": ["USA"],
        "year": [2020]
    })

    with getconnection(tmp_db) as conn:
        sm.create_table("tbl", df, conn)

    assert sm.schemas_match("tbl", df) is True


def test_schemas_do_not_match_when_columns_are_different(tmp_db):
    sm = SchemaManager(tmp_db)

    df1 = pd.DataFrame({
        "country": ["USA"],
        "year": [2020]
    })

    df2 = pd.DataFrame({
        "different_col": ["x"]
    })

    with getconnection(tmp_db) as conn:
        sm.create_table("tbl", df1, conn)

    assert sm.schemas_match("tbl", df2) is False