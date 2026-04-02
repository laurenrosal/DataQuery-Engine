import pytest
from app.sql_validator import SQLValidator, ValidationError

# run tests with: python3 -m pytest tests/ -v

SCHEMA = {
    "threats": [
        {"name": "id", "type": "INTEGER"},
        {"name": "country", "type": "TEXT"},
        {"name": "year", "type": "INTEGER"},
        {"name": "attack_type", "type": "TEXT"},
        {"name": "target_industry", "type": "TEXT"},
        {"name": "financial_loss_(in_million_$)", "type": "REAL"},
        {"name": "number_of_affected_users", "type": "INTEGER"},
        {"name": "attack_source", "type": "TEXT"},
        {"name": "security_vulnerability_type", "type": "TEXT"},
        {"name": "defense_mechanism_used", "type": "TEXT"},
        {"name": "incident_resolution_time_(in_hours)", "type": "INTEGER"},
    ]
}


@pytest.fixture
def validator():
    return SQLValidator()


def test_accepts_basic_select(validator):
    query = "SELECT * FROM threats"
    assert validator.validate(query, SCHEMA) == query.strip()


def test_accepts_select_with_where(validator):
    query = "SELECT country, year FROM threats WHERE attack_type = 'Phishing'"
    assert validator.validate(query, SCHEMA) == query.strip()


def test_accepts_select_with_limit(validator):
    query = "SELECT * FROM threats LIMIT 10"
    assert validator.validate(query, SCHEMA) == query.strip()


def test_rejects_insert(validator):
    with pytest.raises(ValidationError, match="INSERT"):
        validator.validate("INSERT INTO threats VALUES (1)", SCHEMA)


def test_rejects_update(validator):
    with pytest.raises(ValidationError, match="UPDATE"):
        validator.validate("UPDATE threats SET country='USA'", SCHEMA)


def test_rejects_delete(validator):
    with pytest.raises(ValidationError, match="DELETE"):
        validator.validate("DELETE FROM threats", SCHEMA)


def test_rejects_drop(validator):
    with pytest.raises(ValidationError, match="DROP"):
        validator.validate("DROP TABLE threats", SCHEMA)


def test_rejects_empty_query(validator):
    with pytest.raises(ValidationError):
        validator.validate("", SCHEMA)


def test_rejects_semicolon_injection(validator):
    with pytest.raises(ValidationError):
        validator.validate("SELECT * FROM threats; DROP TABLE threats", SCHEMA)


def test_rejects_line_comment(validator):
    with pytest.raises(ValidationError):
        validator.validate("SELECT * FROM threats -- comment", SCHEMA)


def test_rejects_block_comment(validator):
    with pytest.raises(ValidationError):
        validator.validate("SELECT * FROM threats /* hack */", SCHEMA)


def test_rejects_unknown_table(validator):
    with pytest.raises(ValidationError, match="Unknown table"):
        validator.validate("SELECT * FROM fake_table", SCHEMA)


def test_accepts_known_table(validator):
    validator.validate("SELECT * FROM threats", SCHEMA)


def test_rejects_unknown_column(validator):
    with pytest.raises(ValidationError, match="Unknown column"):
        validator.validate("SELECT threats.fake_col FROM threats", SCHEMA)


def test_accepts_known_column(validator):
    validator.validate("SELECT threats.country FROM threats", SCHEMA)


def test_validator_catches_wrong_table_from_llm(validator):
    bad_query = "SELECT * FROM incidents WHERE year = 2020"

    with pytest.raises(ValidationError, match="Unknown table"):
        validator.validate(bad_query, SCHEMA)


def test_validator_catches_wrong_column_from_llm(validator):
    bad_query = "SELECT threats.severity FROM threats"

    with pytest.raises(ValidationError, match="Unknown column"):
        validator.validate(bad_query, SCHEMA)