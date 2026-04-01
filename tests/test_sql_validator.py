import pytest
from app.sql_validator import SQLValidator, ValidationError
#run the tests: python3 -m pytest tests/ -v
# ── Sample schema (mirrors your threats table) ──
SCHEMA = {
    "threats": [
        {"name": "id",                                  "type": "INTEGER"},
        {"name": "country",                             "type": "TEXT"},
        {"name": "year",                                "type": "INTEGER"},
        {"name": "attack_type",                         "type": "TEXT"},
        {"name": "target_industry",                     "type": "TEXT"},
        {"name": "financial_loss_(in_million_$)",       "type": "REAL"},
        {"name": "number_of_affected_users",            "type": "INTEGER"},
        {"name": "attack_source",                       "type": "TEXT"},
        {"name": "security_vulnerability_type",         "type": "TEXT"},
        {"name": "defense_mechanism_used",              "type": "TEXT"},
        {"name": "incident_resolution_time_(in_hours)", "type": "INTEGER"},
    ]
}

@pytest.fixture
def validator():
    return SQLValidator()


# ── Valid queries ──

def test_valid_select(validator):
    q = "SELECT * FROM threats"
    assert validator.validate(q, SCHEMA) == q.strip()

def test_valid_select_with_where(validator):
    q = "SELECT country, year FROM threats WHERE attack_type = 'Phishing'"
    assert validator.validate(q, SCHEMA) == q.strip()

def test_valid_select_with_limit(validator):
    q = "SELECT * FROM threats LIMIT 10"
    assert validator.validate(q, SCHEMA) == q.strip()


# ── Check 1: SELECT only ──

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


# ── Check 2: Injection patterns ──

def test_rejects_semicolon(validator):
    with pytest.raises(ValidationError):
        validator.validate("SELECT * FROM threats; DROP TABLE threats", SCHEMA)

def test_rejects_line_comment(validator):
    with pytest.raises(ValidationError):
        validator.validate("SELECT * FROM threats -- comment", SCHEMA)

def test_rejects_block_comment(validator):
    with pytest.raises(ValidationError):
        validator.validate("SELECT * FROM threats /* hack */", SCHEMA)


# ── Check 3: Table names ──

def test_rejects_unknown_table(validator):
    with pytest.raises(ValidationError, match="Unknown table"):
        validator.validate("SELECT * FROM fake_table", SCHEMA)

def test_accepts_known_table(validator):
    validator.validate("SELECT * FROM threats", SCHEMA)


# ── Check 4: Column names ──

def test_rejects_unknown_column(validator):
    with pytest.raises(ValidationError, match="Unknown column"):
        validator.validate("SELECT threats.fake_col FROM threats", SCHEMA)

def test_accepts_known_column(validator):
    validator.validate("SELECT threats.country FROM threats", SCHEMA)


# ── LLM bad output demo (required by assignment) ──

def test_llm_generated_wrong_table(validator):
    """
    Demonstrates a case where LLM output was incorrect
    and the validator caught it.
    LLM hallucinated a table called 'incidents' that doesn't exist.
    """
    bad_llm_output = "SELECT * FROM incidents WHERE year = 2020"
    with pytest.raises(ValidationError, match="Unknown table"):
        validator.validate(bad_llm_output, SCHEMA)

def test_llm_generated_wrong_column(validator):
    """
    LLM hallucinated a column called 'severity' that doesn't exist.
    """
    bad_llm_output = "SELECT threats.severity FROM threats"
    with pytest.raises(ValidationError, match="Unknown column"):
        validator.validate(bad_llm_output, SCHEMA)