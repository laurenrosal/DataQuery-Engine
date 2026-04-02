import pytest
from unittest.mock import MagicMock, patch
from app.llm_adapter import LLMAdapter, LLMError

SCHEMA = {
    "threats": [
        {"name": "id", "type": "INTEGER"},
        {"name": "country", "type": "TEXT"},
        {"name": "year", "type": "INTEGER"},
        {"name": "attack_type", "type": "TEXT"},
        {"name": "attack_source", "type": "TEXT"},
    ]
}


@pytest.fixture
def adapter():
    with patch("app.llm_adapter.anthropic.Anthropic") as mock_anthropic:
        mock_client = mock_anthropic.return_value
        llm = LLMAdapter()
        llm.client = mock_client
        yield llm, mock_client


def make_mock_response(text):
    mock_piece = MagicMock()
    mock_piece.text = text

    mock_response = MagicMock()
    mock_response.content = [mock_piece]

    return mock_response


def test_prompt_includes_user_question(adapter):
    llm, _ = adapter
    prompt = llm._build_prompt("show me all attacks", SCHEMA)

    assert "show me all attacks" in prompt


def test_prompt_includes_table_name(adapter):
    llm, _ = adapter
    prompt = llm._build_prompt("any question", SCHEMA)

    assert "threats" in prompt


def test_prompt_skips_sqlite_sequence(adapter):
    llm, _ = adapter

    schema_with_internal = dict(SCHEMA)
    schema_with_internal["sqlite_sequence"] = [
        {"name": "name", "type": "TEXT"}
    ]

    prompt = llm._build_prompt("any question", schema_with_internal)
    assert "sqlite_sequence" not in prompt


def test_extract_sql_from_code_block(adapter):
    llm, _ = adapter
    response = "```sql\nSELECT * FROM threats\n```"

    assert llm._extract_sql(response) == "SELECT * FROM threats"


def test_extract_sql_from_plain_text(adapter):
    llm, _ = adapter
    response = "SELECT * FROM threats WHERE country = 'China'"

    assert llm._extract_sql(response) == response


def test_extract_sql_returns_empty_string_when_missing(adapter):
    llm, _ = adapter

    assert llm._extract_sql("I cannot answer that.") == ""


def test_generate_sql_returns_select_query(adapter):
    llm, mock_client = adapter
    mock_client.messages.create.return_value = make_mock_response(
        "SELECT * FROM threats WHERE country = 'China'"
    )

    result = llm.generate_sql("Show attacks from China", SCHEMA)

    assert result.upper().startswith("SELECT")


def test_generate_sql_calls_api_once(adapter):
    llm, mock_client = adapter
    mock_client.messages.create.return_value = make_mock_response(
        "SELECT * FROM threats"
    )

    llm.generate_sql("any question", SCHEMA)

    mock_client.messages.create.assert_called_once()


def test_generate_sql_raises_error_when_no_sql_found(adapter):
    llm, mock_client = adapter
    mock_client.messages.create.return_value = make_mock_response(
        "I don't understand that question."
    )

    with pytest.raises(LLMError, match="Could not extract SQL"):
        llm.generate_sql("gibberish", SCHEMA)


def test_llm_adapter_does_not_execute_queries(adapter):
    llm, _ = adapter

    assert not hasattr(llm, "execute")
    assert not hasattr(llm, "execute_query")


def test_bad_llm_table_name_gets_caught_by_validator(adapter):
    from app.sql_validator import SQLValidator, ValidationError

    llm, mock_client = adapter
    mock_client.messages.create.return_value = make_mock_response(
        "SELECT * FROM incidents WHERE year = 2020"
    )

    sql = llm.generate_sql("show incidents", SCHEMA)

    with pytest.raises(ValidationError, match="Unknown table"):
        SQLValidator().validate(sql, SCHEMA)


def test_bad_llm_column_name_gets_caught_by_validator(adapter):
    from app.sql_validator import SQLValidator, ValidationError

    llm, mock_client = adapter
    mock_client.messages.create.return_value = make_mock_response(
        "SELECT threats.severity FROM threats"
    )

    sql = llm.generate_sql("show severity", SCHEMA)

    with pytest.raises(ValidationError, match="Unknown column"):
        SQLValidator().validate(sql, SCHEMA)