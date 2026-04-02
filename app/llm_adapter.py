import anthropic
import re
from config import ANTHROPIC_API_KEY, MAX_TOKENS

class LLMError(Exception):
    #rasied if something goes wrong when calling the LLM
    pass

class LLMAdapter:
    #using Claude Sonnet for now
    MODEL = "claude-sonnet-4-20250514"

    def __init__(self):
        #set up the client using the API key from config
        self.client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    
    def generate_sql(self, user_question:str, schema: dict) -> str:
        #build the prompt using the current schema
        prompt = self._build_prompt(user_question, schema)

        try:
            #send request to Claude
            message = self.client.messages.create(
                model=self.MODEL,
                max_tokens=MAX_TOKENS,
                messages=[
                    {"role": "user", "content": prompt}
                ]
            )

            #grab the text response
            response_text = message.content[0].text
        
        except anthropic.APIError as e:
            raise LLMError(f"Claude API call failded: {e}")
        
        #try to extract just the SQL from the response
        sql = self._extract_sql(response_text)

        if not sql:
            raise LLMError(
                f"Could not extract SQL from Claude response. "
                f"Response was: {response_text[:200]}"
            )
        return sql
    
    def _build_prompt(self, user_question: str, schema: dict) -> str:
        #turn schema into readable text
        schema_description = self._format_schema(schema)

        prompt = f"""You are a SQL expert. Convert the user's question into a complete, valid SQLite SELECT query.

Database schema:
{schema_description}

Rules:
- Only generate SELECT queries — never INSERT, UPDATE, DELETE, or DROP
- Use only the tables and columns listed in the schema above
- Always wrap column names in double quotes e.g. "column_name"
- EVERY query MUST include FROM table_name — never omit it
- For aggregations always include GROUP BY and ORDER BY
- String values in WHERE clauses are case-sensitive — preserve exact casing
- Use LIKE for case-insensitive matching when the user's intent is broad
- Return ONLY the raw SQL query — no explanation, no markdown, no code blocks

Correct example:
SELECT "country", SUM("financial_loss_(in_million_$)") as total
FROM threats
GROUP BY "country"
ORDER BY total DESC
LIMIT 5

User question: {user_question}

Complete SQL query (must include FROM):"""

        return prompt
    
    def _format_schema(self, schema: dict) -> str:
        lines = []

        for table_name, columns in schema.items():
            # Skip SQLite internal tables
            if table_name == "sqlite_sequence":
                continue

            lines.append(f"Table: {table_name}")

            for col in columns:
                lines.append(f"  - {col['name']} ({col['type']})")

            lines.append("")  # space between tables

        return "\n".join(lines)
    
    def _extract_sql(self, response_text: str) -> str:
        # case 1: sql inside ``` blocks
        code_block = re.search(
        r'```(?:sql)?\s*(.*?)\s*```',
        response_text,
        re.DOTALL | re.IGNORECASE
    )
        if code_block:
            return code_block.group(1).strip().rstrip(";").strip()
        
        # Case 2 — find first SELECT and take everything after it
        lines = response_text.strip().splitlines()

        for i, line in enumerate(lines):
            if line.strip().upper().startswith("SELECT"):
                sql = "\n".join(lines[i:]).strip().rstrip(";").strip()
                return sql

        # Case 3 — whole response is already just SQL
        cleaned = response_text.strip().rstrip(";").strip()
        
        if cleaned.upper().startswith("SELECT"):
            return cleaned

        return ""