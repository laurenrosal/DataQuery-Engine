import anthropic
import re
from config import ANTHROPIC_API_KEY, MAX_TOKENS

class LLMError(Exception):
    #Raised when the LLm adapter fails to generate a valid SQL query
    pass

#translates natural language questions into SQL queries
class LLMAdapter:

    #The model to use - claude-sonnet
    MODEL = "claude-sonnet-4-20250514"

    def __init__(self):
        #initialize the anthropic client with the API key from config
        self.client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    
    #translate a natural language question into SQL query
    def generate_sql(self, user_question:str, schema: dict) -> str:

        #build the promt with the current schema baked in 
        prompt = self._build_prompt(user_question, schema)

        try:
            #call the claude API
            message = self.client.messages.create(
                model=self.MODEL,
                max_tokens=MAX_TOKENS,
                messages=[
                    {"role": "user", "content": prompt}
                ]
            )

            #extract the text from the reponse 
            response_text = message.content[0].text
        
        except anthropic.APIError as e:
            raise LLMError(f"Claude API call failded: {e}")
        
        #pull just the SQL out of the reponse 
        sql = self._extract_sql(response_text)

        if not sql:
            raise LLMError(
                f"Could not extract SQL from Claude response. "
                f"Response was: {response_text[:200]}"
            )
        return sql
    
    #build the prompt sent to claude
    def _build_prompt(self, user_question: str, schema: dict) -> str:

        #format the schema into readable table description 
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
    
    # convert the schema dict into a readable string for the prompt.
    def _format_schema(self, schema: dict) -> str:
        lines = []
        for table_name, columns in schema.items():
            # Skip SQLite internal tables
            if table_name == "sqlite_sequence":
                continue
            lines.append(f"Table: {table_name}")
            for col in columns:
                lines.append(f"  - {col['name']} ({col['type']})")
            lines.append("")  # blank line between tables
        return "\n".join(lines)
    
    # extract a SQL query from claude repose text
    def _extract_sql(self, response_text: str) -> str:
        code_block = re.search(
        r'```(?:sql)?\s*(.*?)\s*```',
        response_text,
        re.DOTALL | re.IGNORECASE
    )
        if code_block:
            return code_block.group(1).strip().rstrip(";").strip()
        
        # Case 2 — find SELECT and grab everything from there to the end
        lines = response_text.strip().splitlines()
        for i, line in enumerate(lines):
            if line.strip().upper().startswith("SELECT"):
                # Join from this line to the end — captures multi-line SQL
                sql = "\n".join(lines[i:]).strip().rstrip(";").strip()
            return sql

        # Case 3 — whole response starts with SELECT
        cleaned = response_text.strip().rstrip(";").strip()
        if cleaned.upper().startswith("SELECT"):
            return cleaned

        return ""