import sqlite3
import pandas as pas
from database.database import getconnection

PANDAS_TO_SQL = {
    "int64": "INTEGER",
    "float64": "REAL",
    "bool": "INTEGER",
    "datetime64[ns]": "TEXT",
    "object": "TEXT",
}

class SchemaManager:
    def __init__(self, db_path: str = None):
        self.db_path = db_path

    def _conn(self) -> list[str]:
        return getconnection(self.db_path)
    
    def get_tables(self) -> list[str]:
        """Return all table names in the database."""
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        return [r["name"] for r in rows]
    
    def get_table_schema(self, table: str) -> list[dict]:
        """Return column info for a table: name, type."""
        with self._conn() as conn:
            rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
        return [{"name": r["name"], "type": r["type"]} for r in rows]
    
    def get_all_schemas(self) -> dict:
        """Return schema for every table - used by Query Service and LLM Adapter."""
        return {t: self.get_table_schema(t) for t in self.get_tables()}
    
    def infere_sql_type(self, dtype) -> str:
        """Map a pandas dtype to a SQLite type."""
        return PANDAS_TO_SQL.get(str(dtype), "TEXT")
    
    def table_exists(self, table: str) -> bool:
        return table in self.get_tables()
    
    def schemas_match(self, table: str, df: pas.DataFrame) -> bool:
        existing = {col["name"].lower(): col["type"]
                    for col in self.get_table_schema(table)
                    if col["name"] != "id"}
        incoming = {col.lower(): self.infere_sql_type(dtype)
                    for col, dtype in df.dtypes.items()}
        return existing == incoming
    
    def create_table(self, table: str, df: pas.DataFrame, conn: sqlite3.Connection):
        """Generate and execute CREATE TABLE from a DataFrame."""
        col_defs = ["id INTEGER PRIMARY KEY AUTOINCREMENT"]
        for col, dtype in df.dtypes.items():
            sql_type = self.infere_sql_type(dtype)
            col_defs.append(f'"{col}" {sql_type}')
        ddl = f'CREATE TABLE "{table}" ({", ".join(col_defs)})'
        conn.execute(ddl)