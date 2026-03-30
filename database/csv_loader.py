import pandas as pas
import sqlite3
from database.database import getconnection
from database.schema_manager import SchemaManager

#These are 4 columns get normalized into lookup tables.
# country and target_industry sty as plain TEXT in incidents.

LOOKUP_Tables = {
    "attack_type":                    ("attack_types", "name"),
    "attack_source":                  ("attack_sources", "name"),
    "security_vulnerability_type":    ("vulnerability_types", "name"),
    "defense_mechanism_used":         ("defense_mechanisms", "name"),
}

# SQL to create the lookup tables (run once on first load)
LOOKUP_DDL = """
CREATE TABLE IF NOT EXISTS attack_types (
    id   INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE
);
CREATE TABLE IF NOT EXISTS attack_sources (
    id   INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE
);
CREATE TABLE IF NOT EXISTS vulnerability_types (
    id   INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE
);
CREATE TABLE IF NOT EXISTS defense_mechanisms (
    id   INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE
);
"""
 
# Schema for the main incidents table
INCIDENTS_DDL = """
CREATE TABLE IF NOT EXISTS incidents (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    year                 INTEGER NOT NULL,
    country              TEXT,
    target_industry      TEXT,
    financial_loss_usd_m REAL,
    num_affected_users   INTEGER,
    resolution_hours     INTEGER,
    attack_type_id       INTEGER REFERENCES attack_types(id),
    source_id            INTEGER REFERENCES attack_sources(id),
    vulnerability_id     INTEGER REFERENCES vulnerability_types(id),
    defense_id           INTEGER REFERENCES defense_mechanisms(id)
);
"""
 

class CSVLoader:
    def __init__(self, db_path: str = None):
        self.db_path = db_path
        self.schema_manager = SchemaManager(db_path)

    def load(self, csv_path: str, table_name: str,
             on_conflict: str = "append") -> dict:
        
        """
        Load the cybersecurity CSV into the normalized incidents schema.
 
        Lookup columns (attack_type, attack_source, vulnerability_type,
        defense_mechanism) are resolved to FK IDs automatically.
        country and target_industry stay as plain TEXT.
 
        Args:
            csv_path:    Path to the CSV file.
            table_name:  Target table (default: 'incidents').
            on_conflict: 'append', 'replace', or 'skip'.
 
        Returns:
            dict with keys: table, rows_inserted, action

        """
        df = pas.read_csv(csv_path)
        df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

        with getconnection(self.db_path) as conn:
            #Always ensure lookup tables and incidents table exist
            for statement in LOOKUP_DDL.strip().split(";"):
                s = statement.strip()
                if s:
                    conn.execute(s)
            conn.execute(INCIDENTS_DDL)

            if self.schema_manager.table_exists(table_name):
                if on_conflict == "skip":
                    return {"table": table_name, "rows_inserted": 0,
                            "action": "skipped"}
                elif on_conflict == "replace":
                    conn.execute(f'DELETE TABLE "{table_name}"')
                    action = "replaced"
                elif on_conflict == "append":
                    if not self.schema_manager.schemas_match(table_name, df):
                        raise ValueError(
                            f"Schema mismatch: CSV columns don't match "
                            f"existing table '{table_name}'."
                        )
                    action = "appended"
                else:
                    raise ValueError(f"Unknown on_conflict value: {on_conflict}")
            else:
                self.schema_manager.create_table(table_name, df, conn)
                action = "created"

            rows_inserted = self._insert_rows(conn, df)

        return {"table": table_name, "rows_inserted": rows_inserted,
                "action": action}
    
    def _get_or_create_id(self, conn: sqlite3.Connection,
                          table: str, value: str) -> int:
        """
        Look up a value in a lookup table, inserting it if it doesn't exist.
        Returns the row's integer ID.
        """
        row = conn.execute(
            f'SELECT id FROM "{table}" WHERE name = ?', (value,)
        ).fetchone()
        if row:
            return row["id"]
        cur = conn.execute(
            f'INSERT INTO "{table}" (name) VALUES (?)', (value,)
        )
        return cur.lastrowid
    
    def _insert_rows(self, conn: sqlite3.Connection,
                     table: str, df: pas.DataFrame) -> int:
        """
        Insert rows into incidents, resolving lookup columns to FK IDs.
        No df.to_sql() used — every row is inserted manually.
        """
        sql = """
            INSERT INTO incidents (
                year, country, target_industry,
                financial_loss_usd_m, num_affected_users, resolution_hours,
                attack_type_id, source_id, vulnerability_id, defense_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
 
        # Map CSV column names → lookup table names
        col_map = {
            "attack_type":                      "attack_types",
            "attack_source":                    "attack_sources",
            "security_vulnerability_type":      "vulnerability_types",
            "defense_mechanism_used":           "defense_mechanisms",
        }
 
        count = 0
        for row in df.itertuples(index=False, name=None):
            row_dict = dict(zip(df.columns, row))
 
            # Resolve the 4 FK columns
            attack_type_id   = self._get_or_create_id(
                conn, "attack_types",       row_dict["attack_type"])
            source_id        = self._get_or_create_id(
                conn, "attack_sources",     row_dict["attack_source"])
            vulnerability_id = self._get_or_create_id(
                conn, "vulnerability_types", row_dict["security_vulnerability_type"])
            defense_id       = self._get_or_create_id(
                conn, "defense_mechanisms", row_dict["defense_mechanism_used"])
 
            conn.execute(sql, (
                row_dict["year"],
                row_dict["country"],
                row_dict["target_industry"],
                row_dict["financial_loss_(in_million_$)"],
                row_dict["number_of_affected_users"],
                row_dict["incident_resolution_time_(in_hours)"],
                attack_type_id,
                source_id,
                vulnerability_id,
                defense_id,
            ))
            count += 1
 
        return count