import os
import sqlite3
from database.database import getconnection


def test_returns_sqlite_connection(tmp_path):
    db_path = str(tmp_path / "test.db")

    conn = getconnection(db_path)
    assert isinstance(conn, sqlite3.Connection)

    conn.close()


def test_row_factory_allows_column_access_by_name(tmp_path):
    db_path = str(tmp_path / "test.db")

    with getconnection(db_path) as conn:
        conn.execute("CREATE TABLE t (id INTEGER, name TEXT)")
        conn.execute("INSERT INTO t VALUES (1, 'hello')")

        row = conn.execute("SELECT * FROM t").fetchone()

    assert row["name"] == "hello"


def test_creates_database_file_if_missing(tmp_path):
    db_path = str(tmp_path / "new.db")

    assert not os.path.exists(db_path)

    getconnection(db_path).close()

    assert os.path.exists(db_path)