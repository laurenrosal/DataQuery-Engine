import sys
import os

# Add project root to path so imports work when running from any directory
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.query_service import QueryService
from app.llm_adapter import LLMAdapter, LLMError
from app.sql_validator import ValidationError


def print_menu():
    """Print the main menu options."""
    print()
    print("=" * 40)
    print("       DataQuery-engine — Main Menu")
    print("=" * 40)
    print("  1. Load a CSV file")
    print("  2. Ask a question (natural language)")
    print("  3. List tables and columns")
    print("  4. Exit")
    print("=" * 40)


def handle_load_csv(qs: QueryService):
    """
    Prompt the user for a CSV path and table name,
    then load it into the database via QueryService.
    """
    print()
    csv_path = input("Enter path to CSV file: ").strip()

    # Check the file actually exists before trying to load it
    if not os.path.exists(csv_path):
        print(f"  Error: File not found — '{csv_path}'")
        return

    table_name = input("Enter table name to load into: ").strip()
    if not table_name:
        print("  Error: Table name cannot be empty.")
        return

    # Ask how to handle conflicts if the table already exists
    print("  If table already exists:")
    print("    a. append   — add rows if schema matches")
    print("    b. replace  — clear and reload")
    print("    c. skip     — do nothing")
    choice = input("  Choose (a/b/c) [default: a]: ").strip().lower()

    conflict_map = {"a": "append", "b": "replace", "c": "skip", "": "append"}
    on_conflict = conflict_map.get(choice, "append")

    try:
        result = qs.load_csv(csv_path, table_name, on_conflict)
        print()
        print(f"  Done! {result['rows_inserted']} rows {result['action']}"
              f" into '{result['table']}'.")
    except ValueError as e:
        print(f"  Error: {e}")
    except Exception as e:
        print(f"  Unexpected error: {e}")


def handle_ask_question(qs: QueryService, adapter: LLMAdapter):
    """
    Take a natural language question from the user,
    generate SQL via the LLM adapter, validate it,
    execute it, and display the results.

    The CLI never touches the database directly —
    everything goes through QueryService.
    """
    print()

    # Make sure there are tables to query before asking
    tables = qs.get_tables()
    if not tables:
        print("  No tables loaded yet. Please load a CSV first (option 1).")
        return

    question = input("Ask your question: ").strip()
    if not question:
        print("  Error: Question cannot be empty.")
        return

    print("  Generating SQL...")

    try:
        # Step 1 — get schema for the LLM prompt
        schema = qs.get_schema()

        # Step 2 — translate natural language to SQL
        sql = adapter.generate_sql(question, schema)
        print(f"  Generated SQL: {sql}")
        print()

        # Step 3 — validate and execute via QueryService
        rows = qs.execute_query(sql)

        # Step 4 — display results
        if not rows:
            print("  No results found.")
            return

        print(f"  {len(rows)} row(s) returned:")
        print()


        # Print each row — limit to 20 rows to keep output readable
        for row in rows[:20]:
            print("  " + " | ".join(f"{str(v):<20}" for v in row))

        if len(rows) > 20:
            print(f"  ... and {len(rows) - 20} more rows.")

    except LLMError as e:
        print(f"  LLM error: {e}")
    except ValidationError as e:
        print(f"  Validation error — unsafe query blocked: {e}")
    except Exception as e:
        print(f"  Error: {e}")


def handle_list_tables(qs: QueryService):
    """
    Display all tables in the database and their columns.
    Uses SchemaManager via QueryService — CLI never queries DB directly.
    """
    print()
    schema = qs.get_schema()

    # Filter out SQLite internal tables
    user_tables = {k: v for k, v in schema.items()
                   if k != "sqlite_sequence"}

    if not user_tables:
        print("  No tables loaded yet. Please load a CSV first (option 1).")
        return

    for table_name, columns in user_tables.items():
        print(f"  Table: {table_name}")
        for col in columns:
            print(f"    - {col['name']} ({col['type']})")
        print()


def _get_main_table(sql: str, schema: dict) -> str:
    """
    Extract the first table name from a SQL query that exists in the schema.
    Used to get column headers for display.
    Returns empty string if no match found.
    """
    import re
    matches = re.findall(r'\bFROM\s+(\w+)', sql, re.IGNORECASE)
    for match in matches:
        if match.lower() in schema:
            return match.lower()
    return ""


def main():
    """
    Main entry point for the SecureQuery CLI.

    Creates a QueryService and LLMAdapter, then runs an
    input loop until the user chooses to exit.

    The CLI is always the last module to be built — it depends
    on all other modules being complete and working.
    """
    print()
    print("  Welcome to DataQuery-Engine")
    print("  Natural language interface for data")

    # Initialize the core services
    qs = QueryService()
    adapter = LLMAdapter()

    while True:
        print_menu()
        choice = input("Enter choice (1-4): ").strip()

        if choice == "1":
            handle_load_csv(qs)

        elif choice == "2":
            handle_ask_question(qs, adapter)

        elif choice == "3":
            handle_list_tables(qs)

        elif choice == "4":
            print()
            print("  Goodbye!")
            print()
            break

        else:
            print()
            print("  Invalid choice — please enter 1, 2, 3, or 4.")


if __name__ == "__main__":
    main()