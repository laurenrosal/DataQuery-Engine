import re

class ValidationError(Exception):
    ## Rasised when a SQL query fails validation 

    ## separete from Python's built-in exections so callers can catch
    # validation failures specifically without catxhing unrelated errors

    pass

class SQLValidator:
    #Validates SQL quereies before they reatch the database

    #SQL keywords that are not allowed - anything that wirtes or destory data 
    FORBIDDEN_KEYWORDS = [
        "INSERT", "UPDATE", "DELETE", "DROP", "CREATE",
        "ALTER", "TRUNCATE", "REPLACE", "MERGE"
    ]

    #Patterns that indicate SQL injection attempts
    INJECTION_PATTERNS = [
        r";",   # stacked queries  — SELECT * FROM t; DROP TABLE t
        r"--",  # line comment     — SELECT * FROM t -- ignore rest
        r"/\*", # block comment    — SELECT /* hack */ * FROM t
        r"\bOR\b\s+\d", # OR 1=1 style     — WHERE id=1 OR 1=1
        r"\bOR\b\s+'", # OR 'a'='a' style
    ]

    #Run all validation check on a SQL query
    def validate(self, query: str, schema: dict) -> str:

        #clean up whitespace for consistent checking
        clean = query.strip()

        self._check_not_empty(clean)
        self._check_select_only(clean)
        self._check_no_injection(clean)
        self._check_tables_exist(clean, schema)
        self._check_columns_exist(clean, schema)

        return clean
    
    def _check_not_empty(self, query:str):
        #reject empty or whitespace-only queries
        if not query:
            raise ValidationError("Query is empty.")
    
    #reject any query that isn't a SELECT statement.
    def _check_select_only(self, query:str):
        #check the first word
        first_word = query.split()[0].upper()
        if first_word != "SELECT":
            raise ValidationError(
                f"Only SELECT queries are allowed. "
                f"Got: '{first_word}'."
            )
        
        #also scan the full query for forbidden keywords
        query_upper = query.upper()
        for keyword in self.FORBIDDEN_KEYWORDS:
            #Use word boundary so 'DELETIONS' doesn't match 'DELETE'
            if re.search(rf"\b{keyword}\b", query_upper):
                raise ValidationError(
                    f"Forbidden keyword '{keyword}' found in query."
                )
    
    #block common SQL injection patterns 
    def _check_no_injection(self, query:str):
        for pattern in self.INJECTION_PATTERNS:
            if re.search(pattern, query, re.IGNORECASE):
                raise ValidationError(
                    f"Potnentially unsafe pattern detected in query."
                    f"Matched pattern: '{pattern}'."
                )
    
    #Verify every table referenced in the query exisits in the schema
    def _check_tables_exist(self, query:str, schema:dict):
        
        #known internal SQLite tables to ignore
        internal_tables = {"sqlite_sequence", "sqlite_master"}

        referenced = self._extract_tables(query)

        for table in referenced:
            if table in internal_tables:
                continue
            if table not in schema:
                raise ValidationError(
                    f"Unknown table '{table}'."
                    f"Avilable tables: {list(schema.keys())}."
                )

    #verify every column referenced in the query exists in its table.    
    def _check_columns_exist(self, query:str, schema: dict):

        #match patterns like 'table.column' or 'alias.column'
        dot_refs = re.findall(r'\b(\w+)\.(\w+)\b', query)

        for table, column in dot_refs:
            table_lower = table.lower()
            if table_lower not in schema:
                continue #already caught by _check_tables_exist
            col_names = [c["name"].lower() for c in schema[table_lower]]
            if column.lower() not in col_names:
                raise ValidationError(
                    f"Unknown column '{column}' in table '{table}'. "
                    f"Available columns: {col_names}."
                )
    
    #Extract table names form FROM and Join clauses 
    def _extract_tables(self, query: str):
        tables = set()

        #match FROM or JOIN followed by a table name 
        pattern = r'\b(?:FROM|JOIN)\s+(\w+)(?:\s+(?:AS\s+)?\w+)?'
        matches = re.findall(pattern, query, re.IGNORECASE)

        for match in matches:
            tables.add(match.lower())

        return tables
