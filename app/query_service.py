import sqlite3
from database.database import getconnection
from database.schema_manager import SchemaManager
from database.csv_loader import CSVLoader
from app.sql_validator import SQLValidator, ValidationError

class QueryService:
    # Two main flows:
    # 1. data ingestion - loading CSV dile into the database 
    # 2. query processing - validating and excuting a SQL

    def __init__(self, db_path=None):
        #store the DB path so every method uses the same database
        self.db_path = db_path

        #Instantiate all dependenceies - QueryService owns these 
        self.schema_manager = SchemaManager(db_path)
        self.csv_loader = CSVLoader(db_path)
        self.validator = SQLValidator()

    
    #FLOW 1: DATA INGESTION
    def load_csv(self, csv_path: str, table_name:str, on_conflict: str = "append") -> dict:
        #loading a CSV file into database 
        return self.csv_loader.load(csv_path, table_name, on_conflict)
    

    #FLOW 2: QUERY PROCESSING 
    def execute_query(self, query:str) -> list[tuple]:
        #Validate and exeute a SQL query against the database 

        #Gets the full schema so validator can check
        schema = self.schema_manager.get_all_schemas()

        #Validate beore tounching the databse 
        validated_query = self.validator.validate(query, schema)

        #execte the validated query
        with getconnection(self.db_path) as conn:
            cursor = conn.execute(validated_query)
            rows = cursor.fetchall()

        
        #convert sqlite3 row objects to plain tuplpes
        return [tuple(row) for row in rows]
    

    #Return the full database schema 
    def get_schema(self) -> dict:
        return self.schema_manager.get_all_schemas()
    
    
    #return a list of all table names in the database
    def get_tables(self) -> list[str]:
        return self.schema_manager.get_tables()
    