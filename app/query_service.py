from database.database import getconnection
from database.schema_manager import SchemaManager
from database.csv_loader import CSVLoader
from app.sql_validator import SQLValidator

# QueryService handles the main parts of the project:
# loading data and running safe SQL queries

class QueryService:
    def __init__(self, db_path=None):
        #save the database path so everything uses the same file
        self.db_path = db_path

        #set up the helper classes used by QueryService  
        self.schema_manager = SchemaManager(db_path)
        self.csv_loader = CSVLoader(db_path)
        self.validator = SQLValidator()

    #FLOW 1: DATA INGESTION
    def load_csv(self, csv_path, table_name, on_conflict="append"):
        #handles loading a csv file into the datbase 
        return self.csv_loader.load(csv_path, table_name, on_conflict)
    

    #FLOW 2: QUERY PROCESSING 
    def execute_query(self, query:str) -> list[tuple]: 
        #Gets the current schema first so the validator knows what tables 
        schema = self.schema_manager.get_all_schemas()

        #Validate the query before running anything 
        validated_query = self.validator.validate(query, schema)

        #run the query and collect the results 
        with getconnection(self.db_path) as conn:
            cursor = conn.execute(validated_query)
            rows = cursor.fetchall()
        
        #convert sqlite row objects to plain tuplpes
        return [tuple(row) for row in rows]
    
    def get_schema(self):
        return self.schema_manager.get_all_schemas()
    
    def get_tables(self):
        return self.schema_manager.get_tables()
    