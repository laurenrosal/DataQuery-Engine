# SecureQuery
SecureQuery is a modular sysem that allows users to query cybersecuity data using natural language. It ranslate user input into SQL using an LLM, validates the query for safetly, and  ececute it on SQLite database. 

The system is designed with a strong focus on security, ensureing that only safe, read-only queries are allowed while preventing harmful operations such as data modification or deletion. 

## Features
- Natural language to SQL translation usng LLMs 
- Secure SQL validation (SELECT-only queries)
- Modular architecture for scalability 
- CSV data ingestion into SQLite 
- CLI-based interface for easy interaction 

## Architecture

The system is composed of several key components:

- **CLI Interface** – Entry point for user input
- **Query Service** – Orchestrates the query workflow
- **LLM Adapter** – Converts natural language into SQL
- **SQL Validator** – Ensures queries are safe to execute
- **Schema Manager** – Maintains database structure information
- **CSV Loader** – Loads raw data into SQLite
- **SQLite Database** – Stores cybersecurity logs
