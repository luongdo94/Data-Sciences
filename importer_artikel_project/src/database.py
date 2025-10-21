import os
import pyodbc
import pandas as pd
from dotenv import load_dotenv
from .config import CONN_STR, SQL_DIR

# Load environment variables from .env file
load_dotenv()

def execute_query(query, params=None):
    """
    Execute a SQL query and return results as a DataFrame
    
    Args:
        query (str): SQL query string
        params (tuple/list/dict, optional): Parameters for the query
        
    Returns:
        pd.DataFrame: Query results
    """
    try:
        with pyodbc.connect(CONN_STR) as conn:
            if isinstance(params, (tuple, list)):
                return pd.read_sql(query, conn, params=params)
            return pd.read_sql(query, conn, params=params)
    except Exception as e:
        print(f"Error in query: {query[:200]}...")
        if params:
            print(f"Parameters: {params[:5]}... (total: {len(params)} parameters)")
        raise Exception(f"Error executing query: {e}")

def read_csv_file(file_path, encoding='utf-8-sig', delimiter=';', required_columns=None, dtype=None):
    """Read CSV file with error handling."""
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
    
    df = pd.read_csv(file_path, encoding=encoding, delimiter=delimiter, dtype=dtype, on_bad_lines='warn')
    
    if required_columns:
        missing = [col for col in required_columns if col not in df.columns]
        if missing:
            raise ValueError(f"Missing columns: {', '.join(missing)}")
    
    return df

def read_sql_query(sql_file, aids=None):
    """Read and format SQL query with optional AIDs"""
    from pathlib import Path
    
    sql_path = Path(__file__).parent.parent / "sql" / sql_file
    if not sql_path.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_path}")
    
    sql_query = sql_path.read_text(encoding='utf-8').strip()
    sql_query = '\n'.join(line for line in sql_query.split('\n') 
                          if not line.strip().startswith('--'))
    
    if aids:
        formatted_aids = ["'" + str(aid).replace("'", "''") + "'" for aid in aids]
        sql_query = sql_query.replace("{aid_placeholders}", ", ".join(formatted_aids))
    
    return sql_query

def get_sql_server_connection():
    """
    Create a connection to SQL Server using environment variables.
    Requires the following environment variables to be set:
    - SQL_SERVER: Server name/address
    - SQL_DATABASE: Database name
    - SQL_USERNAME: SQL Server username
    - SQL_PASSWORD: SQL Server password
    
    Returns:
        pyodbc.Connection: A connection to the SQL Server database
    """
    server = os.getenv('SQL_SERVER')
    database = os.getenv('SQL_DATABASE')
    username = os.getenv('SQL_USERNAME')
    password = os.getenv('SQL_PASSWORD')
    
    if not all([server, database, username, password]):
        raise ValueError("Missing required SQL Server environment variables. Please set SQL_SERVER, SQL_DATABASE, SQL_USERNAME, and SQL_PASSWORD")
    
    connection_string = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}"
    
    try:
        conn = pyodbc.connect(connection_string)
        return conn
    except Exception as e:
        raise Exception(f"Error connecting to SQL Server: {e}")


def read_sql_server_query(query, params=None):
    """
    Execute a query on SQL Server and return results as a DataFrame
    
    Args:
        query (str): SQL query to execute
        params (dict, optional): Parameters for the query
        
    Returns:
        pd.DataFrame: Query results
    """
    try:
        with get_sql_server_connection() as conn:
            return pd.read_sql(query, conn, params=params)
    except Exception as e:
        raise Exception(f"Error executing SQL Server query: {e}")
