"""Database module for Microsoft Access using pyodbc"""
import pyodbc
import pandas as pd
from pathlib import Path
from config import MDB_FILE, SQL_DIR

_conn = None

def _get_connection():
    global _conn
    if _conn is None:
        conn_str = f"DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={MDB_FILE};"
        _conn = pyodbc.connect(conn_str)
    return _conn

def execute_sql_file(filename, params=None):
    """Execute SQL from file and return results as DataFrame"""
    try:
        file_path = SQL_DIR / filename
        with open(file_path, 'r', encoding='utf-8') as f:
            query = f.read()
        conn = _get_connection()
        return pd.read_sql(query, conn, params=params)
    except Exception as e:
        global _conn
        if _conn:
            _conn.close()
            _conn = None
        raise

import atexit
atexit.register(lambda: _conn.close() if _conn else None)
