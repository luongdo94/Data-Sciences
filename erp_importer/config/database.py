"""
Database configuration and connection management.

This module provides functions to manage database connections and configurations.
"""
import pyodbc
from typing import Optional

# Default database file path
DEFAULT_DB_PATH = r"C:\Users\gia.luongdo\Desktop\ERP-Importer\db_Artikel_Export2.mdb"

def get_db_connection(db_path: Optional[str] = None):
    """
    Create and return a database connection.

    Args:
        db_path (str, optional): Path to the database file. If not provided, uses the default path.

    Returns:
        pyodbc.Connection: A connection to the database.
    """
    db_path = db_path or DEFAULT_DB_PATH
    conn_str = (
        r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
        f"DBQ={db_path};"
    )
    return pyodbc.connect(conn_str)


def test_connection():
    """
    Test the database connection.

    Returns:
        bool: True if connection is successful, False otherwise.
    """
    try:
        conn = get_db_connection()
        conn.close()
        return True
    except Exception as e:
        print(f"Connection failed: {e}")
        return False
