"""
Database operations for the warehouse export module.
"""

import logging
import os
import pyodbc
import pandas as pd
from typing import Optional

logger = logging.getLogger(__name__)

class DatabaseExporter:
    """Handles database export operations."""
    
    def __init__(self, config):
        """Initialize with configuration."""
        self.config = config
        self.connection = None
        
    def connect(self, username: str, password: str) -> None:
        """Establish database connection."""
        conn_str = (
            f'DRIVER={{SQL Server}};'
            f'SERVER={self.config.server};'
            f'DATABASE={self.config.database};'
            f'UID={username};'
            f'PWD={password}'
        )
        self.connection = pyodbc.connect(conn_str)
        logger.info("Database connection established")
        
    def disconnect(self) -> None:
        """Close database connection."""
        if self.connection:
            self.connection.close()
            self.connection = None
            logger.info("Database connection closed")
            
    def execute_query(self, query: str) -> pd.DataFrame:
        """Execute SQL query and return results as DataFrame."""
        if not self.connection:
            raise ConnectionError("Not connected to database")
            
        logger.info("Executing SQL query")
        return pd.read_sql_query(query, self.connection)
    
    def export_to_csv(self, df: pd.DataFrame, output_path: str) -> None:
        """Export DataFrame to CSV file."""
        try:
            output_dir = os.path.dirname(output_path)
            if output_dir and not os.path.exists(output_dir):
                os.makedirs(output_dir, exist_ok=True)
                
            df.to_csv(output_path, sep=';', index=False, encoding='windows-1252')
            logger.info(f"Data exported to {output_path}")
        except Exception as e:
            logger.error(f"Export failed: {str(e)}")
            raise


