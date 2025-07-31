"""
Base repository class with common database operations.

This module provides a base class for all repository classes to inherit from.
"""
from typing import Any, Dict, List, Optional, TypeVar, Generic, Type
import pandas as pd
from ..config.database import get_db_connection
from ..utils.logger import setup_logger

T = TypeVar('T')

class BaseRepository(Generic[T]):
    """Base repository class with common database operations."""
    
    def __init__(self, logger_name: Optional[str] = None):
        """
        Initialize the base repository.
        
        Args:
            logger_name (str, optional): Name for the logger. Defaults to the class name.
        """
        self.logger = setup_logger(logger_name or self.__class__.__name__)
    
    def _execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """
        Execute a query and return the results as a pandas DataFrame.
        
        Args:
            query (str): The SQL query to execute.
            params (Dict[str, Any], optional): Parameters for the query.
            
        Returns:
            pd.DataFrame: The query results.
        """
        try:
            with get_db_connection() as conn:
                if params:
                    return pd.read_sql(query, conn, params=params)
                return pd.read_sql(query, conn)
        except Exception as e:
            self.logger.error(f"Error executing query: {e}", exc_info=True)
            raise
    
    def _execute_non_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> int:
        """
        Execute a non-query (INSERT, UPDATE, DELETE) and return the number of affected rows.
        
        Args:
            query (str): The SQL query to execute.
            params (Dict[str, Any], optional): Parameters for the query.
            
        Returns:
            int: Number of affected rows.
        """
        try:
            with get_db_connection() as conn:
                cursor = conn.cursor()
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                conn.commit()
                return cursor.rowcount
        except Exception as e:
            self.logger.error(f"Error executing non-query: {e}", exc_info=True)
            if 'conn' in locals():
                conn.rollback()
            raise
    
    def _get_one(self, query: str, params: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        """
        Execute a query and return the first row as a dictionary.
        
        Args:
            query (str): The SQL query to execute.
            params (Dict[str, Any], optional): Parameters for the query.
            
        Returns:
            Optional[Dict[str, Any]]: The first row as a dictionary, or None if no rows.
        """
        try:
            with get_db_connection() as conn:
                cursor = conn.cursor()
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                
                columns = [column[0] for column in cursor.description]
                row = cursor.fetchone()
                
                if row:
                    return dict(zip(columns, row))
                return None
        except Exception as e:
            self.logger.error(f"Error getting one record: {e}", exc_info=True)
            raise
