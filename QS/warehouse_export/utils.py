"""
Utility functions for the warehouse export module.
"""

import json
import logging
import os
from typing import Tuple
from cryptography.fernet import Fernet, InvalidToken

logger = logging.getLogger(__name__)

# Initialize Fernet cipher
cipher_suite = Fernet(b'l0cD3IJGNVc4c6dArV_u8XkXMffeumPFNd4hltjoiA8=')

def load_credential(file_path: str) -> Tuple[str, str]:
    """
    Load and decrypt database credentials from file.
    
    Args:
        file_path: Path to the encrypted credentials file
        
    Returns:
        Tuple of (username, password)
    """
    try:
        with open(file_path, 'rb') as file:
            encrypted_data = file.read()
        
        decrypted_data = cipher_suite.decrypt(encrypted_data)
        credentials = json.loads(decrypted_data.decode('utf-8'))
        
        if 'username' not in credentials or 'password' not in credentials:
            raise ValueError("Invalid credentials format")
            
        return credentials['username'], credentials['password']
        
    except FileNotFoundError:
        logger.error(f"Credentials file not found: {file_path}")
        raise
    except InvalidToken:
        logger.error("Invalid encryption key or corrupted credentials file")
        raise
    except json.JSONDecodeError:
        logger.error("Failed to parse credentials file")
        raise
    except Exception as e:
        logger.error(f"Error loading credentials: {str(e)}")
        raise

def load_query(file_path: str) -> str:
    """
    Load SQL query from file.
    
    Args:
        file_path: Path to the SQL file
        
    Returns:
        SQL query as string
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            return file.read().strip()
    except FileNotFoundError:
        logger.error(f"SQL file not found: {file_path}")
        raise
    except Exception as e:
        logger.error(f"Error reading SQL file: {str(e)}")
        raise
