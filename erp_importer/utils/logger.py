"""
Logging configuration and utilities.

This module provides a centralized logging configuration for the application.
"""
import logging
import sys
from datetime import datetime
from typing import Optional

def setup_logger(
    name: str,
    log_level: int = logging.INFO,
    log_file: Optional[str] = None,
    log_format: str = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
) -> logging.Logger:
    """
    Configure and return a logger with the specified settings.

    Args:
        name (str): The name of the logger.
        log_level (int, optional): The logging level. Defaults to logging.INFO.
        log_file (str, optional): Path to the log file. If None, logs to console only.
        log_format (str, optional): The log message format.

    Returns:
        logging.Logger: Configured logger instance.
    """
    logger = logging.getLogger(name)
    logger.setLevel(log_level)

    # Clear any existing handlers
    if logger.hasHandlers():
        logger.handlers.clear()

    # Create formatter
    formatter = logging.Formatter(log_format)

    # Console handler (always add)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # File handler (if log_file is provided)
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger

def get_default_logger() -> logging.Logger:
    """
    Get a default logger with standard configuration.

    Returns:
        logging.Logger: Configured logger instance.
    """
    log_file = f'export_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
    return setup_logger(
        name=__name__,
        log_level=logging.INFO,
        log_file=log_file,
        log_format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
