import logging
from logging.handlers import RotatingFileHandler
import os

def setup_logging(log_file='logs/app.log', log_level=logging.DEBUG):
    # Create a logs directory if it doesn't exist
    os.makedirs('logs', exist_ok=True)

    # Configure logging
    max_log_size = 5 * 1024 * 1024  # 5 MB
    backup_count = 3  # Keep 3 backup files

    # Create a RotatingFileHandler
    file_handler = RotatingFileHandler(
        log_file, maxBytes=max_log_size, backupCount=backup_count
    )
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))

    # Create a StreamHandler for console output
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))

    # Get the root logger and set its level
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    # Remove any existing handlers to avoid duplication
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # Add the file and console handlers to the root logger
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

def get_logger(name):
    return logging.getLogger(name)
