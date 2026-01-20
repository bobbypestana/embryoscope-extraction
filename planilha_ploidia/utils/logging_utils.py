"""Logging utilities for standardized logging setup"""
import logging
import os
from datetime import datetime
from config.database import DatabaseConfig


def setup_logger(script_name, logs_dir=None):
    """
    Setup standardized logging for scripts.
    
    Args:
        script_name: Name of the script (used for log filename)
        logs_dir: Directory for log files (defaults to config.database.DatabaseConfig.LOGS_DIR)
    
    Returns:
        logging.Logger: Configured logger instance
    """
    if logs_dir is None:
        logs_dir = DatabaseConfig.LOGS_DIR
    
    os.makedirs(logs_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_path = os.path.join(logs_dir, f'{script_name}_{timestamp}.log')
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(message)s',
        handlers=[
            logging.FileHandler(log_path),
            logging.StreamHandler()
        ]
    )
    
    logger = logging.getLogger(script_name)
    logger.info(f"Logging to: {log_path}")
    
    return logger
