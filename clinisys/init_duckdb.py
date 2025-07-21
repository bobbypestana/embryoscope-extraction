import os
import sys
import logging
from datetime import datetime
import duckdb

# Debug mode
DEBUG_MODE = True

# Paths
BASE_DIR = os.path.dirname(__file__)
# Use Desktop for test
DESKTOP_DIR = os.path.join(os.path.expanduser('~'), 'Desktop')
DATABASE_DIR = DESKTOP_DIR
DUCKDB_PATH = os.path.join(DATABASE_DIR, 'clinisys_all.duckdb')
LOGS_DIR = os.path.join(BASE_DIR, 'logs')
os.makedirs(LOGS_DIR, exist_ok=True)

# Timestamped log file
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
LOG_PATH = os.path.join(LOGS_DIR, f'init_duckdb_{timestamp}.log')

# Setup logging to both file and terminal
log_level = logging.DEBUG if DEBUG_MODE else logging.INFO
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')

file_handler = logging.FileHandler(LOG_PATH)
file_handler.setLevel(log_level)
file_handler.setFormatter(formatter)

stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setLevel(log_level)
stream_handler.setFormatter(formatter)

logger = logging.getLogger(__name__)
logger.setLevel(log_level)
logger.handlers = []
logger.addHandler(file_handler)
logger.addHandler(stream_handler)

def main():
    logger.info('Starting DuckDB database creation script (Desktop test)')
    logger.debug(f'Ensuring database directory exists: {DATABASE_DIR}')
    try:
        os.makedirs(DATABASE_DIR, exist_ok=True)
        logger.info(f'Database directory ensured: {DATABASE_DIR}')
    except Exception as e:
        logger.error(f'Failed to create database directory: {e}', exc_info=True)
        sys.exit(1)

    logger.debug(f'Attempting to create DuckDB at: {DUCKDB_PATH}')
    try:
        con = duckdb.connect(DUCKDB_PATH)
        logger.info(f'DuckDB database created or opened at: {DUCKDB_PATH}')
        con.close()
        logger.info('DuckDB connection closed')
    except Exception as e:
        logger.error(f'Failed to create DuckDB database: {e}', exc_info=True)
        sys.exit(1)
    logger.info('DuckDB database creation script finished successfully')

if __name__ == '__main__':
    main() 