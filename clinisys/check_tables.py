import duckdb
import logging
import os
from datetime import datetime

# Setup logging
LOGS_DIR = os.path.join(os.path.dirname(__file__), 'logs')
os.makedirs(LOGS_DIR, exist_ok=True)
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
LOG_PATH = os.path.join(LOGS_DIR, f'check_tables_{timestamp}.log')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.FileHandler(LOG_PATH),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Connect to the database
con = duckdb.connect('../database/clinisys_all.duckdb')

# Check table counts
bronze_count = con.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'bronze'").fetchone()[0]
silver_count = con.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'silver'").fetchone()[0]

logger.info(f'Bronze tables: {bronze_count}')
logger.info(f'Silver tables: {silver_count}')

# Get table names
bronze_tables = [t[0] for t in con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'bronze'").fetchall()]
silver_tables = [t[0] for t in con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'silver'").fetchall()]

logger.info(f'Bronze tables: {bronze_tables}')
logger.info(f'Silver tables: {silver_tables}')

# Check which tables are missing from silver
missing_tables = set(bronze_tables) - set(silver_tables)
if missing_tables:
    logger.warning(f'Missing from silver: {missing_tables}')
else:
    logger.info('All bronze tables have corresponding silver tables')

# Check column counts for all tables
logger.info('Column counts comparison:')
for table in bronze_tables:
    bronze_cols = con.execute(f"SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = 'bronze' AND table_name = '{table}'").fetchone()[0]
    silver_cols = con.execute(f"SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = 'silver' AND table_name = '{table}'").fetchone()[0]
    logger.info(f'{table}: Bronze={bronze_cols}, Silver={silver_cols}')
    if bronze_cols != silver_cols:
        logger.warning(f'Column count mismatch for {table}: Bronze={bronze_cols}, Silver={silver_cols}')

# Check row counts for all tables
logger.info('Row counts comparison:')
for table in bronze_tables:
    bronze_rows = con.execute(f"SELECT COUNT(*) FROM bronze.{table}").fetchone()[0]
    silver_rows = con.execute(f"SELECT COUNT(*) FROM silver.{table}").fetchone()[0]
    logger.info(f'{table}: Bronze={bronze_rows}, Silver={silver_rows}')

con.close()
logger.info('Database connection closed') 