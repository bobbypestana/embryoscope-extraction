import duckdb
import logging
import os
import json
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

def load_previous_counts():
    """Load previous row counts from JSON file"""
    previous_file = os.path.join(LOGS_DIR, 'previous_table_counts.json')
    if os.path.exists(previous_file):
        try:
            with open(previous_file, 'r') as f:
                return json.load(f)
        except:
            return {}
    return {}

def save_current_counts(counts):
    """Save current row counts to JSON file"""
    previous_file = os.path.join(LOGS_DIR, 'previous_table_counts.json')
    try:
        with open(previous_file, 'w') as f:
            json.dump(counts, f, indent=2)
    except Exception as e:
        logger.warning(f"Could not save current counts: {e}")

def format_delta(current, previous):
    """Format delta with + or - sign"""
    if previous is None:
        return "N/A"
    delta = current - previous
    if delta > 0:
        return f"+{delta}"
    elif delta < 0:
        return f"{delta}"
    else:
        return "0"

# Connect to the database
con = duckdb.connect('../database/clinisys_all.duckdb')

# Check table counts
bronze_result = con.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'bronze'").fetchone()
silver_result = con.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'silver'").fetchone()

bronze_count = bronze_result[0] if bronze_result else 0
silver_count = silver_result[0] if silver_result else 0

logger.info(f'Bronze tables: {bronze_count}')
logger.info(f'Silver tables: {silver_count}')

# Get table names
bronze_tables_result = con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'bronze'").fetchall()
silver_tables_result = con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'silver'").fetchall()

bronze_tables = [t[0] for t in bronze_tables_result if t and t[0]]
silver_tables = [t[0] for t in silver_tables_result if t and t[0]]

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
    try:
        bronze_cols_result = con.execute(f"SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = 'bronze' AND table_name = '{table}'").fetchone()
        silver_cols_result = con.execute(f"SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = 'silver' AND table_name = '{table}'").fetchone()
        
        bronze_cols = bronze_cols_result[0] if bronze_cols_result else 0
        silver_cols = silver_cols_result[0] if silver_cols_result else 0
        
        logger.info(f'{table}: Bronze={bronze_cols}, Silver={silver_cols}')
        if bronze_cols != silver_cols:
            logger.warning(f'Column count mismatch for {table}: Bronze={bronze_cols}, Silver={silver_cols}')
    except Exception as e:
        logger.error(f"Error checking column counts for table {table}: {e}")

# Load previous counts for delta comparison
previous_counts = load_previous_counts()
current_counts = {}

# Check row counts for all tables with delta reporting
logger.info('Row counts comparison with deltas:')
logger.info('Format: Table: Bronze=count, Silver=count (Bronze_delta, Silver_delta)')
logger.info('=' * 80)

for table in bronze_tables:
    try:
        bronze_result = con.execute(f"SELECT COUNT(*) FROM bronze.{table}").fetchone()
        silver_result = con.execute(f"SELECT COUNT(*) FROM silver.{table}").fetchone()
        
        bronze_rows = bronze_result[0] if bronze_result else 0
        silver_rows = silver_result[0] if silver_result else 0
        
        # Get previous counts
        previous_bronze = previous_counts.get(f'{table}_bronze')
        previous_silver = previous_counts.get(f'{table}_silver')
        
        # Format deltas
        bronze_delta = format_delta(bronze_rows, previous_bronze)
        silver_delta = format_delta(silver_rows, previous_silver)
        
        logger.info(f'{table}: Bronze={bronze_rows}, Silver={silver_rows} (DeltaBronze={bronze_delta}, DeltaSilver={silver_delta})')
        
        # Store current counts for next run
        current_counts[f'{table}_bronze'] = bronze_rows
        current_counts[f'{table}_silver'] = silver_rows
    except Exception as e:
        logger.error(f"Error checking table {table}: {e}")
        current_counts[f'{table}_bronze'] = 0
        current_counts[f'{table}_silver'] = 0

# Save current counts for next comparison
save_current_counts(current_counts)

# Summary statistics
total_bronze_rows = sum(current_counts.get(f'{table}_bronze', 0) for table in bronze_tables)
total_silver_rows = sum(current_counts.get(f'{table}_silver', 0) for table in bronze_tables)

logger.info('=' * 80)
logger.info(f'SUMMARY: Total Bronze rows: {total_bronze_rows:,}, Total Silver rows: {total_silver_rows:,}')

# Show tables with significant growth (more than 100 rows)
logger.info('Tables with significant growth (>100 rows):')
for table in bronze_tables:
    bronze_rows = current_counts.get(f'{table}_bronze', 0)
    previous_bronze = previous_counts.get(f'{table}_bronze')
    if previous_bronze and (bronze_rows - previous_bronze) > 100:
        growth = bronze_rows - previous_bronze
        logger.info(f'  {table}: +{growth:,} rows')

con.close()
logger.info('Database connection closed') 