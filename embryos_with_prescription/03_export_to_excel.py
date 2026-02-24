import duckdb
import pandas as pd
import os
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.FileHandler(f"logs/03_export_to_excel_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def main():
    logger.info("=== STARTING EXPORT TO EXCEL ===")
    
    db_path = r'g:\My Drive\projetos_individuais\Huntington\database\huntington_data_lake.duckdb'
    export_dir = "data_exports"
    export_path = os.path.join(export_dir, "embryos_with_prescription_wide.xlsx")
    
    if not os.path.exists("logs"):
        os.makedirs("logs")
    if not os.path.exists(export_dir):
        os.makedirs(export_dir)

    conn = duckdb.connect(db_path, read_only=True)
    
    try:
        logger.info("Loading table gold.embryos_with_prescription_wide into pandas...")
        df = conn.execute("SELECT * FROM gold.embryos_with_prescription_wide").df()
        
        logger.info(f"Loaded {len(df):,} rows and {len(df.columns):,} columns.")
        
        logger.info(f"Exporting to {export_path}...")
        # engine='openpyxl' is default for .xlsx in modern pandas
        df.to_excel(export_path, index=False)
        
        logger.info("Export completed successfully.")

    except Exception as e:
        logger.error(f"Error during export: {e}")
        raise
    finally:
        conn.close()
        logger.info("Done.")

if __name__ == "__main__":
    main()
