#!/usr/bin/env python3
"""
Compare gold.data_ploidia (original) with gold.data_ploidia_new (refactored)
to verify they produce identical results.
"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.db_utils import get_connection
from utils.logging_utils import setup_logger

logger = setup_logger('compare_tables')


def compare_tables():
    """Compare the two tables"""
    logger.info("=" * 80)
    logger.info("COMPARING gold.data_ploidia vs gold.data_ploidia_new")
    logger.info("=" * 80)
    
    conn = get_connection(read_only=True)
    
    try:
        # Get row counts
        count_old = conn.execute("SELECT COUNT(*) FROM gold.data_ploidia").fetchone()[0]
        count_new = conn.execute("SELECT COUNT(*) FROM gold.data_ploidia_new").fetchone()[0]
        
        logger.info(f"Row count - Original: {count_old}, Refactored: {count_new}")
        
        if count_old != count_new:
            logger.error(f"❌ Row counts differ! Original: {count_old}, Refactored: {count_new}")
            return False
        else:
            logger.info(f"✅ Row counts match: {count_old}")
        
        # Compare schemas
        schema_old = conn.execute("DESCRIBE gold.data_ploidia").df()
        schema_new = conn.execute("DESCRIBE gold.data_ploidia_new").df()
        
        if not schema_old.equals(schema_new):
            logger.error("❌ Schemas differ!")
            logger.info("Original columns:")
            logger.info(schema_old['column_name'].tolist())
            logger.info("Refactored columns:")
            logger.info(schema_new['column_name'].tolist())
            return False
        else:
            logger.info(f"✅ Schemas match: {len(schema_old)} columns")
        
        # Compare data (row by row)
        logger.info("Comparing data...")
        
        # Get all data sorted by Patient ID and Slide ID for consistent comparison
        df_old = conn.execute("""
            SELECT * FROM gold.data_ploidia 
            ORDER BY "Patient ID", "Slide ID", "Well"
        """).df()
        
        df_new = conn.execute("""
            SELECT * FROM gold.data_ploidia_new 
            ORDER BY "Patient ID", "Slide ID", "Well"
        """).df()
        
        # Compare
        if df_old.equals(df_new):
            logger.info("✅ Data matches perfectly!")
            return True
        else:
            logger.warning("⚠️ Data differs, checking details...")
            
            # Find differences
            differences = []
            for col in df_old.columns:
                if not df_old[col].equals(df_new[col]):
                    diff_count = (df_old[col] != df_new[col]).sum()
                    differences.append(f"  - Column '{col}': {diff_count} differences")
            
            if differences:
                logger.error(f"❌ Found differences in {len(differences)} columns:")
                for diff in differences[:10]:  # Show first 10
                    logger.error(diff)
                return False
            else:
                logger.info("✅ All columns match (differences may be due to NaN handling)")
                return True
    
    finally:
        conn.close()


if __name__ == "__main__":
    success = compare_tables()
    sys.exit(0 if success else 1)
