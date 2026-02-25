import duckdb
import os
import logging
from datetime import datetime

# Configuration - Absolute paths to ensure consistency
BASE_DIR = r'g:\My Drive\projetos_individuais\Huntington'
DB_PATH = os.path.join(BASE_DIR, 'database', 'huntington_data_lake.duckdb')
LOGS_DIR = os.path.join(BASE_DIR, 'embryoscope', 'logs')

# Setup logging
os.makedirs(LOGS_DIR, exist_ok=True)
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
LOG_PATH = os.path.join(LOGS_DIR, f'query_biopsy_counts_{timestamp}.log')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(LOG_PATH),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def main():
    if not os.path.exists(DB_PATH):
        logger.error(f"Database not found at: {DB_PATH}")
        return

    conn = duckdb.connect(DB_PATH)

    logger.info("=" * 70)
    logger.info("EMBRYO COUNTS BY CATEGORY (BIOPSY & OUTCOME)")
    logger.info(f"Source: gold.data_ploidia")
    logger.info("=" * 70)

    # Check data_ploidia totals
    dp_total_rows = conn.execute('SELECT COUNT(*) FROM gold.data_ploidia').fetchone()[0]
    dp_unique_embryos = conn.execute('SELECT COUNT(DISTINCT "Slide ID") FROM gold.data_ploidia WHERE "Slide ID" IS NOT NULL').fetchone()[0]

    logger.info(f"Table Statistics:")
    logger.info(f"  Total Rows: {dp_total_rows:,}")
    logger.info(f"  Unique Slide IDs: {dp_unique_embryos:,}")
    logger.info("-" * 70)

    # Query from data_ploidia (the actual source)
    total = dp_unique_embryos

    if total == 0:
        logger.warning("No embryos found in gold.data_ploidia.")
        conn.close()
        return

    # Categories
    with_biopsy = conn.execute("""
        SELECT COUNT(DISTINCT "Slide ID") 
        FROM gold.data_ploidia
        WHERE "Slide ID" IS NOT NULL 
        AND ("Embryo Description" IS NOT NULL 
             OR "Embryo Description Clinisys" IS NOT NULL 
             OR "Embryo Description Clinisys Detalhes" IS NOT NULL)
    """).fetchone()[0]

    without_biopsy_with_outcome = conn.execute("""
        SELECT COUNT(DISTINCT "Slide ID") 
        FROM gold.data_ploidia
        WHERE "Slide ID" IS NOT NULL 
        AND ("Embryo Description" IS NULL 
             AND "Embryo Description Clinisys" IS NULL 
             AND "Embryo Description Clinisys Detalhes" IS NULL)
        AND (outcome_type IS NOT NULL 
             OR merged_numero_de_nascidos IS NOT NULL 
             OR fet_gravidez_clinica IS NOT NULL 
             OR fet_tipo_resultado IS NOT NULL 
             OR trat1_resultado_tratamento IS NOT NULL 
             OR trat2_resultado_tratamento IS NOT NULL)
    """).fetchone()[0]

    other = total - with_biopsy - without_biopsy_with_outcome

    # Detailed Biopsy Breakdown
    embryo_desc = conn.execute('SELECT COUNT(DISTINCT "Slide ID") FROM gold.data_ploidia WHERE "Slide ID" IS NOT NULL AND "Embryo Description" IS NOT NULL').fetchone()[0]
    clinisys_desc = conn.execute('SELECT COUNT(DISTINCT "Slide ID") FROM gold.data_ploidia WHERE "Slide ID" IS NOT NULL AND "Embryo Description Clinisys" IS NOT NULL').fetchone()[0]
    clinisys_detalhes = conn.execute('SELECT COUNT(DISTINCT "Slide ID") FROM gold.data_ploidia WHERE "Slide ID" IS NOT NULL AND "Embryo Description Clinisys Detalhes" IS NOT NULL').fetchone()[0]

    logger.info(f"EXTRACTION CRITERIA BREAKDOWN:")
    logger.info(f"  1. WITH_BIOPSY: {with_biopsy:,} embryos ({with_biopsy/total*100:.1f}%)")
    logger.info(f"     - from 'Embryo Description':            {embryo_desc:,}")
    logger.info(f"     - from 'Clinisys Embryo Description':   {clinisys_desc:,}")
    logger.info(f"     - from 'Clinisys Detalles':             {clinisys_detalhes:,}")
    logger.info("")
    logger.info(f"  2. WITHOUT_BIOPSY (with outcome): {without_biopsy_with_outcome:,} embryos ({without_biopsy_with_outcome/total*100:.1f}%)")
    logger.info("")
    logger.info(f"  3. OTHER (residual): {other:,} embryos ({other/total*100:.1f}%)")
    logger.info("=" * 70)
    logger.info(f"Metrics saved to log: {LOG_PATH}")

    conn.close()

if __name__ == '__main__':
    main()
