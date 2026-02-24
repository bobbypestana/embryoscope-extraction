import duckdb
import os
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.FileHandler(f"logs/02_create_wide_table_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def main():
    logger.info("=== STARTING PRESCRIPTION WIDE TABLE CREATION ===")
    
    db_path = r'g:\My Drive\projetos_individuais\Huntington\database\huntington_data_lake.duckdb'
    if not os.path.exists(db_path):
        logger.error(f"Database not found at {db_path}")
        return

    conn = duckdb.connect(db_path)
    
    try:
        # 1. Get unique medication groups to ensure clean column names
        groups_df = conn.execute("""
            SELECT DISTINCT presc_grupo_medicamento 
            FROM gold.embryos_with_prescription_long 
            WHERE presc_grupo_medicamento IS NOT NULL
        """).df()
        
        groups = groups_df['presc_grupo_medicamento'].tolist()
        logger.info(f"Pivoting {len(groups)} medication groups.")

        # 2. Create the wide table using PIVOT
        # We aggregate multiple entries for the same oocito_id + group (e.g. dose adjustments)
        # using SUM for dose/days and MIN/MAX for dates.
        
        logger.info("Executing Pivot and Join metadata...")
        
        pivot_query = """
        CREATE OR REPLACE TABLE gold.embryos_with_prescription_wide AS
        WITH aggregated_long AS (
            SELECT 
                oocito_id,
                presc_grupo_medicamento,
                sum(presc_dose_total) as total_dose,
                sum(presc_numero_dias) as total_days,
                min(presc_data_inicial) as start_date,
                max(presc_data_final) as end_date,
                FIRST(presc_unidade_padronizada) as unit,
                FIRST(presc_intervalo) as interval
            FROM gold.embryos_with_prescription_long
            WHERE presc_grupo_medicamento IS NOT NULL
            GROUP BY 1, 2
        ),
        pivoted AS (
            PIVOT aggregated_long
            ON presc_grupo_medicamento
            USING 
                FIRST(total_dose) as dose, 
                FIRST(total_days) as days, 
                FIRST(start_date) as start, 
                FIRST(end_date) as end,
                FIRST(unit) as unit,
                FIRST(interval) as interval
        ),
        embryo_metadata AS (
            -- Extract embryo info directly from the long table, ensuring uniqueness
            SELECT DISTINCT ON (oocito_id) * EXCLUDE (
                presc_id, 
                presc_ficha_id,
                presc_medicamento,
                presc_med_nome,
                presc_dose,
                presc_unidade,
                presc_unidade_padronizada,
                presc_intervalo,
                presc_data_inicial,
                presc_data_final,
                presc_numero_dias,
                presc_dose_total,
                presc_grupo_medicamento
            )
            FROM gold.embryos_with_prescription_long
        )
        SELECT 
            m.*,
            p.* EXCLUDE (oocito_id)
        FROM embryo_metadata m
        LEFT JOIN pivoted p ON m.oocito_id = p.oocito_id
        ORDER BY m.oocito_id DESC
        """
        
        conn.execute(pivot_query)
        logger.info("Table gold.embryos_with_prescription_wide created successfully.")

        # 3. Statistics
        total_embryos = conn.execute("SELECT COUNT(*) FROM gold.embryos_with_prescription_wide").fetchone()[0]
        with_any_presc = conn.execute("""
            SELECT COUNT(*) FROM gold.embryos_with_prescription_wide 
            WHERE oocito_id IN (SELECT DISTINCT oocito_id FROM gold.embryos_with_prescription_long WHERE presc_id IS NOT NULL)
        """).fetchone()[0]
        
        logger.info(f"Total embryos in wide table: {total_embryos:,}")
        logger.info(f"Embryos with ≥1 prescription: {with_any_presc:,} ({with_any_presc/total_embryos:.1%})")

        # 4. Detailed Group Report
        logger.info("=" * 60)
        logger.info("EMBRYOS PER MEDICATION GROUP")
        logger.info("=" * 60)
        group_stats = conn.execute("""
            SELECT presc_grupo_medicamento, COUNT(DISTINCT oocito_id) as embryo_count
            FROM gold.embryos_with_prescription_long
            WHERE presc_grupo_medicamento IS NOT NULL
            GROUP BY 1
            ORDER BY 2 DESC
        """).fetchall()

        for group, count in group_stats:
            logger.info(f"  {group:<30}: {count:>6,}")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Error during wide table creation: {e}")
        raise
    finally:
        conn.close()
        logger.info("Done.")

if __name__ == "__main__":
    if not os.path.exists("logs"):
        os.makedirs("logs")
    main()
