#!/usr/bin/env python3
"""
RD Station Bronze to Silver Promotion Script
Promotes all columns from Bronze, applies type casts on known fields,
extracts custom fields and nested fields, and deduplicates by business PK.
"""

import os
import yaml
import logging
import duckdb
from datetime import datetime

# Setup logging standard
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LOGS_DIR = os.path.join(SCRIPT_DIR, 'logs')
os.makedirs(LOGS_DIR, exist_ok=True)
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
script_name = os.path.splitext(os.path.basename(__file__))[0]
LOG_PATH = os.path.join(LOGS_DIR, f'{script_name}_{timestamp}.log')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.FileHandler(LOG_PATH),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load parameters
PARAMS_PATH = os.path.join(SCRIPT_DIR, 'params.yml')
with open(PARAMS_PATH, 'r') as f:
    config = yaml.safe_load(f)

DUCKDB_PATH = config['duckdb_path']

def promote_pipelines(con):
    logger.info("Promoting table 'pipelines' from Bronze to Silver...")
    exists = con.execute("""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_schema = 'bronze' AND table_name = 'pipelines'
    """).fetchone()[0]
    if not exists:
        logger.error("Bronze table 'bronze.pipelines' not found.")
        return

    query = """
    CREATE OR REPLACE TABLE silver.pipelines AS
    SELECT * REPLACE (
        TRY_CAST("order" AS INTEGER) AS "order",
        CAST(try_strptime(substring(created_at, 1, 19), '%Y-%m-%dT%H:%M:%S') AS TIMESTAMP) AS created_at,
        CAST(try_strptime(substring(updated_at, 1, 19), '%Y-%m-%dT%H:%M:%S') AS TIMESTAMP) AS updated_at
    )
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY id
                   ORDER BY extraction_timestamp DESC
               ) AS rn
        FROM bronze.pipelines
        WHERE COALESCE(is_deleted, 'FALSE') = 'FALSE'
    )
    WHERE rn = 1;
    """
    con.execute(query)
    count = con.execute("SELECT COUNT(*) FROM silver.pipelines").fetchone()[0]
    logger.info(f"Successfully promoted 'pipelines' to Silver. Rows: {count}")

def promote_stages(con):
    logger.info("Promoting table 'stages' from Bronze to Silver...")
    exists = con.execute("""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_schema = 'bronze' AND table_name = 'stages'
    """).fetchone()[0]
    if not exists:
        logger.error("Bronze table 'bronze.stages' not found.")
        return

    query = """
    CREATE OR REPLACE TABLE silver.stages AS
    SELECT * REPLACE (
        TRY_CAST("order" AS INTEGER) AS "order",
        CAST(try_strptime(substring(created_at, 1, 19), '%Y-%m-%dT%H:%M:%S') AS TIMESTAMP) AS created_at,
        CAST(try_strptime(substring(updated_at, 1, 19), '%Y-%m-%dT%H:%M:%S') AS TIMESTAMP) AS updated_at
    )
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY pipeline_id, id
                   ORDER BY extraction_timestamp DESC
               ) AS rn
        FROM bronze.stages
        WHERE COALESCE(is_deleted, 'FALSE') = 'FALSE'
    )
    WHERE rn = 1;
    """
    con.execute(query)
    count = con.execute("SELECT COUNT(*) FROM silver.stages").fetchone()[0]
    logger.info(f"Successfully promoted 'stages' to Silver. Rows: {count}")

def promote_users(con):
    logger.info("Promoting table 'users' from Bronze to Silver...")
    exists = con.execute("""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_schema = 'bronze' AND table_name = 'users'
    """).fetchone()[0]
    if not exists:
        logger.error("Bronze table 'bronze.users' not found.")
        return

    query = """
    CREATE OR REPLACE TABLE silver.users AS
    SELECT * REPLACE (
        CAST(try_strptime(substring(created_at, 1, 19), '%Y-%m-%dT%H:%M:%S') AS TIMESTAMP) AS created_at,
        CAST(try_strptime(substring(updated_at, 1, 19), '%Y-%m-%dT%H:%M:%S') AS TIMESTAMP) AS updated_at
    )
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY id
                   ORDER BY extraction_timestamp DESC
               ) AS rn
        FROM bronze.users
        WHERE COALESCE(is_deleted, 'FALSE') = 'FALSE'
    )
    WHERE rn = 1;
    """
    con.execute(query)
    count = con.execute("SELECT COUNT(*) FROM silver.users").fetchone()[0]
    logger.info(f"Successfully promoted 'users' to Silver. Rows: {count}")

def promote_sources(con):
    logger.info("Promoting table 'sources' from Bronze to Silver...")
    exists = con.execute("""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_schema = 'bronze' AND table_name = 'sources'
    """).fetchone()[0]
    if not exists:
        logger.error("Bronze table 'bronze.sources' not found.")
        return

    query = """
    CREATE OR REPLACE TABLE silver.sources AS
    SELECT * REPLACE (
        CAST(try_strptime(substring(created_at, 1, 19), '%Y-%m-%dT%H:%M:%S') AS TIMESTAMP) AS created_at,
        CAST(try_strptime(substring(updated_at, 1, 19), '%Y-%m-%dT%H:%M:%S') AS TIMESTAMP) AS updated_at
    )
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY id
                   ORDER BY extraction_timestamp DESC
               ) AS rn
        FROM bronze.sources
        WHERE COALESCE(is_deleted, 'FALSE') = 'FALSE'
    )
    WHERE rn = 1;
    """
    con.execute(query)
    count = con.execute("SELECT COUNT(*) FROM silver.sources").fetchone()[0]
    logger.info(f"Successfully promoted 'sources' to Silver. Rows: {count}")

def promote_deals(con):
    logger.info("Promoting table 'deals' from Bronze to Silver...")
    exists = con.execute("""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_schema = 'bronze' AND table_name = 'deals'
    """).fetchone()[0]
    if not exists:
        logger.error("Bronze table 'bronze.deals' not found.")
        return

    query = """
    CREATE OR REPLACE TABLE silver.deals AS
    SELECT 
        id,
        name,
        status,
        TRY_CAST(total_price AS DOUBLE) AS total_price,
        TRY_CAST(one_time_price AS DOUBLE) AS one_time_price,
        TRY_CAST(recurrence_price AS DOUBLE) AS recurrence_price,
        TRY_CAST(rating AS INTEGER) AS rating,
        CAST(try_strptime(substring(expected_close_date, 1, 10), '%Y-%m-%d') AS DATE) AS expected_close_date,
        CAST(try_strptime(substring(created_at, 1, 19), '%Y-%m-%dT%H:%M:%S') AS TIMESTAMP) AS created_at,
        CAST(try_strptime(substring(updated_at, 1, 19), '%Y-%m-%dT%H:%M:%S') AS TIMESTAMP) AS updated_at,
        CAST(try_strptime(substring(closed_at, 1, 19), '%Y-%m-%dT%H:%M:%S') AS TIMESTAMP) AS closed_at,
        pipeline_id,
        stage_id,
        owner_id,
        source_id,
        campaign_id,
        lost_reason_id,
        contact_ids, -- Kept as JSON string for unnesting in Gold
        extraction_timestamp,
        -- Custom Fields Extraction
        json_extract_string(custom_fields, '$."nome-completo"') AS custom_nome_completo,
        json_extract_string(custom_fields, '$."unidade"') AS custom_unidade,
        json_extract_string(custom_fields, '$."procurou-por"') AS custom_procurou_por,
        json_extract_string(custom_fields, '$."data-de-agendamento"') AS custom_data_de_agendamento,
        json_extract_string(custom_fields, '$."agendado-por"') AS custom_agendado_por,
        json_extract_string(custom_fields, '$."agendou-com"') AS custom_agendou_com,
        json_extract_string(custom_fields, '$."como-conheceu-a-huntington"') AS custom_como_conheceu_a_huntington,
        json_extract_string(custom_fields, '$."medico-encaminhante"') AS custom_medico_encaminhante,
        json_extract_string(custom_fields, '$."canal-de-atendimento"') AS custom_canal_de_atendimento,
        json_extract_string(custom_fields, '$."tipo-paciente-clinisys"') AS custom_tipo_paciente_clinisys
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY id
                   ORDER BY extraction_timestamp DESC
               ) AS rn
        FROM bronze.deals
        WHERE COALESCE(is_deleted, 'FALSE') = 'FALSE'
    )
    WHERE rn = 1;
    """
    con.execute(query)
    count = con.execute("SELECT COUNT(*) FROM silver.deals").fetchone()[0]
    logger.info(f"Successfully promoted 'deals' to Silver. Rows: {count}")

def promote_contacts(con):
    logger.info("Promoting table 'contacts' from Bronze to Silver...")
    exists = con.execute("""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_schema = 'bronze' AND table_name = 'contacts'
    """).fetchone()[0]
    if not exists:
        logger.error("Bronze table 'bronze.contacts' not found.")
        return

    query = """
    CREATE OR REPLACE TABLE silver.contacts AS
    SELECT 
        id,
        name,
        CAST(try_strptime(substring(created_at, 1, 19), '%Y-%m-%dT%H:%M:%S') AS TIMESTAMP) AS created_at,
        CAST(try_strptime(substring(updated_at, 1, 19), '%Y-%m-%dT%H:%M:%S') AS TIMESTAMP) AS updated_at,
        extraction_timestamp,
        -- Extract primary email and phone from nested structures
        json_extract_string(emails, '$[0].email') AS primary_email,
        json_extract_string(phones, '$[0].phone') AS primary_phone,
        -- Custom Fields Extraction
        json_extract_string(custom_fields, '$.cpf') AS custom_cpf,
        json_extract_string(custom_fields, '$.pin') AS custom_pin
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY id
                   ORDER BY extraction_timestamp DESC
               ) AS rn
        FROM bronze.contacts
        WHERE COALESCE(is_deleted, 'FALSE') = 'FALSE'
    )
    WHERE rn = 1;
    """
    con.execute(query)
    count = con.execute("SELECT COUNT(*) FROM silver.contacts").fetchone()[0]
    logger.info(f"Successfully promoted 'contacts' to Silver. Rows: {count}")

def main():
    logger.info("=== RD STATION BRONZE TO SILVER PROMOTION STARTED ===")
    logger.info(f"Target Database: {DUCKDB_PATH}")

    try:
        with duckdb.connect(DUCKDB_PATH) as con:
            con.execute("CREATE SCHEMA IF NOT EXISTS silver")
            
            promote_pipelines(con)
            promote_stages(con)
            promote_users(con)
            promote_sources(con)
            promote_deals(con)
            promote_contacts(con)
            
            logger.info("=== RD STATION BRONZE TO SILVER PROMOTION FINISHED SUCCESSFUL ===")
    except Exception as e:
        logger.error(f"Silver Promotion Failed: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    main()
