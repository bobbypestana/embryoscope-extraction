#!/usr/bin/env python3
"""
RD Station Silver to Gold Consolidation Script
Combines Silver tables (deals, pipelines, stages, users, sources, contacts)
to build final gold schema tables for dashboards and reports.
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

def create_gold_deals(con):
    logger.info("Creating table gold.rdstation_deals...")
    con.execute("DROP TABLE IF EXISTS gold.rdstation_deals")
    
    query = """
    CREATE TABLE gold.rdstation_deals AS
    SELECT 
        d.id AS "Deal ID",
        d.name AS "Negócio",
        d.status AS "Status",
        d.total_price AS "Valor Total",
        d.one_time_price AS "Valor Único",
        d.recurrence_price AS "Valor Recorrente",
        d.rating AS "Estrelas",
        d.expected_close_date AS "Data Fechamento Estimada",
        d.created_at AS "Data Criação",
        d.updated_at AS "Data Atualização",
        d.closed_at AS "Data Fechamento",
        p.name AS "Funil",
        s.name AS "Etapa",
        u.name AS "Responsável",
        src.name AS "Fonte",
        d.campaign_id AS "Campanha ID",
        d.lost_reason_id AS "Motivo Perda ID",
        -- Custom Fields
        d.custom_nome_completo AS "Nome Completo",
        d.custom_unidade AS "Unidade",
        d.custom_procurou_por AS "Procurou Por",
        d.custom_data_de_agendamento AS "Data de Agendamento",
        d.custom_agendado_por AS "Agendado Por",
        d.custom_agendou_com AS "Agendou Com",
        d.custom_como_conheceu_a_huntington AS "Como Conheceu a Huntington",
        d.custom_medico_encaminhante AS "Médico Encaminhante",
        d.custom_canal_de_atendimento AS "Canal de Atendimento",
        d.custom_tipo_paciente_clinisys AS "Tipo Paciente CliniSYS",
        d.extraction_timestamp AS "extraction_timestamp"
    FROM silver.deals d
    LEFT JOIN silver.pipelines p
        ON d.pipeline_id = p.id
    LEFT JOIN silver.stages s
        ON d.stage_id = s.id AND d.pipeline_id = s.pipeline_id
    LEFT JOIN silver.users u
        ON d.owner_id = u.id
    LEFT JOIN silver.sources src
        ON d.source_id = src.id;
    """
    con.execute(query)
    count = con.execute("SELECT COUNT(*) FROM gold.rdstation_deals").fetchone()[0]
    logger.info(f"Created gold.rdstation_deals with {count} rows")

def create_gold_contacts(con):
    logger.info("Creating table gold.rdstation_contacts...")
    con.execute("DROP TABLE IF EXISTS gold.rdstation_contacts")
    
    query = """
    CREATE TABLE gold.rdstation_contacts AS
    SELECT 
        id AS "Contact ID",
        name AS "Nome Contato",
        primary_email AS "E-mail",
        primary_phone AS "Telefone",
        custom_cpf AS "CPF",
        custom_pin AS "PIN",
        created_at AS "Data Criação Contato",
        updated_at AS "Data Atualização Contato",
        extraction_timestamp AS "extraction_timestamp"
    FROM silver.contacts;
    """
    con.execute(query)
    count = con.execute("SELECT COUNT(*) FROM gold.rdstation_contacts").fetchone()[0]
    logger.info(f"Created gold.rdstation_contacts with {count} rows")

def create_gold_deal_contacts(con):
    logger.info("Creating bridge table gold.rdstation_deal_contacts...")
    con.execute("DROP TABLE IF EXISTS gold.rdstation_deal_contacts")
    
    # We parse the contact_ids array (represented as double-quoted JSON) and unnest it
    query = """
    CREATE TABLE gold.rdstation_deal_contacts AS
    SELECT 
        id AS "Deal ID",
        UNNEST(json_extract(contact_ids, '$')::VARCHAR[]) AS "Contact ID"
    FROM silver.deals
    WHERE contact_ids IS NOT NULL 
      AND contact_ids != '[]' 
      AND contact_ids != '';
    """
    con.execute(query)
    count = con.execute("SELECT COUNT(*) FROM gold.rdstation_deal_contacts").fetchone()[0]
    logger.info(f"Created gold.rdstation_deal_contacts with {count} rows")

def main():
    logger.info("=== RD STATION SILVER TO GOLD CONSOLIDATION STARTED ===")
    logger.info(f"Target Database: {DUCKDB_PATH}")

    try:
        with duckdb.connect(DUCKDB_PATH) as con:
            con.execute("CREATE SCHEMA IF NOT EXISTS gold")
            
            create_gold_deals(con)
            create_gold_contacts(con)
            create_gold_deal_contacts(con)
            
            logger.info("=== RD STATION SILVER TO GOLD CONSOLIDATION FINISHED SUCCESSFUL ===")
    except Exception as e:
        logger.error(f"Gold Consolidation Failed: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    main()
