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
        camp.name AS "Campanha Nome",
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
        ON d.source_id = src.id
    LEFT JOIN silver.campaigns camp
        ON d.campaign_id = camp.id;
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

def create_gold_campaigns(con):
    logger.info("Creating table gold.rdstation_campaigns...")
    con.execute("DROP TABLE IF EXISTS gold.rdstation_campaigns")
    
    # Check if silver.campaigns exists
    has_campaigns = con.execute("""
        SELECT COUNT(*) FROM information_schema.tables 
        WHERE table_schema = 'silver' AND table_name = 'campaigns'
    """).fetchone()[0]
    
    if not has_campaigns:
        logger.warning("silver.campaigns not found. Creating empty gold.rdstation_campaigns.")
        con.execute("""
            CREATE TABLE gold.rdstation_campaigns (
                "Campanha ID" VARCHAR,
                "Campanha Nome" VARCHAR,
                "Data Cadastro" TIMESTAMP,
                "Data Início" TIMESTAMP,
                "Data Fim" TIMESTAMP,
                "Status Campanha" VARCHAR,
                "Total Leads" BIGINT,
                "Dias Desde Último Lead" INTEGER
            )
        """)
        return

    query = """
    CREATE TABLE gold.rdstation_campaigns AS
    WITH campaign_lead_stats AS (
        SELECT 
            c.id AS campaign_id,
            c.name AS campaign_name,
            c.created_at AS campaign_created_at,
            COUNT(d.id) AS total_leads,
            MIN(d.created_at) AS first_lead_at,
            MAX(d.created_at) AS last_lead_at,
            date_diff('day', MAX(d.created_at), current_date) AS days_since_last_lead,
            date_diff('day', MIN(d.created_at), MAX(d.created_at)) AS active_span_days
        FROM silver.campaigns c
        LEFT JOIN silver.deals d ON c.id = d.campaign_id
        GROUP BY c.id, c.name, c.created_at
    ),
    campaign_lifecycle AS (
        SELECT
            cls.*,
            -- Adaptive lifecycle based purely on created_at and volume
            CASE 
                -- When no leads were ever received: if created recently (<180d), consider active/ramp-up, else closed
                WHEN cls.total_leads = 0 AND date_diff('day', cls.campaign_created_at, current_date) <= 180 THEN 'Active'
                WHEN cls.total_leads = 0 THEN 'Closed'

                -- Tier 1: High Volume (> 500 leads)
                WHEN cls.total_leads > 500 AND cls.days_since_last_lead <= 45 THEN 'Active'
                WHEN cls.total_leads > 500 AND cls.days_since_last_lead <= 90 THEN 'Dormant'
                WHEN cls.total_leads > 500 THEN 'Closed'

                -- Tier 2: Medium Volume (50 - 500 leads)
                WHEN cls.total_leads BETWEEN 50 AND 500 AND cls.days_since_last_lead <= 90 THEN 'Active'
                WHEN cls.total_leads BETWEEN 50 AND 500 AND cls.days_since_last_lead <= 150 THEN 'Dormant'
                WHEN cls.total_leads BETWEEN 50 AND 500 THEN 'Closed'

                -- Tier 3: Low Volume (< 50 leads) - wide window protecting niche campaigns
                WHEN cls.total_leads < 50 AND cls.days_since_last_lead <= 150 THEN 'Active'
                WHEN cls.total_leads < 50 AND cls.days_since_last_lead <= 240 THEN 'Dormant'
                ELSE 'Closed'
            END AS calculated_status
        FROM campaign_lead_stats cls
    )
    SELECT
        campaign_id AS "Campanha ID",
        campaign_name AS "Campanha Nome",
        campaign_created_at AS "Data Cadastro",
        -- Start date: earliest of RD campaign registration and first lead created
        CASE 
            WHEN first_lead_at IS NOT NULL AND first_lead_at < campaign_created_at THEN first_lead_at
            ELSE campaign_created_at 
        END AS "Data Início",
        -- End date: set to last lead date when closed; NULL if still active or dormant
        CASE 
            WHEN calculated_status = 'Closed' THEN last_lead_at
            ELSE NULL 
        END AS "Data Fim",
        last_lead_at AS "Data Último Lead",
        calculated_status AS "Status Campanha",
        total_leads AS "Total Leads",
        days_since_last_lead AS "Dias Desde Último Lead"
    FROM campaign_lifecycle;
    """
    con.execute(query)
    count = con.execute("SELECT COUNT(*) FROM gold.rdstation_campaigns").fetchone()[0]
    logger.info(f"Created gold.rdstation_campaigns with {count} rows")

def main():
    logger.info("=== RD STATION SILVER TO GOLD CONSOLIDATION STARTED ===")
    logger.info(f"Target Database: {DUCKDB_PATH}")

    try:
        with duckdb.connect(DUCKDB_PATH) as con:
            con.execute("CREATE SCHEMA IF NOT EXISTS gold")
            
            create_gold_deals(con)
            create_gold_contacts(con)
            create_gold_deal_contacts(con)
            create_gold_campaigns(con)
            
            logger.info("=== RD STATION SILVER TO GOLD CONSOLIDATION FINISHED SUCCESSFUL ===")
    except Exception as e:
        logger.error(f"Gold Consolidation Failed: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    main()
