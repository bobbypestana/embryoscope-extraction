#!/usr/bin/env python3
"""
Protheus API to DuckDB Bronze Ingestion Script
Extracts data from 5 endpoints, flattens Notas, maps all columns to VARCHAR,
and performs hash-based incremental appends to the database.
Handles multi-tenant ingestion for Notas and optimized page sizes.
"""

import os
import yaml
import logging
import requests
import hashlib
import pandas as pd
import duckdb
import time
from requests.auth import HTTPBasicAuth
from datetime import datetime, timedelta

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
API_CONF = config['api']
AUTH = HTTPBasicAuth(API_CONF['username'], API_CONF['password'])
BASE_URL = API_CONF['base_url']
BACKFILL_START = API_CONF.get('backfill_start_date', '20220101')

# List of all verified active/accessible tenants for branch invoices
ACCESSIBLE_TENANTS = [
    "01,010101", # Ibirapuera
    "01,010104", # Vila Mariana
    "01,010106", # Vila Mariana
    "01,010150", # Ibirapuera
    "01,010155", # Vila Mariana
    "03,030101", # Campinas
    "06,060101", # Pro Fiv
    "07,010101", # Salvador - Cenafert
    "07,020101", # Salvador - Cenafert
    "07,030101", # FIV Brasilia
]

def make_request(path, params=None, tenant_id=None):
    url = f"{BASE_URL}{path}"
    headers = {
        "Accept": "application/json"
    }
    if tenant_id:
        headers["TenantId"] = tenant_id
    try:
        r = requests.get(url, params=params, headers=headers, auth=AUTH, timeout=45)
        if r.status_code == 200:
            r.encoding = 'utf-8'
            return r.json()
        else:
            logger.error(f"API Error {r.status_code} for {path} (Tenant: {headers.get('TenantId', 'None')}): {r.text[:500]}")
            return None
    except Exception as e:
        logger.error(f"Connection Exception for {path} (Tenant: {headers.get('TenantId', 'None')}): {e}")
        return None

def compute_row_hash(row_dict):
    # Exclude metadata keys from hash computation
    clean_dict = {k: v for k, v in row_dict.items() if k not in ['hash', 'extraction_timestamp']}
    # Normalize values to string, handling None
    row_str = "|".join(f"{k}:{str(v).strip() if v is not None else ''}" for k, v in sorted(clean_dict.items()))
    return hashlib.md5(row_str.encode('utf-8')).hexdigest()

def get_existing_hashes(con, table_name):
    try:
        # Check if table exists
        exists = con.execute(f"""
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_schema = 'bronze' AND table_name = '{table_name}'
        """).fetchone()[0]
        if exists:
            hashes = con.execute(f"SELECT hash FROM bronze.{table_name}").fetchall()
            return {h[0] for h in hashes if h[0]}
        return set()
    except Exception as e:
        logger.warning(f"Could not fetch existing hashes for bronze.{table_name}: {e}")
        return set()

def write_to_bronze(con, table_name, rows):
    if not rows:
        logger.info(f"No new rows to write to bronze.{table_name}")
        return
        
    df = pd.DataFrame(rows)
    # Ensure all columns are string/NULL
    for col in df.columns:
        df[col] = df[col].apply(lambda x: None if pd.isna(x) or x is None or str(x).upper() == 'NONE' else str(x))
        
    # Check if table exists
    exists = con.execute(f"""
        SELECT COUNT(*) FROM information_schema.tables 
        WHERE table_schema = 'bronze' AND table_name = '{table_name}'
    """).fetchone()[0]
    
    con.register('df_temp', df)
    
    if not exists:
        logger.info(f"Creating table bronze.{table_name} and inserting {len(df)} rows")
        con.execute(f"CREATE TABLE bronze.{table_name} AS SELECT * FROM df_temp")
    else:
        # Check and handle schema evolution
        existing_cols = {row[0] for row in con.execute(f"DESCRIBE bronze.{table_name}").fetchall()}
        new_cols = set(df.columns) - existing_cols
        for col in new_cols:
            logger.info(f"Schema evolution: Adding column '{col}' to bronze.{table_name}")
            con.execute(f"ALTER TABLE bronze.{table_name} ADD COLUMN \"{col}\" VARCHAR")
            
        # Select and insert aligned columns
        cols_str = ", ".join(f'"{c}"' for c in df.columns)
        logger.info(f"Inserting {len(df)} new/modified rows to bronze.{table_name}")
        con.execute(f"INSERT INTO bronze.{table_name} ({cols_str}) SELECT {cols_str} FROM df_temp")
        
    con.unregister('df_temp')
    logger.info(f"Successfully wrote data to bronze.{table_name}")

def get_tenant_start_date(con, company_id, tenant_id, default_start):
    try:
        table_exists = con.execute("""
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_schema = 'bronze' AND table_name = 'notas'
        """).fetchone()[0]
        
        if not table_exists:
            return default_start
            
        has_rows = con.execute("SELECT COUNT(*) FROM bronze.notas").fetchone()[0] > 0
        if not has_rows:
            return default_start
            
        # Extract branch code from tenant_id (e.g., '01,010150' -> '010150')
        branch_id = tenant_id.split(',')[1] if ',' in tenant_id else None
        
        # Check for backfill gaps for tenants affected by the historical company_id bug
        if tenant_id in ["01,010150", "01,010155", "07,030101"]:
            if branch_id:
                min_date = con.execute(f"""
                    SELECT MIN(F2_EMISSAO) FROM bronze.notas 
                    WHERE company_id = '{company_id}' AND F2_FILIAL = '{branch_id}' AND F2_EMISSAO IS NOT NULL
                """).fetchone()[0]
            else:
                min_date = con.execute(f"""
                    SELECT MIN(F2_EMISSAO) FROM bronze.notas 
                    WHERE company_id = '{company_id}' AND F2_EMISSAO IS NOT NULL
                """).fetchone()[0]
            
            if min_date and min_date > "20220201":
                logger.info(f"Tenant {tenant_id} has a backfill gap (earliest record: {min_date}). Forcing full backfill.")
                return default_start
        
        if branch_id:
            max_date = con.execute(f"""
                SELECT MAX(F2_EMISSAO) FROM bronze.notas 
                WHERE company_id = '{company_id}' AND F2_FILIAL = '{branch_id}' AND F2_EMISSAO IS NOT NULL
            """).fetchone()[0]
        else:
            max_date = con.execute(f"""
                SELECT MAX(F2_EMISSAO) FROM bronze.notas 
                WHERE company_id = '{company_id}' AND F2_EMISSAO IS NOT NULL
            """).fetchone()[0]
        
        if max_date:
            try:
                # Subtract 2 days buffer to ensure we cover boundary updates/duplicates safely
                base_dt = datetime.strptime(max_date, "%Y%m%d")
                start_dt = base_dt - timedelta(days=2)
                # Keep it bounded by default_start so we don't go back too far unnecessarily
                if start_dt < default_start:
                    return default_start
                return start_dt
            except Exception:
                return default_start
    except Exception as e:
        logger.warning(f"Error checking tenant start date for company {company_id}: {e}")
    return default_start

def ingest_notas(con):
    table_name = "notas"
    logger.info("Starting ingestion of 'notas' (invoices)...")
    
    today = datetime.now()
    default_start_dt = datetime.strptime(BACKFILL_START, "%Y%m%d")
    
    # Cache existing hashes to avoid duplicates
    existing_hashes = get_existing_hashes(con, table_name)
    
    # Loop through each tenant sequentially
    for tenant_id in ACCESSIBLE_TENANTS:
        logger.info(f"=== Ingesting Notas for Tenant: {tenant_id} ===")
        company_id = tenant_id.split(',')[0]
        
        # 1. Quick check if tenant has any invoices at all
        check_params = {
            "dataIni": BACKFILL_START,
            "dataFim": today.strftime("%Y%m%d"),
            "nPage": 1,
            "nPageSize": 1
        }
        check_res = make_request("/rest/CONSNOTA/notas", params=check_params, tenant_id=tenant_id)
        if not check_res or "data" not in check_res or not check_res["data"]:
            logger.info(f"Tenant {tenant_id} has no invoices in the backfill range. Skipping.")
            continue
            
        # 2. Get start date for this tenant
        start_dt = get_tenant_start_date(con, company_id, tenant_id, default_start_dt)
        end_dt = today
        
        logger.info(f"Ingestion range for Tenant {tenant_id}: {start_dt.strftime('%Y%m%d')} to {end_dt.strftime('%Y%m%d')}")
        
        # 3. Chunk monthly to write granularly and keep memory consumption low
        current_start = start_dt
        while current_start <= end_dt:
            next_month = (current_start.replace(day=28) + timedelta(days=4)).replace(day=1)
            current_end = next_month - timedelta(days=1)
            if current_end > end_dt:
                current_end = end_dt
                
            data_ini_str = current_start.strftime("%Y%m%d")
            data_fim_str = current_end.strftime("%Y%m%d")
            
            logger.info(f"Processing chunk {data_ini_str}-{data_fim_str} for Tenant {tenant_id}...")
            new_rows = []
            
            page = 1
            page_size = 500  # API sweet spot
            while True:
                logger.info(f"  Fetching page {page}...")
                params = {
                    "dataIni": data_ini_str,
                    "dataFim": data_fim_str,
                    "nPage": page,
                    "nPageSize": page_size
                }
                res = make_request("/rest/CONSNOTA/notas", params=params, tenant_id=tenant_id)
                if not res or "data" not in res or not res["data"]:
                    break
                    
                extraction_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                for invoice in res["data"]:
                    # Flatten items nested list
                    items = invoice.get("ITENS", [])
                    header = {k: v for k, v in invoice.items() if k != "ITENS"}
                    
                    if not items:
                        flat_row = header.copy()
                        flat_row["company_id"] = company_id
                        flat_row["extraction_timestamp"] = extraction_ts
                        flat_row["hash"] = compute_row_hash(flat_row)
                        if flat_row["hash"] not in existing_hashes:
                            new_rows.append(flat_row)
                            existing_hashes.add(flat_row["hash"])
                    else:
                        for item in items:
                            flat_row = header.copy()
                            flat_row.update(item)
                            flat_row["company_id"] = company_id
                            flat_row["extraction_timestamp"] = extraction_ts
                            flat_row["hash"] = compute_row_hash(flat_row)
                            if flat_row["hash"] not in existing_hashes:
                                new_rows.append(flat_row)
                                existing_hashes.add(flat_row["hash"])
                                
                has_next = res.get("hasNext", False)
                if not has_next:
                    break
                page += 1
                
            if new_rows:
                logger.info(f"Writing {len(new_rows)} new/modified invoice rows for chunk {data_ini_str}-{data_fim_str}")
                write_to_bronze(con, table_name, new_rows)
                
            current_start = next_month

def ingest_full_table(con, name, path):
    logger.info(f"Starting ingestion of '{name}' (full load with row-level incremental append)...")
    existing_hashes = get_existing_hashes(con, name)
    new_rows = []
    
    extraction_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    page = 1
    page_size = 500  # API sweet spot
    while True:
        params = {
            "nPage": page,
            "nPageSize": page_size
        }
        logger.info(f"Fetching page {page} of {name}...")
        res = make_request(path, params=params, tenant_id=None)
        if not res or "data" not in res or not res["data"]:
            break
            
        for item in res["data"]:
            row_dict = item.copy()
            row_dict["extraction_timestamp"] = extraction_ts
            row_dict["hash"] = compute_row_hash(row_dict)
            if row_dict["hash"] not in existing_hashes:
                new_rows.append(row_dict)
                existing_hashes.add(row_dict["hash"])
                
        has_next = res.get("hasNext", False)
        if not has_next:
            break
        page += 1
        
    if new_rows:
        logger.info(f"Writing {len(new_rows)} new/modified rows to bronze.{name}")
        write_to_bronze(con, name, new_rows)
    else:
        logger.info(f"No new rows found for bronze.{name}")

def main():
    logger.info("=== PROTHEUS SOURCE TO BRONZE INGESTION STARTED ===")
    logger.info(f"Target Database: {DUCKDB_PATH}")
    
    max_retries = 10
    retry_delay = 15
    
    for attempt in range(1, max_retries + 1):
        con = None
        try:
            con = duckdb.connect(DUCKDB_PATH)
            con.execute("CREATE SCHEMA IF NOT EXISTS bronze")
            
            # Ingest multi-tenant invoices (Notas)
            ingest_notas(con)
            
            # Ingest globally shared full-load tables
            ingest_full_table(con, "tes", "/rest/CONSTES/tes")
            ingest_full_table(con, "produtos", "/rest/CONSPROD/produtos")
            ingest_full_table(con, "clientes", "/rest/CONSCLI/clientes")
            ingest_full_table(con, "vendedores", "/rest/CONSVEN/vendedores")
            
            con.close()
            logger.info("=== PROTHEUS SOURCE TO BRONZE INGESTION FINISHED SUCCESSFUL ===")
            break
        except Exception as e:
            logger.error(f"Ingestion Pipeline Attempt {attempt}/{max_retries} Failed: {e}", exc_info=True)
            if con:
                try:
                    con.close()
                except Exception:
                    pass
            if attempt == max_retries:
                logger.error("Maximum retries reached. Pipeline failed permanently.")
                raise
            logger.info(f"Retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)

if __name__ == "__main__":
    main()
