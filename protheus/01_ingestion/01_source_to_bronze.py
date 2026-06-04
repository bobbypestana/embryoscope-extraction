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
        
    max_attempts = 5
    timeout = 90  # Increased timeout for offset pagination queries
    
    for attempt in range(1, max_attempts + 1):
        try:
            r = requests.get(url, params=params, headers=headers, auth=AUTH, timeout=timeout)
            if r.status_code == 200:
                r.encoding = 'utf-8'
                return r.json()
            elif r.status_code in [400, 404]:
                logger.error(f"API Error {r.status_code} for {path} (Tenant: {headers.get('TenantId', 'None')}): {r.text[:500]}")
                return None
            else:
                logger.warning(f"Attempt {attempt}/{max_attempts} failed with status code {r.status_code} for {path} (Tenant: {headers.get('TenantId', 'None')}). Retrying...")
        except Exception as e:
            logger.warning(f"Attempt {attempt}/{max_attempts} failed with exception for {path} (Tenant: {headers.get('TenantId', 'None')}): {e}")
            
        if attempt < max_attempts:
            sleep_time = attempt * 5
            time.sleep(sleep_time)
            
    raise RuntimeError(f"Failed to fetch {path} (Tenant: {headers.get('TenantId', 'None')}) after {max_attempts} attempts.")

def compute_row_hash(row_dict):
    # Exclude metadata keys from hash computation
    clean_dict = {k: v for k, v in row_dict.items() if k not in ['hash', 'extraction_timestamp']}
    # Normalize values to string, handling None
    row_str = "|".join(f"{k}:{str(v).strip() if v is not None else ''}" for k, v in sorted(clean_dict.items()))
    return hashlib.md5(row_str.encode('utf-8')).hexdigest()

def get_existing_hashes(table_name):
    con = None
    try:
        con = duckdb.connect(DUCKDB_PATH)
        # Check if table exists
        exists = con.execute(f"""
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_schema = 'bronze' AND table_name = '{table_name}'
        """).fetchone()[0]
        if exists:
            hashes = con.execute(f"SELECT hash FROM bronze.{table_name}").fetchall()
            con.close()
            return {h[0] for h in hashes if h[0]}
        con.close()
        return set()
    except Exception as e:
        logger.warning(f"Could not fetch existing hashes for bronze.{table_name}: {e}")
        if con:
            try:
                con.close()
            except Exception:
                pass
        return set()

def write_to_bronze(table_name, rows):
    if not rows:
        logger.info(f"No new rows to write to bronze.{table_name}")
        return
        
    df = pd.DataFrame(rows)
    
    def clean_val(x):
        if x is None:
            return None
        if isinstance(x, (list, dict, tuple)):
            return str(x)
        try:
            if pd.isna(x):
                return None
        except Exception:
            pass
        s = str(x).strip()
        if s.upper() == 'NONE' or s == '':
            return None
        return s

    # Ensure all columns are string/NULL
    for col in df.columns:
        df[col] = df[col].apply(clean_val)
        
    # Count unique parent records for nested-item tables
    if table_name == "notas":
        unique_parents = len({(r.get("company_id"), r.get("F2_FILIAL"), r.get("F2_DOC"), r.get("F2_SERIE")) for r in rows if r.get("F2_DOC")})
        parent_label = "invoices"
    elif table_name == "pedidos":
        unique_parents = len({(r.get("company_id"), r.get("C5_FILIAL"), r.get("C5_NUM")) for r in rows if r.get("C5_NUM")})
        parent_label = "orders"
    elif table_name == "pedidos_venda":
        unique_parents = len({(r.get("company_id"), r.get("L1_FILIAL"), r.get("L1_NUM")) for r in rows if r.get("L1_NUM")})
        parent_label = "direct sales"
    else:
        unique_parents = len(rows)
        parent_label = "records"
        
    con = duckdb.connect(DUCKDB_PATH)
    # Check if table exists
    exists = con.execute(f"""
        SELECT COUNT(*) FROM information_schema.tables 
        WHERE table_schema = 'bronze' AND table_name = '{table_name}'
    """).fetchone()[0]
    
    con.register('df_temp', df)
    
    try:
        if not exists:
            logger.info(f"Creating table bronze.{table_name} with VARCHAR columns and inserting {unique_parents} unique {parent_label} ({len(df)} flat rows)")
            cols_def = ", ".join(f'"{c}" VARCHAR' for c in df.columns)
            con.execute(f"CREATE TABLE bronze.{table_name} ({cols_def})")
            cols_str = ", ".join(f'"{c}"' for c in df.columns)
            con.execute(f"INSERT INTO bronze.{table_name} ({cols_str}) SELECT {cols_str} FROM df_temp")
        else:
            # Check and handle schema evolution
            existing_cols = {row[0] for row in con.execute(f"DESCRIBE bronze.{table_name}").fetchall()}
            new_cols = set(df.columns) - existing_cols
            for col in new_cols:
                logger.info(f"Schema evolution: Adding column '{col}' to bronze.{table_name}")
                con.execute(f"ALTER TABLE bronze.{table_name} ADD COLUMN \"{col}\" VARCHAR")
                
            # Select and insert aligned columns
            cols_str = ", ".join(f'"{c}"' for c in df.columns)
            logger.info(f"Inserting {unique_parents} unique {parent_label} ({len(df)} flat rows) to bronze.{table_name}")
            con.execute(f"INSERT INTO bronze.{table_name} ({cols_str}) SELECT {cols_str} FROM df_temp")
            
        con.unregister('df_temp')
        con.close()
        logger.info(f"Successfully wrote {unique_parents} unique {parent_label} to bronze.{table_name}")
        
    except Exception as bulk_err:
        try:
            con.unregister('df_temp')
        except Exception:
            pass
        try:
            con.close()
        except Exception:
            pass
        logger.error(f"Bulk insert failed for bronze.{table_name}: {bulk_err}")
        raise bulk_err

def generate_date_chunks(start_dt, end_dt, force_backfill):
    chunks = []
    import calendar
    current_start = start_dt
    while current_start <= end_dt:
        last_day = calendar.monthrange(current_start.year, current_start.month)[1]
        current_end = datetime(current_start.year, current_start.month, last_day)
        if current_end > end_dt:
            current_end = end_dt
        chunks.append((current_start, current_end))
        current_start = current_end + timedelta(days=1)
    return chunks

def fetch_invoice_range(tenant_id, company_id, start_date_str, end_date_str, existing_hashes):
    new_rows = []
    invoices_read = 0
    flat_rows_read = 0
    seen_hashes = set()
    page = 1
    page_size = 500
    api_total = 0
    
    while True:
        params = {
            "dataIni": start_date_str,
            "dataFim": end_date_str,
            "nPage": page,
            "nPageSize": page_size
        }
        res = make_request("/rest/CONSNOTA/notas", params=params, tenant_id=tenant_id)
        if not res or "data" not in res or not res["data"]:
            break
            
        if page == 1:
            api_total = res.get("total", 0)
            
        extraction_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        invoices_read += len(res["data"])
        for invoice in res["data"]:
            items = invoice.get("ITENS", [])
            header = {k: v for k, v in invoice.items() if k != "ITENS"}
            
            if not items:
                flat_rows_read += 1
                flat_row = header.copy()
                flat_row["company_id"] = company_id
                flat_row["extraction_timestamp"] = extraction_ts
                flat_row["hash"] = compute_row_hash(flat_row)
                if flat_row["hash"] not in existing_hashes and flat_row["hash"] not in seen_hashes:
                    new_rows.append(flat_row)
                    seen_hashes.add(flat_row["hash"])
            else:
                flat_rows_read += len(items)
                for item in items:
                    flat_row = header.copy()
                    flat_row.update(item)
                    flat_row["company_id"] = company_id
                    flat_row["extraction_timestamp"] = extraction_ts
                    flat_row["hash"] = compute_row_hash(flat_row)
                    if flat_row["hash"] not in existing_hashes and flat_row["hash"] not in seen_hashes:
                        new_rows.append(flat_row)
                        seen_hashes.add(flat_row["hash"])
                        
        if not res.get("hasNext", False):
            break
        page += 1
        
    logger.info(f"Notas Auditing for {start_date_str}-{end_date_str} (Tenant: {tenant_id}): API total={api_total}, Fetched parent records={invoices_read}")
    if api_total != invoices_read:
        logger.warning(f"Notas count mismatch for {start_date_str}-{end_date_str} (Tenant: {tenant_id}): API total={api_total}, fetched={invoices_read}")
        
    return new_rows, invoices_read, flat_rows_read

def fetch_sales_orders_range(tenant_id, company_id, start_date_str, end_date_str, existing_hashes):
    new_rows = []
    orders_read = 0
    flat_rows_read = 0
    seen_hashes = set()
    page = 1
    page_size = 500
    api_total = 0
    
    while True:
        params = {
            "dataIni": start_date_str,
            "dataFim": end_date_str,
            "nPage": page,
            "nPageSize": page_size
        }
        res = make_request("/rest/CONSPED/pedidos", params=params, tenant_id=tenant_id)
        if not res or "data" not in res or not res["data"]:
            break
            
        if page == 1:
            api_total = res.get("total", 0)
            
        extraction_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        orders_read += len(res["data"])
        for order in res["data"]:
            items = order.get("ITENS", [])
            header = {k: v for k, v in order.items() if k != "ITENS"}
            
            if not items:
                flat_rows_read += 1
                flat_row = header.copy()
                flat_row["company_id"] = company_id
                flat_row["extraction_timestamp"] = extraction_ts
                flat_row["hash"] = compute_row_hash(flat_row)
                if flat_row["hash"] not in existing_hashes and flat_row["hash"] not in seen_hashes:
                    new_rows.append(flat_row)
                    seen_hashes.add(flat_row["hash"])
            else:
                flat_rows_read += len(items)
                for item in items:
                    flat_row = header.copy()
                    flat_row.update(item)
                    flat_row["company_id"] = company_id
                    flat_row["extraction_timestamp"] = extraction_ts
                    flat_row["hash"] = compute_row_hash(flat_row)
                    if flat_row["hash"] not in existing_hashes and flat_row["hash"] not in seen_hashes:
                        new_rows.append(flat_row)
                        seen_hashes.add(flat_row["hash"])
                        
        if not res.get("hasNext", False):
            break
        page += 1
        
    logger.info(f"Pedidos Auditing for {start_date_str}-{end_date_str} (Tenant: {tenant_id}): API total={api_total}, Fetched parent records={orders_read}")
    if api_total != orders_read:
        logger.warning(f"Pedidos count mismatch for {start_date_str}-{end_date_str} (Tenant: {tenant_id}): API total={api_total}, fetched={orders_read}")
        
    return new_rows, orders_read, flat_rows_read

def fetch_direct_sales_range(tenant_id, company_id, start_date_str, end_date_str, existing_hashes):
    new_rows = []
    sales_read = 0
    flat_rows_read = 0
    seen_hashes = set()
    page = 1
    page_size = 500
    api_total = 0
    
    while True:
        params = {
            "dataIni": start_date_str,
            "dataFim": end_date_str,
            "nPage": page,
            "nPageSize": page_size
        }
        res = make_request("/rest/CONSPEVD/pedidos", params=params, tenant_id=tenant_id)
        if not res or "data" not in res or not res["data"]:
            break
            
        if page == 1:
            api_total = res.get("total", 0)
            
        extraction_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        sales_read += len(res["data"])
        for sale in res["data"]:
            items = sale.get("ITENS", [])
            header = {k: v for k, v in sale.items() if k != "ITENS"}
            
            if not items:
                flat_rows_read += 1
                flat_row = header.copy()
                flat_row["company_id"] = company_id
                flat_row["extraction_timestamp"] = extraction_ts
                flat_row["hash"] = compute_row_hash(flat_row)
                if flat_row["hash"] not in existing_hashes and flat_row["hash"] not in seen_hashes:
                    new_rows.append(flat_row)
                    seen_hashes.add(flat_row["hash"])
            else:
                flat_rows_read += len(items)
                for item in items:
                    flat_row = header.copy()
                    flat_row.update(item)
                    flat_row["company_id"] = company_id
                    flat_row["extraction_timestamp"] = extraction_ts
                    flat_row["hash"] = compute_row_hash(flat_row)
                    if flat_row["hash"] not in existing_hashes and flat_row["hash"] not in seen_hashes:
                        new_rows.append(flat_row)
                        seen_hashes.add(flat_row["hash"])
                        
        if not res.get("hasNext", False):
            break
        page += 1
        
    logger.info(f"Pedidos Venda Auditing for {start_date_str}-{end_date_str} (Tenant: {tenant_id}): API total={api_total}, Fetched parent records={sales_read}")
    if api_total != sales_read:
        logger.warning(f"Pedidos Venda count mismatch for {start_date_str}-{end_date_str} (Tenant: {tenant_id}): API total={api_total}, fetched={sales_read}")
        
    return new_rows, sales_read, flat_rows_read

def ingest_notas(force_backfill=False):
    table_name = "notas"
    logger.info("Starting ingestion of 'notas' (invoices)...")
    
    today = datetime.now()
    if force_backfill:
        start_dt = datetime.strptime(BACKFILL_START, "%Y%m%d")
        logger.info(f"Force backfill enabled for Notas. Bypassing incremental check.")
    else:
        start_dt = today - timedelta(days=60)
    end_dt = today
    
    # Cache existing hashes to avoid duplicates
    existing_hashes = get_existing_hashes(table_name)
    
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
            
        logger.info(f"Ingestion range for Tenant {tenant_id}: {start_dt.strftime('%Y%m%d')} to {end_dt.strftime('%Y%m%d')}")
        
        # Generate chunks
        chunks = generate_date_chunks(start_dt, end_dt, force_backfill)
        
        for current_start, current_end in chunks:
            data_ini_str = current_start.strftime("%Y%m%d")
            data_fim_str = current_end.strftime("%Y%m%d")
            
            logger.info(f"Processing chunk {data_ini_str}-{data_fim_str} for Tenant {tenant_id}...")
            new_rows, invoices_read, flat_rows_read = fetch_invoice_range(
                tenant_id, company_id, data_ini_str, data_fim_str, existing_hashes
            )
            
            logger.info(f"Chunk {data_ini_str}-{data_fim_str} complete. Invoices read: {invoices_read}, Flat rows read: {flat_rows_read}. Unique new written: {len(new_rows)}")
            
            if new_rows:
                for r in new_rows:
                    existing_hashes.add(r["hash"])
                write_to_bronze(table_name, new_rows)

def ingest_pedidos(force_backfill=False):
    table_name = "pedidos"
    logger.info("Starting ingestion of 'pedidos' (Sales Orders)...")
    
    today = datetime.now()
    if force_backfill:
        start_dt = datetime.strptime(BACKFILL_START, "%Y%m%d")
        logger.info(f"Force backfill enabled for Pedidos. Bypassing incremental check.")
    else:
        start_dt = today - timedelta(days=60)
    end_dt = today
    
    existing_hashes = get_existing_hashes(table_name)
    
    for tenant_id in ACCESSIBLE_TENANTS:
        logger.info(f"=== Ingesting Pedidos for Tenant: {tenant_id} ===")
        company_id = tenant_id.split(',')[0]
        
        # Quick check if tenant has any orders
        check_params = {
            "dataIni": BACKFILL_START,
            "dataFim": today.strftime("%Y%m%d"),
            "nPage": 1,
            "nPageSize": 1
        }
        check_res = make_request("/rest/CONSPED/pedidos", params=check_params, tenant_id=tenant_id)
        if not check_res or "data" not in check_res or not check_res["data"]:
            logger.info(f"Tenant {tenant_id} has no orders in the backfill range. Skipping.")
            continue
            
        logger.info(f"Ingestion range for Tenant {tenant_id}: {start_dt.strftime('%Y%m%d')} to {end_dt.strftime('%Y%m%d')}")
        
        chunks = generate_date_chunks(start_dt, end_dt, force_backfill)
        
        for current_start, current_end in chunks:
            data_ini_str = current_start.strftime("%Y%m%d")
            data_fim_str = current_end.strftime("%Y%m%d")
            
            logger.info(f"Processing chunk {data_ini_str}-{data_fim_str} for Tenant {tenant_id}...")
            new_rows, orders_read, flat_rows_read = fetch_sales_orders_range(
                tenant_id, company_id, data_ini_str, data_fim_str, existing_hashes
            )
            
            logger.info(f"Chunk {data_ini_str}-{data_fim_str} complete. Orders read: {orders_read}, Flat rows read: {flat_rows_read}. Unique new written: {len(new_rows)}")
            
            if new_rows:
                for r in new_rows:
                    existing_hashes.add(r["hash"])
                write_to_bronze(table_name, new_rows)

def ingest_pedidos_venda(force_backfill=False):
    table_name = "pedidos_venda"
    logger.info("Starting ingestion of 'pedidos_venda' (Direct Sales)...")
    
    today = datetime.now()
    if force_backfill:
        start_dt = datetime.strptime(BACKFILL_START, "%Y%m%d")
        logger.info(f"Force backfill enabled for Pedidos Venda. Bypassing incremental check.")
    else:
        start_dt = today - timedelta(days=60)
    end_dt = today
    
    existing_hashes = get_existing_hashes(table_name)
    
    for tenant_id in ACCESSIBLE_TENANTS:
        logger.info(f"=== Ingesting Pedidos Venda for Tenant: {tenant_id} ===")
        company_id = tenant_id.split(',')[0]
        
        # Quick check if tenant has any direct sales
        check_params = {
            "dataIni": BACKFILL_START,
            "dataFim": today.strftime("%Y%m%d"),
            "nPage": 1,
            "nPageSize": 1
        }
        check_res = make_request("/rest/CONSPEVD/pedidos", params=check_params, tenant_id=tenant_id)
        if not check_res or "data" not in check_res or not check_res["data"]:
            logger.info(f"Tenant {tenant_id} has no direct sales in the backfill range. Skipping.")
            continue
            
        logger.info(f"Ingestion range for Tenant {tenant_id}: {start_dt.strftime('%Y%m%d')} to {end_dt.strftime('%Y%m%d')}")
        
        chunks = generate_date_chunks(start_dt, end_dt, force_backfill)
        
        for current_start, current_end in chunks:
            data_ini_str = current_start.strftime("%Y%m%d")
            data_fim_str = current_end.strftime("%Y%m%d")
            
            logger.info(f"Processing chunk {data_ini_str}-{data_fim_str} for Tenant {tenant_id}...")
            new_rows, sales_read, flat_rows_read = fetch_direct_sales_range(
                tenant_id, company_id, data_ini_str, data_fim_str, existing_hashes
            )
            
            logger.info(f"Chunk {data_ini_str}-{data_fim_str} complete. Sales read: {sales_read}, Flat rows read: {flat_rows_read}. Unique new written: {len(new_rows)}")
            
            if new_rows:
                for r in new_rows:
                    existing_hashes.add(r["hash"])
                write_to_bronze(table_name, new_rows)

def ingest_full_table(name, path, max_sweeps=10):
    """
    Full-load ingestion with convergence-based sweeping.
    """
    logger.info(
        f"Starting ingestion of '{name}' "
        f"(sequential full load, up to {max_sweeps} convergence sweeps)..."
    )
    existing_hashes = get_existing_hashes(name)
    page_size = 500

    for sweep in range(1, max_sweeps + 1):
        logger.info(f"--- Sweep {sweep} for {name} ---")
        sweep_new_rows = []
        extraction_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        page = 1
        total_read = 0

        while True:
            logger.info(f"Fetching page {page} of {name} (Sweep {sweep})...")
            res = make_request(path, params={"nPage": page, "nPageSize": page_size}, tenant_id=None)
            if not res or "data" not in res or not res["data"]:
                break

            total_read += len(res["data"])
            for item in res["data"]:
                row_dict = item.copy()
                row_dict["extraction_timestamp"] = extraction_ts
                row_dict["hash"] = compute_row_hash(row_dict)
                if row_dict["hash"] not in existing_hashes:
                    sweep_new_rows.append(row_dict)
                    existing_hashes.add(row_dict["hash"])

            if not res.get("hasNext", False):
                break
            page += 1

        logger.info(f"Sweep {sweep} complete. Total records read: {total_read:,}. New unique rows found: {len(sweep_new_rows):,}")

        if sweep_new_rows:
            logger.info(f"Writing {len(sweep_new_rows):,} new rows to bronze.{name}")
            write_to_bronze(name, sweep_new_rows)
        else:
            logger.info(f"Sweep {sweep} complete. Total records read: {total_read:,}. 0 new rows found. Converged — stopping sweeps for {name}.")
            break

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Protheus API Ingestion Script")
    parser.add_argument("--force-backfill", action="store_true", help="Force a full backfill for all endpoints, bypassing incremental checks")
    args = parser.parse_args()

    logger.info("=== PROTHEUS SOURCE TO BRONZE INGESTION STARTED ===")
    logger.info(f"Target Database: {DUCKDB_PATH}")
    
    max_retries = 10
    retry_delay = 15
    
    # Initialize Schema if not exists
    try:
        con = duckdb.connect(DUCKDB_PATH)
        con.execute("CREATE SCHEMA IF NOT EXISTS bronze")
        con.close()
    except Exception as e:
        logger.warning(f"Failed to create schema directly on target database: {e}")

    for attempt in range(1, max_retries + 1):
        try:
            # Ingest multi-tenant invoices (Notas)
            ingest_notas(force_backfill=args.force_backfill)
            
            # Ingest multi-tenant sales orders (Pedidos)
            ingest_pedidos(force_backfill=args.force_backfill)
            
            # Ingest multi-tenant direct sales (Pedidos Venda)
            ingest_pedidos_venda(force_backfill=args.force_backfill)
            
            # Ingest globally shared full-load tables
            ingest_full_table("tes", "/rest/CONSTES/tes", max_sweeps=1)
            ingest_full_table("produtos", "/rest/CONSPROD/produtos", max_sweeps=1)
            ingest_full_table("clientes", "/rest/CONSCLI/clientes", max_sweeps=1)
            ingest_full_table("vendedores", "/rest/CONSVEN/vendedores", max_sweeps=1)
            
            logger.info("=== PROTHEUS SOURCE TO BRONZE INGESTION FINISHED SUCCESSFUL ===")
            break
        except Exception as e:
            logger.error(f"Ingestion Pipeline Attempt {attempt}/{max_retries} Failed: {e}", exc_info=True)
            if attempt == max_retries:
                logger.error("Maximum retries reached. Pipeline failed permanently.")
                raise
            logger.info(f"Retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)

if __name__ == "__main__":
    main()
