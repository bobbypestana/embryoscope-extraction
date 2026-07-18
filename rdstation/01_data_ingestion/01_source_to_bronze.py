#!/usr/bin/env python3
"""
RD Station CRM API to DuckDB Bronze Ingestion Script
Extracts data from Deals, Contacts, Pipelines, Stages, Users, and Sources,
converts all columns to VARCHAR, and performs hash-based incremental appends to the database.
Handles rate limiting (HTTP 429) and token rolling refreshes.
"""

import os
import yaml
import json
import logging
import requests
import hashlib
import pandas as pd
import duckdb
import time
import random
import argparse
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
TOKENS_PATH = os.path.join(SCRIPT_DIR, config.get('tokens_path', 'tokens.json'))
BACKFILL_START = config.get('backfill_start_date', '2020-01-01')

class RDStationCRMClient:
    def __init__(self, tokens_filepath):
        self.tokens_filepath = tokens_filepath
        self.load_tokens()
        self.base_url = "https://api.rd.services/crm/v2"
        self.session = requests.Session()

    def load_tokens(self):
        """Loads client credentials and tokens from the JSON storage."""
        if not os.path.exists(self.tokens_filepath):
            raise FileNotFoundError(
                f"Token file not found at '{self.tokens_filepath}'. "
                "Ensure auth bootstrap has run."
            )
            
        with open(self.tokens_filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
            
        self.client_id = data.get("client_id")
        self.client_secret = data.get("client_secret")
        self.access_token = data.get("access_token")
        self.refresh_token = data.get("refresh_token")

    def save_tokens(self):
        """Saves current credentials and tokens back to the JSON storage."""
        with open(self.tokens_filepath, "w", encoding="utf-8") as f:
            json.dump({
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "access_token": self.access_token,
                "refresh_token": self.refresh_token
            }, f, indent=4)

    def refresh_access_token(self):
        """Refreshes the access token using the refresh token."""
        logger.info("[*] Refreshing CRM v2 access token in the background...")
        url = "https://api.rd.services/oauth2/token"
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json"
        }
        payload = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "refresh_token": self.refresh_token,
            "grant_type": "refresh_token"
        }
        
        response = self.session.post(url, data=payload, headers=headers)
        if response.status_code == 200:
            data = response.json()
            self.access_token = data.get("access_token")
            if "refresh_token" in data:
                self.refresh_token = data.get("refresh_token")
            self.save_tokens()
            logger.info("[+] CRM v2 access token successfully refreshed and saved.")
        else:
            logger.error(f"[!] Failed to refresh CRM token: {response.text}")
            response.raise_for_status()

    def make_request(self, method, endpoint, **kwargs):
        """Makes API request with automatic 401 retry and 429 rate limit backoff."""
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        
        if "headers" not in kwargs:
            kwargs["headers"] = {}
        kwargs["headers"]["Authorization"] = f"Bearer {self.access_token}"
        kwargs["headers"]["Accept"] = "application/json"
        
        max_attempts = 6
        timeout = 60
        
        for attempt in range(1, max_attempts + 1):
            try:
                r = self.session.request(method, url, timeout=timeout, **kwargs)
                
                # Handle Rate Limit (Too Many Requests)
                if r.status_code == 429:
                    retry_after = r.headers.get("Retry-After")
                    sleep_time = int(retry_after) if retry_after and retry_after.isdigit() else (5 * attempt + random.uniform(1, 3))
                    logger.warning(f"[!] Rate limit (429) hit. Sleeping for {sleep_time} seconds before attempt {attempt}/{max_attempts}...")
                    time.sleep(sleep_time)
                    continue
                    
                # Handle Expired Token (401 Unauthorized)
                if r.status_code == 401:
                    logger.warning("[!] Request returned 401. Refreshing token and retrying...")
                    self.refresh_access_token()
                    kwargs["headers"]["Authorization"] = f"Bearer {self.access_token}"
                    continue
                
                return r
            except Exception as e:
                logger.warning(f"Attempt {attempt}/{max_attempts} failed with exception: {e}")
                if attempt == max_attempts:
                    raise
                sleep_time = 4 * attempt + random.uniform(1, 3)
                time.sleep(sleep_time)
                
        raise RuntimeError(f"Request to {endpoint} failed after {max_attempts} attempts.")

# Primary keys for deduplication and deletion auditing
TABLE_PKS = {
    "deals": ["id"],
    "contacts": ["id"],
    "pipelines": ["id"],
    "stages": ["pipeline_id", "id"],
    "users": ["id"],
    "sources": ["id"]
}

def get_existing_hashes(table_name):
    try:
        with duckdb.connect(DUCKDB_PATH, read_only=True) as con:
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

def get_max_updated_at(table_name):
    try:
        with duckdb.connect(DUCKDB_PATH, read_only=True) as con:
            exists = con.execute(f"""
                SELECT COUNT(*) FROM information_schema.tables 
                WHERE table_schema = 'bronze' AND table_name = '{table_name}'
            """).fetchone()[0]
            if not exists:
                return None
            res = con.execute(f"SELECT MAX(updated_at) FROM bronze.{table_name} WHERE COALESCE(is_deleted, 'FALSE') = 'FALSE'").fetchone()
            if res and res[0]:
                return str(res[0]).strip()
    except Exception as e:
        logger.warning(f"Could not fetch max updated_at for bronze.{table_name}: {e}")
    return None

def get_existing_pks(table_name):
    pks = TABLE_PKS.get(table_name, [])
    if not pks:
        return set()
    try:
        with duckdb.connect(DUCKDB_PATH, read_only=True) as con:
            exists = con.execute(f"""
                SELECT COUNT(*) FROM information_schema.tables 
                WHERE table_schema = 'bronze' AND table_name = '{table_name}'
            """).fetchone()[0]
            if not exists:
                return set()
            pk_cols_str = ", ".join(f'COALESCE("{p}", \'\')' for p in pks)
            query = f'SELECT DISTINCT {pk_cols_str} FROM bronze.{table_name}'
            rows = con.execute(query).fetchall()
        if len(pks) == 1:
            return {str(r[0]).strip() for r in rows if r[0] is not None}
        else:
            return {tuple(str(val).strip() for val in r) for r in rows}
    except Exception as e:
        logger.warning(f"Could not fetch existing PKs for bronze.{table_name}: {e}")
        return set()

def flag_deleted_in_bronze(table_name, deleted_pks):
    if not deleted_pks:
        return
    pks = TABLE_PKS.get(table_name, [])
    if not pks:
        return
    try:
        with duckdb.connect(DUCKDB_PATH) as con:
            exists = con.execute(f"""
                SELECT COUNT(*) FROM information_schema.tables 
                WHERE table_schema = 'bronze' AND table_name = '{table_name}'
            """).fetchone()[0]
            if not exists:
                return
            existing_cols = {row[0] for row in con.execute(f"DESCRIBE bronze.{table_name}").fetchall()}
            if "is_deleted" not in existing_cols:
                logger.info(f"Adding is_deleted column to bronze.{table_name}")
                con.execute(f"ALTER TABLE bronze.{table_name} ADD COLUMN is_deleted VARCHAR")
                con.execute(f"UPDATE bronze.{table_name} SET is_deleted = 'FALSE'")

            logger.info(f"Flagging {len(deleted_pks)} deleted records in bronze.{table_name}...")
            
            pk_cols_str = ", ".join(f'"{p}"' for p in pks)
            list_pks = list(deleted_pks)
            chunk_size = 500
            for i in range(0, len(list_pks), chunk_size):
                chunk = list_pks[i:i+chunk_size]
                tuple_list_str = []
                for item in chunk:
                    if len(pks) == 1:
                        escaped_val = str(item).replace("'", "''")
                        tuple_list_str.append(f"('{escaped_val}')")
                    else:
                        escaped_vals = ["'" + str(val).replace("'", "''") + "'" for val in item]
                        tuple_list_str.append(f"({', '.join(escaped_vals)})")
                
                tuples_in_str = ", ".join(tuple_list_str)
                update_query = f"""
                    UPDATE bronze.{table_name}
                    SET is_deleted = 'TRUE'
                    WHERE ({pk_cols_str}) IN ({tuples_in_str})
                """
                con.execute(update_query)
            logger.info(f"Successfully flagged {len(deleted_pks)} deleted records in bronze.{table_name}")
    except Exception as e:
        logger.error(f"Failed to flag deleted records in bronze.{table_name}: {e}")

def compute_row_hash(row_dict):
    clean_dict = {k: v for k, v in row_dict.items() if k not in ['hash', 'extraction_timestamp']}
    row_str = "|".join(f"{k}:{str(v).strip() if v is not None else ''}" for k, v in sorted(clean_dict.items()))
    return hashlib.md5(row_str.encode('utf-8')).hexdigest()

def write_to_bronze(table_name, rows):
    if not rows:
        logger.info(f"No new rows to write to bronze.{table_name}")
        return
        
    df = pd.DataFrame(rows)
    
    def clean_val(x):
        if x is None:
            return None
        if isinstance(x, (list, dict, tuple)):
            return json.dumps(x)
        try:
            if pd.isna(x):
                return None
        except Exception:
            pass
        s = str(x).strip()
        if s.upper() == 'NONE' or s == '':
            return None
        return s

    for col in df.columns:
        df[col] = df[col].apply(clean_val)
        
    try:
        with duckdb.connect(DUCKDB_PATH) as con:
            exists = con.execute(f"""
                SELECT COUNT(*) FROM information_schema.tables 
                WHERE table_schema = 'bronze' AND table_name = '{table_name}'
            """).fetchone()[0]
            
            con.register('df_temp', df)
            try:
                if not exists:
                    logger.info(f"Creating table bronze.{table_name} with VARCHAR columns and inserting {len(df)} rows")
                    cols_def = ", ".join(f'"{c}" VARCHAR' for c in df.columns)
                    con.execute(f"CREATE TABLE bronze.{table_name} ({cols_def})")
                    cols_str = ", ".join(f'"{c}"' for c in df.columns)
                    con.execute(f"INSERT INTO bronze.{table_name} ({cols_str}) SELECT {cols_str} FROM df_temp")
                else:
                    existing_cols = {row[0] for row in con.execute(f"DESCRIBE bronze.{table_name}").fetchall()}
                    new_cols = set(df.columns) - existing_cols
                    for col in new_cols:
                        logger.info(f"Schema evolution: Adding column '{col}' to bronze.{table_name}")
                        con.execute(f"ALTER TABLE bronze.{table_name} ADD COLUMN \"{col}\" VARCHAR")
                        
                    cols_str = ", ".join(f'"{c}"' for c in df.columns)
                    logger.info(f"Inserting {len(df)} rows to bronze.{table_name}")
                    con.execute(f"INSERT INTO bronze.{table_name} ({cols_str}) SELECT {cols_str} FROM df_temp")
            finally:
                try:
                    con.unregister('df_temp')
                except Exception:
                    pass
            logger.info(f"Successfully wrote {len(rows)} rows to bronze.{table_name}")
    except Exception as bulk_err:
        logger.error(f"Bulk insert failed for bronze.{table_name}: {bulk_err}")
        raise bulk_err

def format_datetime_for_api(dt_str):
    if not dt_str:
        return None
    normalized = dt_str.replace(" ", "T")
    if "." in normalized:
        normalized = normalized.split(".")[0]
    if not normalized.endswith("Z") and not ("+" in normalized or "-" in normalized.split("T")[-1]):
        normalized += "Z"
    return normalized

def fetch_and_ingest_range(client, table_name, endpoint, start_str, end_str, existing_hashes, fetched_pks):
    logger.info(f"Fetching {table_name} range: {start_str} to {end_str}")
    page = 1
    page_size = 100
    pks = TABLE_PKS.get(table_name, [])
    new_rows = []
    resp = None
    
    while True:
        # Check if page is too high
        if page > 100:
            logger.warning(f"Exceeded 100 pages (10,000 records) for range {start_str} to {end_str}. Splitting range...")
            break
            
        params = {
            "page[number]": page,
            "page[size]": page_size,
            "filter": f"updated_at:>={start_str} and updated_at:<={end_str}"
        }
        
        resp = client.make_request("GET", endpoint, params=params)
        
        if resp.status_code == 400:
            err_text = resp.text
            if "10,000 records" in err_text:
                logger.warning(f"API returned 400 (10,000 limit) for range {start_str} to {end_str}. Splitting range...")
                break
            else:
                logger.error(f"[!] Error fetching {table_name} page {page}: {resp.status_code} - {err_text}")
                resp.raise_for_status()
        elif resp.status_code != 200:
            logger.error(f"[!] Error fetching {table_name} page {page}: {resp.status_code} - {resp.text}")
            resp.raise_for_status()
            
        data_json = resp.json()
        data_list = data_json.get("data", [])
        
        if not data_list:
            break
            
        extraction_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        for item in data_list:
            row = item.copy()
            row["extraction_timestamp"] = extraction_ts
            row["is_deleted"] = "FALSE"
            row["hash"] = compute_row_hash(row)
            
            if pks:
                if len(pks) == 1:
                    fetched_pks.add(str(row.get(pks[0], '')).strip())
                else:
                    fetched_pks.add(tuple(str(row.get(p, '')).strip() for p in pks))
                    
            if row["hash"] not in existing_hashes:
                new_rows.append(row)
                existing_hashes.add(row["hash"])
                
        # Write batches of 500 records to prevent memory build-up
        if len(new_rows) >= 500:
            write_to_bronze(table_name, new_rows)
            new_rows = []
            
        if len(data_list) < page_size:
            break
            
        page += 1

    if page > 100 or (resp is not None and resp.status_code == 400 and "10,000 records" in resp.text):
        start_dt = pd.to_datetime(start_str)
        end_dt = pd.to_datetime(end_str)
        if (end_dt - start_dt).total_seconds() < 60:
            logger.error(f"Cannot split range further (less than 1 minute). Writing what we have...")
            if new_rows:
                write_to_bronze(table_name, new_rows)
            return len(new_rows)
            
        mid_dt = start_dt + (end_dt - start_dt) / 2
        start_fmt = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        end_fmt = end_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        mid_fmt = mid_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        
        logger.info(f"Recursive split: Left: {start_fmt} to {mid_fmt} | Right: {mid_fmt} to {end_fmt}")
        left_count = fetch_and_ingest_range(client, table_name, endpoint, start_fmt, mid_fmt, existing_hashes, fetched_pks)
        right_count = fetch_and_ingest_range(client, table_name, endpoint, mid_fmt, end_fmt, existing_hashes, fetched_pks)
        return left_count + right_count
    else:
        if new_rows:
            write_to_bronze(table_name, new_rows)
        return len(new_rows)

def generate_date_chunks(start_str, end_str):
    start_dt = pd.to_datetime(start_str)
    end_dt = pd.to_datetime(end_str)
    chunks = []
    curr = start_dt
    delta = pd.Timedelta(days=30)
    
    while curr < end_dt:
        next_curr = curr + delta
        if next_curr > end_dt:
            next_curr = end_dt
        chunks.append((curr.strftime("%Y-%m-%dT%H:%M:%SZ"), next_curr.strftime("%Y-%m-%dT%H:%M:%SZ")))
        curr = next_curr
    return chunks

def ingest_incremental_table(client, table_name, endpoint, force_backfill=False):
    logger.info(f"=== Starting incremental ingestion of '{table_name}' using date ranges ===")
    existing_hashes = get_existing_hashes(table_name)
    existing_pks = get_existing_pks(table_name)
    fetched_pks = set()
    
    # Calculate global incremental start date
    if force_backfill:
        start_date = BACKFILL_START + "T00:00:00Z"
        logger.info(f"Force backfill enabled for {table_name}. Querying since {start_date}")
    else:
        max_updated = get_max_updated_at(table_name)
        if max_updated:
            start_date = format_datetime_for_api(max_updated)
            logger.info(f"Incremental mode for {table_name}. Querying since max updated_at: {start_date}")
        else:
            start_date = BACKFILL_START + "T00:00:00Z"
            logger.info(f"No previous data found for {table_name}. Starting backfill from {start_date}")
            
    # Set end_date as current time UTC
    end_date = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    
    chunks = generate_date_chunks(start_date, end_date)
    logger.info(f"Generated {len(chunks)} date range chunks to query for {table_name}")
    
    total_written = 0
    for idx, (chunk_start, chunk_end) in enumerate(chunks):
        logger.info(f"[{idx+1}/{len(chunks)}] Processing range {chunk_start} to {chunk_end}...")
        count = fetch_and_ingest_range(client, table_name, endpoint, chunk_start, chunk_end, existing_hashes, fetched_pks)
        total_written += count
        
    logger.info(f"Finished ingestion of '{table_name}'. Ingested {total_written} new unique records.")
    
    # Perform deletion audit
    if force_backfill and existing_pks:
        deleted_pks = existing_pks - fetched_pks
        if deleted_pks:
            logger.warning(f"Auditing '{table_name}': {len(deleted_pks)} entries missing from API. Flagging as deleted.")
            flag_deleted_in_bronze(table_name, deleted_pks)
        else:
            logger.info(f"Auditing '{table_name}': 0 entries deleted.")

def ingest_full_table(client, table_name, endpoint):
    logger.info(f"Starting full load of '{table_name}'...")
    existing_hashes = get_existing_hashes(table_name)
    new_rows = []
    
    page = 1
    page_size = 100
    extraction_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    while True:
        params = {
            "page[number]": page,
            "page[size]": page_size
        }
        
        logger.info(f"Fetching page {page} of {table_name}...")
        resp = client.make_request("GET", endpoint, params=params)
        
        if resp.status_code != 200:
            logger.error(f"[!] Error fetching {table_name} page {page}: {resp.status_code} - {resp.text}")
            resp.raise_for_status()
            
        data_json = resp.json()
        data_list = data_json.get("data", [])
        
        if not data_list:
            break
            
        for item in data_list:
            row = item.copy()
            row["extraction_timestamp"] = extraction_ts
            row["is_deleted"] = "FALSE"
            row["hash"] = compute_row_hash(row)
            
            if row["hash"] not in existing_hashes:
                new_rows.append(row)
                existing_hashes.add(row["hash"])
                
        if len(data_list) < page_size:
            break
            
        page += 1
        
    logger.info(f"Full load complete for {table_name}. Found {len(new_rows)} new unique records.")
    if new_rows:
        write_to_bronze(table_name, new_rows)

def ingest_stages(client):
    logger.info("Ingesting 'stages' per pipeline...")
    # Read pipelines from bronze to get IDs (or list first)
    pipelines_list = []
    
    # Fetch current active pipelines from API to loop through
    page = 1
    page_size = 100
    while True:
        resp = client.make_request("GET", "pipelines", params={"page[number]": page, "page[size]": page_size})
        if resp.status_code == 200:
            p_data = resp.json().get("data", [])
            if not p_data:
                break
            pipelines_list.extend(p_data)
            if len(p_data) < page_size:
                break
            page += 1
        else:
            logger.error(f"[!] Failed to fetch pipelines: {resp.status_code} - {resp.text}")
            break
            
    logger.info(f"Found {len(pipelines_list)} pipelines to fetch stages for.")
    existing_hashes = get_existing_hashes("stages")
    new_rows = []
    extraction_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    for pipe in pipelines_list:
        pipeline_id = pipe.get("id")
        pipeline_name = pipe.get("name")
        logger.info(f"Fetching stages for pipeline: {pipeline_name} (ID: {pipeline_id})...")
        
        # Endpoint: pipelines/{pipeline_id}/stages
        endpoint = f"pipelines/{pipeline_id}/stages"
        page_s = 1
        while True:
            resp = client.make_request("GET", endpoint, params={"page[number]": page_s, "page[size]": page_size})
            if resp.status_code != 200:
                logger.error(f"[!] Failed to fetch stages for {pipeline_id}: {resp.text}")
                break
                
            data_json = resp.json()
            data_list = data_json.get("data", [])
            
            if not data_list:
                break
                
            for item in data_list:
                row = item.copy()
                row["pipeline_id"] = pipeline_id
                row["extraction_timestamp"] = extraction_ts
                row["is_deleted"] = "FALSE"
                row["hash"] = compute_row_hash(row)
                
                if row["hash"] not in existing_hashes:
                    new_rows.append(row)
                    existing_hashes.add(row["hash"])
                    
            if len(data_list) < page_size:
                break
            page_s += 1
            
    logger.info(f"Ingested {len(new_rows)} new unique stages.")
    if new_rows:
        write_to_bronze("stages", new_rows)

def main():
    parser = argparse.ArgumentParser(description="RD Station CRM API Ingestion Script")
    parser.add_argument("--force-backfill", action="store_true", help="Force a full backfill for all endpoints, bypassing incremental checks")
    args = parser.parse_args()

    logger.info("=== RD STATION CRM SOURCE TO BRONZE INGESTION STARTED ===")
    logger.info(f"Target Database: {DUCKDB_PATH}")
    logger.info(f"Tokens File Path: {TOKENS_PATH}")
    
    # Initialize Schema if not exists
    try:
        with duckdb.connect(DUCKDB_PATH) as con:
            con.execute("CREATE SCHEMA IF NOT EXISTS bronze")
    except Exception as e:
        logger.warning(f"Failed to create schema directly on target database: {e}")

    try:
        client = RDStationCRMClient(TOKENS_PATH)
        
        # 1. Ingest metadata/full-load tables
        ingest_full_table(client, "pipelines", "pipelines")
        ingest_stages(client)
        ingest_full_table(client, "users", "users")
        ingest_full_table(client, "sources", "sources")
        
        # 2. Ingest transaction/incremental tables
        ingest_incremental_table(client, "deals", "deals", force_backfill=args.force_backfill)
        ingest_incremental_table(client, "contacts", "contacts", force_backfill=args.force_backfill)
        
        logger.info("=== RD STATION CRM SOURCE TO BRONZE INGESTION FINISHED SUCCESSFUL ===")
    except Exception as e:
        logger.error(f"Ingestion Pipeline Failed: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    main()
