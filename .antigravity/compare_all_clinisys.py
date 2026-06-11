import os
import sys
import yaml
import duckdb
import pandas as pd

# Add this folder to path to import generic_comparator
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from generic_comparator import run_comparison

def main():
    print("=========================================================")
    print("      CLINISYS MULTI-TABLE DATA LAKE RECONCILIATION")
    print("=========================================================\n")
    
    # 1. Load primary keys from configuration
    config_path = "clinisys/column_config.yml"
    if not os.path.exists(config_path):
        print(f"Error: Column configuration file not found at {config_path}")
        sys.exit(1)
        
    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
        
    primary_keys = config.get("primary_keys", {})
    logger_mapping = config.get("special_columns", {})
    
    local_db = "database/clinisys_all.duckdb"
    athena_db = "silver_clinisys_staging"
    
    if not os.path.exists(local_db):
        print(f"Error: Local DuckDB database not found at {local_db}")
        sys.exit(1)
        
    # 2. Get list of tables in local silver schema
    print(f"Scanning local database '{local_db}'...")
    local_conn = duckdb.connect(local_db, read_only=True)
    local_tables_df = local_conn.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'silver'"
    ).df()
    local_tables = sorted(local_tables_df['table_name'].tolist())
    local_conn.close()
    
    print(f"Found {len(local_tables)} tables in local DuckDB 'silver' schema.")
    
    # 3. Connect to Athena to verify remote tables
    from pyathena import connect
    region = "sa-east-1"
    workgroup = "datalake-admins"
    
    print("Connecting to AWS Athena to verify remote tables...")
    try:
        athena_conn = connect(region_name=region, work_group=workgroup)
        athena_cur = athena_conn.cursor()
        athena_cur.execute(f"SHOW TABLES IN {athena_db}")
        athena_tables = set([row[0] for row in athena_cur.fetchall()])
        athena_conn.close()
    except Exception as e:
        print(f"Error connecting to Athena to list tables: {e}")
        print("Please check your AWS session or credentials.")
        sys.exit(1)
        
    print(f"Found {len(athena_tables)} tables in Athena database '{athena_db}'.")
    
    # Identify common tables to compare
    common_tables = sorted(list(set(local_tables) & athena_tables))
    print(f"\nReady to reconcile {len(common_tables)} overlapping tables between Local and Athena.")
    
    results = []
    
    # 4. Iterate and compare
    for idx, table in enumerate(common_tables, 1):
        if table not in primary_keys:
            print(f"[{idx}/{len(common_tables)}] Skipping table '{table}' - no primary key in config.")
            results.append({"table": table, "status": "Skipped (No Key)"})
            continue
            
        key = primary_keys[table]
        
        # Check if table has 'valor' currency column to verify sums
        value_cols = None
        # We check by querying columns of the table
        conn_temp = duckdb.connect(local_db, read_only=True)
        cols_temp = [c[0].lower() for c in conn_temp.execute(f"SELECT * FROM silver.{table} LIMIT 0").description]
        conn_temp.close()
        
        if 'valor' in cols_temp:
            value_cols = ['valor']
            print(f"[{idx}/{len(common_tables)}] Table '{table}' has currency column 'valor'. Adding to value checks.")
            
        print(f"[{idx}/{len(common_tables)}] Reconciling table: {table} (primary key: {key})")
        
        try:
            report_path = run_comparison(
                local_db=local_db,
                local_table=f"silver.{table}",
                keys=[key],
                value_cols=value_cols,
                athena_db=athena_db,
                athena_table=table,
                output_name=f"clinisys_{table}_reconciliation"
            )
            results.append({
                "table": table,
                "status": "Success",
                "report": report_path
            })
        except Exception as e:
            print(f"Error validating table '{table}': {str(e)}")
            results.append({
                "table": table,
                "status": "Failed",
                "error": str(e)
            })
            
    # 5. Output Summary Results
    print("\n=========================================================")
    print("           RECONCILIATION EXECUTION SUMMARY")
    print("=========================================================")
    
    success_count = sum(1 for r in results if r["status"] == "Success")
    failed_count = sum(1 for r in results if r["status"] == "Failed")
    skipped_count = sum(1 for r in results if r["status"].startswith("Skipped"))
    
    print(f"Total Tables Checked: {len(results)}")
    print(f"  - Passed/Success:  {success_count}")
    print(f"  - Failed:          {failed_count}")
    print(f"  - Skipped:         {skipped_count}\n")
    
    print("| Table Name | Execution Status | Report Path / Details |")
    print("| :--- | :--- | :--- |")
    for r in results:
        status_str = f"**{r['status']}**" if r["status"] != "Success" else "Success"
        detail_str = r.get("report", r.get("error", "N/A"))
        if "report" in r:
            abs_path = os.path.abspath(r['report']).replace('\\', '/')
            detail_str = f"[{os.path.basename(r['report'])}](file:///{abs_path})"
        print(f"| {r['table']} | {status_str} | {detail_str} |")

if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.WARNING, format="%(asctime)s - %(levelname)s - %(message)s")
    main()
