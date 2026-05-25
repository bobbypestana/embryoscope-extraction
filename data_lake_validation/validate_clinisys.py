import os
import sys
import yaml
import pandas as pd
import duckdb
from pyathena import connect

# Add parent directory to path to allow importing local modules if needed
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

# Database configurations
duckdb_path = r"g:\My Drive\projetos_individuais\Huntington\database\clinisys_all.duckdb"
region = "sa-east-1"
workgroup = "datalake-admins"
athena_db = "bronze_clinisys_staging"

def main():
    print("=================================================================")
    print("CLINISYS DATA LAKE VALIDATION: DUCKDB VS AWS ATHENA")
    print("=================================================================")
    
    # 1. Connect to databases
    if not os.path.exists(duckdb_path):
        print(f"Error: Local DuckDB database not found at {duckdb_path}")
        sys.exit(1)
        
    print(f"Connecting to local DuckDB: {duckdb_path}")
    local_conn = duckdb.connect(duckdb_path, read_only=True)
    
    print(f"Connecting to AWS Athena (region: {region}, workgroup: {workgroup})")
    try:
        athena_conn = connect(region_name=region, work_group=workgroup)
        athena_cur = athena_conn.cursor()
    except Exception as e:
        print(f"Error connecting to AWS Athena: {e}")
        local_conn.close()
        sys.exit(1)
        
    # 2. Get list of tables in local silver schema
    local_tables_df = local_conn.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'silver'"
    ).df()
    local_tables = sorted(local_tables_df['table_name'].tolist())
    print(f"\nFound {len(local_tables)} tables in local DuckDB 'silver' schema.")
    
    # 3. Get list of tables in Athena bronze_clinisys_staging database
    athena_cur.execute(f"SHOW TABLES IN {athena_db}")
    athena_tables = sorted([row[0] for row in athena_cur.fetchall()])
    print(f"Found {len(athena_tables)} tables in Athena '{athena_db}' database.")
    
    # Identify common tables
    common_tables = sorted(list(set(local_tables) & set(athena_tables)))
    print(f"\nComparing {len(common_tables)} common tables:")
    
    comparison_results = []
    
    for idx, table in enumerate(common_tables, 1):
        print(f"[{idx}/{len(common_tables)}] Comparing table: {table}...")
        
        # Row count local
        try:
            local_count = local_conn.execute(f"SELECT COUNT(*) FROM silver.{table}").fetchone()[0]
        except Exception as e:
            print(f"  Error reading local table {table}: {e}")
            local_count = -1
            
        # Row count Athena
        try:
            athena_cur.execute(f"SELECT COUNT(*) FROM {athena_db}.{table}")
            athena_count = athena_cur.fetchone()[0]
        except Exception as e:
            print(f"  Error reading Athena table {table}: {e}")
            athena_count = -1
            
        diff = local_count - athena_count if local_count >= 0 and athena_count >= 0 else None
        pct_diff = (diff / athena_count * 100) if diff is not None and athena_count > 0 else None
        
        comparison_results.append({
            "Table": table,
            "Local_Silver_Count": local_count,
            "Athena_Bronze_Count": athena_count,
            "Difference": diff,
            "Pct_Diff": pct_diff
        })
        
    df_res = pd.DataFrame(comparison_results)
    
    # Display summary table
    print("\nSummary of row count comparisons:")
    pd.set_option('display.max_rows', 100)
    print(df_res.to_string(index=False))
    
    # Write report file
    output_dir = r"g:\My Drive\projetos_individuais\Huntington\data_lake_validation\reports"
    os.makedirs(output_dir, exist_ok=True)
    report_csv = os.path.join(output_dir, "clinisys_row_counts.csv")
    df_res.to_csv(report_csv, index=False)
    print(f"\nDetailed counts report written to: {report_csv}")
    
    # 4. Schema analysis of a few key tables
    print("\nPerforming schema analysis for key tables...")
    key_tables = [
        "view_pacientes",
        "view_tratamentos",
        "view_micromanipulacao",
        "view_micromanipulacao_oocitos",
        "view_congelamentos_embrioes",
        "view_descongelamentos_embrioes"
    ]
    
    schema_diff_file = os.path.join(output_dir, "clinisys_schema_diff.txt")
    with open(schema_diff_file, "w") as f:
        f.write("CLINISYS SCHEMA COMPARISON (Local Silver vs Athena Bronze)\n")
        f.write("=========================================================\n\n")
        
        for table in key_tables:
            if table not in common_tables:
                continue
            
            f.write(f"Table: {table}\n")
            f.write(f"-----------------------------------------\n")
            
            # Local columns
            local_desc = local_conn.execute(f"DESCRIBE silver.{table}").fetchall()
            local_cols = {row[0].lower(): row[0] for row in local_desc}
            
            # Athena columns
            athena_cur.execute(f"DESCRIBE {athena_db}.{table}")
            athena_desc = athena_cur.fetchall()
            athena_cols = {}
            for row in athena_desc:
                col_str = row[0]
                if not col_str.strip() or col_str.startswith("#"):
                    continue
                parts = col_str.split("\t")
                col_name = parts[0].strip().lower()
                athena_cols[col_name] = parts[0].strip()
                
            # Normalize helper to compare snake vs camel/Pascal cases
            def norm(c):
                return c.replace('_', '').lower()
                
            local_norm = {norm(col): col for col in local_cols.values()}
            athena_norm = {norm(col): col for col in athena_cols.values()}
            
            local_norm_set = set(local_norm.keys())
            athena_norm_set = set(athena_norm.keys())
            
            only_in_local_norm = local_norm_set - athena_norm_set
            only_in_athena_norm = athena_norm_set - local_norm_set
            
            # Map normalized back to original
            only_in_local = [local_norm[k] for k in sorted(only_in_local_norm)]
            only_in_athena = [athena_norm[k] for k in sorted(only_in_athena_norm)]
            
            # Specifically check for encoding issues (where normalized names are different but close, or non-ascii columns)
            # E.g. bcf_embrião1_fonte vs bcf_embri_o1_fonte
            encoding_issues = []
            for l_col in local_cols.values():
                if not l_col.isascii():
                    # Look for a similar ascii column in Athena
                    l_norm_clean = norm(l_col).replace('ã', 'o').replace('õ', 'o').replace('ç', 'c').replace('í', 'i').replace('á', 'a').replace('é', 'e')
                    for a_col in athena_cols.values():
                        if norm(a_col) == l_norm_clean:
                            encoding_issues.append(f"{l_col} (Local) -> {a_col} (Athena)")
                            # Remove from only_in_local and only_in_athena as it's an encoding issue, not a missing column
                            if l_col in only_in_local:
                                only_in_local.remove(l_col)
                            if a_col in only_in_athena:
                                only_in_athena.remove(a_col)
            
            f.write(f"Local Silver columns count: {len(local_cols)}\n")
            f.write(f"Athena Bronze columns count: {len(athena_cols)}\n\n")
            
            if encoding_issues:
                f.write("Columns with Encoding/Character Mapping Differences:\n")
                for issue in encoding_issues:
                    f.write(f"  - {issue}\n")
                f.write("\n")
            
            f.write("Columns in Local Silver but missing in Athena Bronze:\n")
            if only_in_local:
                for col in only_in_local:
                    f.write(f"  - {col}\n")
            else:
                f.write("  None\n")
                
            f.write("\nColumns in Athena Bronze but missing in Local Silver:\n")
            if only_in_athena:
                for col in only_in_athena:
                    f.write(f"  - {col}\n")
            else:
                f.write("  None\n")
            f.write("\n" + "="*50 + "\n\n")
            
    print(f"Detailed schema analysis report written to: {schema_diff_file}")
    
    local_conn.close()
    athena_conn.close()

if __name__ == '__main__':
    main()
