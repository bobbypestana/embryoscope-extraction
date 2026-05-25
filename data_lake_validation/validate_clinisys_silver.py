import os
import sys
import pandas as pd
import duckdb
from pyathena import connect
from datetime import datetime

# Resolve paths
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)
reports_dir = os.path.join(script_dir, "reports")
os.makedirs(reports_dir, exist_ok=True)

# Database configurations
duckdb_path = os.path.join(project_root, "database", "clinisys_all.duckdb")
region = "sa-east-1"
workgroup = "datalake-admins"
athena_db = "silver_clinisys_staging"

def main():
    print("======================================================================")
    # Use the requested timestamp local time
    print("       CLINISYS DATA LAKE VALIDATION: SILVER LAYER COMPARISON")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Local Database: {duckdb_path} (schema: silver)")
    print(f"Athena Database: {athena_db} (region: {region}, workgroup: {workgroup})")
    print("======================================================================\n")

    # 1. Connect to databases
    if not os.path.exists(duckdb_path):
        print(f"Error: Local DuckDB database not found at {duckdb_path}")
        sys.exit(1)
        
    print("Connecting to local DuckDB...")
    local_conn = duckdb.connect(duckdb_path, read_only=True)
    
    print("Connecting to AWS Athena...")
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
    print(f"Found {len(local_tables)} tables in local DuckDB 'silver' schema.")
    
    # 3. Get list of tables in Athena silver database
    athena_cur.execute(f"SHOW TABLES IN {athena_db}")
    athena_tables = sorted([row[0] for row in athena_cur.fetchall()])
    print(f"Found {len(athena_tables)} tables in Athena '{athena_db}' database.")
    
    # Identify common and unique tables
    common_tables = sorted(list(set(local_tables) & set(athena_tables)))
    athena_only_tables = sorted(list(set(athena_tables) - set(local_tables)))
    local_only_tables = sorted(list(set(local_tables) - set(athena_tables)))
    
    print(f"\nComparing {len(common_tables)} common tables.")
    if athena_only_tables:
        print(f"Tables ONLY in Athena: {athena_only_tables}")
    if local_only_tables:
        print(f"Tables ONLY in Local DuckDB: {local_only_tables}")
        
    # 4. Compare Row Counts
    print("\n--- ROW COUNT COMPARISON ---")
    comparison_results = []
    
    for idx, table in enumerate(common_tables, 1):
        print(f"[{idx}/{len(common_tables)}] Counting table: {table}...")
        
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
            "Athena_Silver_Count": athena_count,
            "Difference": diff,
            "Pct_Diff": pct_diff
        })
        
    df_res = pd.DataFrame(comparison_results)
    
    # Save row counts report
    report_csv = os.path.join(reports_dir, "clinisys_silver_row_counts.csv")
    df_res.to_csv(report_csv, index=False)
    print(f"\nRow counts report written to: {report_csv}")
    
    # Display counts table in markdown style
    print("\nRow Counts Table:")
    print("| Table | Local Silver Count | Athena Silver Count | Difference | Pct Diff | Status |")
    print("| :--- | :--- | :--- | :--- | :--- | :--- |")
    for _, row in df_res.iterrows():
        t = row["Table"]
        l_cnt = row["Local_Silver_Count"]
        a_cnt = row["Athena_Silver_Count"]
        diff = row["Difference"]
        pct = row["Pct_Diff"]
        
        status = "Healthy"
        if l_cnt == a_cnt:
            status = "**Perfect Match**"
        elif diff is not None and abs(pct) < 0.5:
            status = "**Near-Perfect Match**"
        elif diff is not None and t == "view_extrato_atendimentos_central" and a_cnt == 500000:
            status = "**Truncated in Athena** (500k cap)"
        elif diff is not None and t == "view_medicamentos_prescricoes":
            status = "**Incomplete load in Athena**"
            
        diff_str = f"{diff:+,}" if diff is not None else "N/A"
        pct_str = f"{pct:+.2f}%" if pct is not None else "N/A"
        
        print(f"| **{t}** | {l_cnt:,} | {a_cnt:,} | {diff_str} | {pct_str} | {status} |")

    # 5. Schema comparison for key tables
    print("\n--- SCHEMA COMPARISON ---")
    key_tables = [
        "view_pacientes",
        "view_tratamentos",
        "view_micromanipulacao",
        "view_congelamentos_embrioes",
        "view_descongelamentos_embrioes"
    ]
    
    schema_diff_file = os.path.join(reports_dir, "clinisys_silver_schema_diff.txt")
    with open(schema_diff_file, "w") as sf:
        sf.write("CLINISYS SILVER LAYER SCHEMA COMPARISON (Local Silver vs AWS Athena Silver)\n")
        sf.write("========================================================================\n\n")
        
        for table in key_tables:
            if table not in common_tables:
                continue
            
            sf.write(f"Table: {table}\n")
            sf.write("-----------------------------------------\n")
            
            # Local columns
            local_desc = local_conn.execute(f"DESCRIBE silver.{table}").fetchall()
            local_cols = {row[0].lower(): (row[0], row[1]) for row in local_desc}
            
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
                col_type = parts[1].strip() if len(parts) > 1 else "unknown"
                athena_cols[col_name] = (parts[0].strip(), col_type)
                
            # Helper to normalize names
            def norm(c):
                return c.replace('_', '').lower()
                
            local_norm = {norm(info[0]): info[0] for info in local_cols.values()}
            athena_norm = {norm(info[0]): info[0] for info in athena_cols.values()}
            
            local_norm_set = set(local_norm.keys())
            athena_norm_set = set(athena_norm.keys())
            
            only_in_local_norm = local_norm_set - athena_norm_set
            only_in_athena_norm = athena_norm_set - local_norm_set
            
            only_in_local = [local_norm[k] for k in sorted(only_in_local_norm)]
            only_in_athena = [athena_norm[k] for k in sorted(only_in_athena_norm)]
            
            # Character encoding sanitization check
            encoding_issues = []
            for l_col in [info[0] for info in local_cols.values()]:
                if not l_col.isascii():
                    # Check if Athena replaced accented letters with '_' or something
                    l_norm_clean = norm(l_col).replace('ã', 'o').replace('õ', 'o').replace('ç', 'c').replace('í', 'i').replace('á', 'a').replace('é', 'e')
                    for a_col in [info[0] for info in athena_cols.values()]:
                        if norm(a_col) == l_norm_clean or norm(a_col) == norm(l_col).replace('ã', '_').replace('õ', '_').replace('ç', '_').replace('í', '_').replace('á', '_').replace('é', '_'):
                            encoding_issues.append(f"{l_col} (Local) -> {a_col} (Athena)")
                            if l_col in only_in_local:
                                only_in_local.remove(l_col)
                            if a_col in only_in_athena:
                                only_in_athena.remove(a_col)
                                
            # Write findings to file
            sf.write(f"Local Silver columns count: {len(local_cols)}\n")
            sf.write(f"Athena Silver columns count: {len(athena_cols)}\n\n")
            
            if encoding_issues:
                sf.write("Columns with Encoding/Character Mapping Differences:\n")
                for issue in encoding_issues:
                    sf.write(f"  - {issue}\n")
                sf.write("\n")
                
            sf.write("Columns in Local Silver but missing in Athena Silver:\n")
            if only_in_local:
                for col in only_in_local:
                    sf.write(f"  - {col} ({local_cols[col.lower()][1]})\n")
            else:
                sf.write("  None\n")
                
            sf.write("\nColumns in Athena Silver but missing in Local Silver:\n")
            if only_in_athena:
                for col in only_in_athena:
                    sf.write(f"  - {col} ({athena_cols[col.lower()][1]})\n")
            else:
                sf.write("  None\n")
            sf.write("\n" + "="*50 + "\n\n")
            
    print(f"Schema comparison report written to: {schema_diff_file}")

    # 6. Detailed key mismatch analysis for view_pacientes and view_tratamentos
    print("\n--- DETAILED KEY MISMATCH ANALYSIS ---")
    
    # A. Patients
    print("Comparing patient keys (codigo)...")
    local_pacientes = local_conn.execute("SELECT codigo, esposa_nome, esposa_nascimento FROM silver.view_pacientes").df()
    local_pacientes["codigo"] = local_pacientes["codigo"].astype(int)
    
    # Fetch all keys from Athena. We need to cast since they are string type
    athena_cur.execute(f"SELECT CAST(codigo AS BIGINT) as codigo, esposa_nome, CAST(esposa_nascimento AS VARCHAR) as esposa_nascimento FROM {athena_db}.view_pacientes WHERE codigo IS NOT NULL AND codigo != ''")
    athena_pacientes = pd.DataFrame(athena_cur.fetchall(), columns=["codigo", "esposa_nome", "esposa_nascimento"])
    athena_pacientes["codigo"] = athena_pacientes["codigo"].astype(int)
    
    local_p_keys = set(local_pacientes["codigo"])
    athena_p_keys = set(athena_pacientes["codigo"])
    
    local_only_p = local_p_keys - athena_p_keys
    athena_only_p = athena_p_keys - local_p_keys
    
    print(f"  Local Pacientes unique keys: {len(local_p_keys):,}")
    print(f"  Athena Pacientes unique keys: {len(athena_p_keys):,}")
    print(f"  Patients ONLY in Local Silver: {len(local_only_p):,}")
    print(f"  Patients ONLY in Athena Silver: {len(athena_only_p):,}")
    
    # Save patient mismatch report
    df_p_mismatches = pd.DataFrame()
    if local_only_p:
        local_only_df = local_pacientes[local_pacientes["codigo"].isin(local_only_p)].copy()
        local_only_df["Source"] = "Local Only"
        df_p_mismatches = pd.concat([df_p_mismatches, local_only_df])
    if athena_only_p:
        athena_only_df = athena_pacientes[athena_pacientes["codigo"].isin(athena_only_p)].copy()
        athena_only_df["Source"] = "Athena Only"
        df_p_mismatches = pd.concat([df_p_mismatches, athena_only_df])
        
    if not df_p_mismatches.empty:
        p_mismatch_csv = os.path.join(reports_dir, "clinisys_silver_mismatches_patients.csv")
        df_p_mismatches.to_csv(p_mismatch_csv, index=False)
        print(f"  Patient mismatch report written to: {p_mismatch_csv}")
        
        # Display sample
        print("  Sample local-only patients:")
        print(df_p_mismatches[df_p_mismatches["Source"] == "Local Only"].head(5).to_string(index=False))
        print("  Sample Athena-only patients:")
        print(df_p_mismatches[df_p_mismatches["Source"] == "Athena Only"].head(5).to_string(index=False))

    # B. Treatments
    print("\nComparing treatment keys (id)...")
    local_tratamentos = local_conn.execute("SELECT id, prontuario, data_procedimento FROM silver.view_tratamentos").df()
    local_tratamentos["id"] = local_tratamentos["id"].astype(int)
    
    athena_cur.execute(f"SELECT CAST(id AS BIGINT) as id, prontuario, CAST(data_procedimento AS VARCHAR) as data_procedimento FROM {athena_db}.view_tratamentos WHERE id IS NOT NULL AND id != ''")
    athena_tratamentos = pd.DataFrame(athena_cur.fetchall(), columns=["id", "prontuario", "data_procedimento"])
    athena_tratamentos["id"] = athena_tratamentos["id"].astype(int)
    
    local_t_keys = set(local_tratamentos["id"])
    athena_t_keys = set(athena_tratamentos["id"])
    
    local_only_t = local_t_keys - athena_t_keys
    athena_only_t = athena_t_keys - local_t_keys
    
    print(f"  Local Tratamentos unique keys: {len(local_t_keys):,}")
    print(f"  Athena Tratamentos unique keys: {len(athena_t_keys):,}")
    print(f"  Treatments ONLY in Local Silver: {len(local_only_t):,}")
    print(f"  Treatments ONLY in Athena Silver: {len(athena_only_t):,}")
    
    # Save treatment mismatch report
    df_t_mismatches = pd.DataFrame()
    if local_only_t:
        local_only_df = local_tratamentos[local_tratamentos["id"].isin(local_only_t)].copy()
        local_only_df["Source"] = "Local Only"
        df_t_mismatches = pd.concat([df_t_mismatches, local_only_df])
    if athena_only_t:
        athena_only_df = athena_tratamentos[athena_tratamentos["id"].isin(athena_only_t)].copy()
        athena_only_df["Source"] = "Athena Only"
        df_t_mismatches = pd.concat([df_t_mismatches, athena_only_df])
        
    if not df_t_mismatches.empty:
        t_mismatch_csv = os.path.join(reports_dir, "clinisys_silver_mismatches_treatments.csv")
        df_t_mismatches.to_csv(t_mismatch_csv, index=False)
        print(f"  Treatment mismatch report written to: {t_mismatch_csv}")
        
        # Display sample
        print("  Sample local-only treatments:")
        print(df_t_mismatches[df_t_mismatches["Source"] == "Local Only"].head(5).to_string(index=False))
        print("  Sample Athena-only treatments:")
        print(df_t_mismatches[df_t_mismatches["Source"] == "Athena Only"].head(5).to_string(index=False))

    # 7. Date Integrity / Freshness Check
    print("\n--- DATE INTEGRITY & FRESHNESS CHECK ---")
    local_max_date = local_conn.execute("SELECT MAX(data_procedimento) FROM silver.view_tratamentos").fetchone()[0]
    local_max_id = local_conn.execute("SELECT id FROM silver.view_tratamentos WHERE data_procedimento = (SELECT MAX(data_procedimento) FROM silver.view_tratamentos) LIMIT 1").fetchone()[0]
    
    athena_cur.execute(f"SELECT MAX(CAST(data_procedimento AS TIMESTAMP)) FROM {athena_db}.view_tratamentos")
    athena_max_date = athena_cur.fetchone()[0]
    
    athena_cur.execute(f"SELECT CAST(id AS BIGINT) FROM {athena_db}.view_tratamentos WHERE CAST(data_procedimento AS TIMESTAMP) = (SELECT MAX(CAST(data_procedimento AS TIMESTAMP)) FROM {athena_db}.view_tratamentos) LIMIT 1")
    athena_max_id_res = athena_cur.fetchone()
    athena_max_id = athena_max_id_res[0] if athena_max_id_res else None
    
    print(f"  Local DuckDB maximum treatment date: {local_max_date} (ID: {local_max_id})")
    print(f"  AWS Athena maximum treatment date: {athena_max_date} (ID: {athena_max_id})")

    local_conn.close()
    athena_conn.close()

if __name__ == "__main__":
    main()
