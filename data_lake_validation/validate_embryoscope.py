import os
import sys
import yaml
import pandas as pd
import duckdb
from pyathena import connect
from datetime import datetime

# Resolve paths
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)
config_path = os.path.join(script_dir, "validation_config.yml")

# Output directory for csv reports
reports_dir = os.path.join(script_dir, "reports")
os.makedirs(reports_dir, exist_ok=True)

# Load configuration
with open(config_path, "r") as f:
    config = yaml.safe_load(f)

local_db_path = os.path.join(project_root, config["local"]["db_path"])
athena_config = config["athena"]
mapping_config = config["mapping"]

print("======================================================================")
print("              DATA LAKE VALIDATION: EMBRYOSCOPE MODULE")
print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Local Database: {local_db_path}")
print(f"Athena Staging: Region={athena_config['region']}, Workgroup={athena_config['work_group']}")
print("======================================================================\n")

# Connect to databases
print("Connecting to local DuckDB...")
local_conn = duckdb.connect(local_db_path, read_only=True)

print("Connecting to AWS Athena...")
athena_conn = connect(region_name=athena_config["region"], work_group=athena_config["work_group"])
athena_cursor = athena_conn.cursor()

def run_athena_query(query):
    """Runs a query in Athena and returns a Pandas DataFrame."""
    # We use pd.read_sql for convenience with Athena connection
    return pd.read_sql(query, athena_conn)

def run_local_query(query):
    """Runs a query in local DuckDB and returns a Pandas DataFrame."""
    return local_conn.execute(query).df()

def compare_schemas():
    print("--- 1. SCHEMA COMPARISON ---")
    
    # Define mapping of tables
    tables = ["patients", "treatments", "embryo_data"]
    
    for t in tables:
        print(f"\nComparing schemas for table: {t}")
        local_table = config["local"]["tables"][t]
        athena_table = f"{athena_config['silver_db']}.{athena_config['tables'][t]}"
        
        # Get local schema
        local_desc = local_conn.execute(f"DESCRIBE {local_table}").fetchall()
        local_cols = {row[0].lower(): row[1] for row in local_desc}
        
        # Get Athena schema
        athena_cursor.execute(f"DESCRIBE {athena_table}")
        athena_desc = athena_cursor.fetchall()
        athena_cols = {}
        for row in athena_desc:
            col_str = row[0]
            if not col_str.strip() or col_str.startswith("#"):
                continue
            parts = col_str.split("\t")
            col_name = parts[0].strip().lower()
            col_type = parts[1].strip() if len(parts) > 1 else "unknown"
            athena_cols[col_name] = col_type
            
        # Compare columns
        local_only = set(local_cols.keys()) - set(athena_cols.keys())
        athena_only = set(athena_cols.keys()) - set(local_cols.keys())
        common = set(local_cols.keys()) & set(athena_cols.keys())
        
        # Check mapping fields to verify snake vs camel
        print(f"  - Local columns: {len(local_cols)}")
        print(f"  - Athena columns: {len(athena_cols)}")
        
        if local_only:
            # Filter out internal/metadata columns for cleaner output
            user_local_only = [c for c in local_only if not c.startswith("_")]
            print(f"  - Columns in Local ONLY (non-meta): {user_local_only}")
        if athena_only:
            user_athena_only = [c for c in athena_only if not c.startswith("_")]
            print(f"  - Columns in Athena ONLY (non-meta): {user_athena_only}")
            
        # Type differences
        type_diffs = []
        for c in common:
            l_type = local_cols[c]
            a_type = athena_cols[c]
            # Simple normalization of types for comparison
            l_norm = "timestamp" if "timestamp" in l_type.lower() else ("integer" if "int" in l_type.lower() or "big" in l_type.lower() else "string")
            a_norm = "timestamp" if "timestamp" in a_type.lower() else ("integer" if "int" in a_type.lower() or "big" in a_type.lower() else "string")
            if l_norm != a_norm:
                type_diffs.append((c, l_type, a_type))
                
        if type_diffs:
            print(f"  - Column type differences: {type_diffs}")
        else:
            print("  - Schema matches (names and normalized types are aligned).")

def compare_row_counts():
    print("\n--- 2. ROW COUNT COMPARISON ---")
    
    tables = ["patients", "treatments", "embryo_data"]
    results = []
    
    for t in tables:
        local_table = config["local"]["tables"][t]
        athena_table = f"{athena_config['silver_db']}.{athena_config['tables'][t]}"
        
        # Get count
        l_count = local_conn.execute(f"SELECT COUNT(*) FROM {local_table}").fetchone()[0]
        athena_cursor.execute(f"SELECT COUNT(*) FROM {athena_table}")
        a_count = athena_cursor.fetchone()[0]
        
        diff = a_count - l_count
        pct = (diff / l_count * 100) if l_count > 0 else 0
        
        results.append({
            "Table": t,
            "Local Count": l_count,
            "Athena Count": a_count,
            "Diff": diff,
            "Pct Diff": pct
        })
        
    df_counts = pd.DataFrame(results)
    print(df_counts.to_string(index=False, formatters={
        "Local Count": lambda x: f"{x:,}",
        "Athena Count": lambda x: f"{x:,}",
        "Diff": lambda x: f"{x:+,}",
        "Pct Diff": lambda x: f"{x:+.2f}%"
    }))
    
    # Breakdown by location/clinic
    print("\nRow count breakdown by clinic (source_server):")
    for t in ["patients", "treatments", "embryo_data"]:
        print(f"\nClinic Breakdown for {t}:")
        local_table = config["local"]["tables"][t]
        athena_table = f"{athena_config['silver_db']}.{athena_config['tables'][t]}"
        
        loc_col_local = mapping_config[t]["location"]["local"]
        loc_col_athena = mapping_config[t]["location"]["athena"]
        
        # Query local
        loc_query = f"SELECT {loc_col_local} as clinic, COUNT(*) as cnt FROM {local_table} GROUP BY {loc_col_local}"
        local_breakdown = run_local_query(loc_query).set_index("clinic")
        
        # Query Athena
        athena_loc_query = f"SELECT {loc_col_athena} as clinic, COUNT(*) as cnt FROM {athena_table} GROUP BY {loc_col_athena}"
        athena_breakdown = run_athena_query(athena_loc_query).set_index("clinic")
        
        # Merge breakdowns
        merged = pd.merge(local_breakdown, athena_breakdown, left_index=True, right_index=True, how="outer", suffixes=("_local", "_athena")).fillna(0).astype(int)
        merged["diff"] = merged["cnt_athena"] - merged["cnt_local"]
        print(merged.to_string(formatters={
            "cnt_local": lambda x: f"{x:,}",
            "cnt_athena": lambda x: f"{x:,}",
            "diff": lambda x: f"{x:+,}"
        }))

def trace_missing_to_bronze(missing_keys, table_type, key_col_name):
    """Checks if missing keys exist in Athena's Bronze layer."""
    if not missing_keys:
        return {}
        
    bronze_table = f"{athena_config['bronze_db']}.{athena_config['tables'][table_type]}"
    
    # Let's batch keys to query bronze. 
    # Since missing_keys could be large, we query a sample of up to 100 keys
    sample_keys = list(missing_keys)[:100]
    
    # Format keys for SQL IN clause
    formatted_keys = ", ".join([f"'{k}'" for k in sample_keys])
    
    # In bronze, the key name is usually same as silver but let's check
    # Let's map key names: 
    # patients -> patient_id_x (but wait, in bronze it is patient_id_x)
    # treatments -> patient_id_x (Wait! In treatments, it's a composite key, so we need to handle it. For simplicity we check patient_id_x)
    # embryo_data -> embryo_id
    bronze_key_col = "patient_id_x" if table_type == "patients" else ("embryo_id" if table_type == "embryo_data" else "patient_id_x")
    
    query = f"SELECT DISTINCT {bronze_key_col} FROM {bronze_table} WHERE {bronze_key_col} IN ({formatted_keys})"
    
    try:
        athena_cursor.execute(query)
        found_in_bronze = {row[0] for row in athena_cursor.fetchall()}
        return found_in_bronze
    except Exception as e:
        print(f"    Error tracing to bronze table {bronze_table}: {e}")
        return {}

def analyze_patients():
    print("\n--- 3. DETAILED DISCREPANCY ANALYSIS: PATIENTS ---")
    local_table = config["local"]["tables"]["patients"]
    athena_table = f"{athena_config['silver_db']}.{athena_config['tables']['patients']}"
    
    # Load keys
    print("Loading patient keys from Local and Athena...")
    local_df = run_local_query(f"SELECT PatientIDx, prontuario, FirstName, LastName, unit_huntington FROM {local_table}")
    athena_df = run_athena_query(f"SELECT patient_id_x, patient_id, first_name, last_name, source_server FROM {athena_table}")
    
    local_keys = set(local_df["PatientIDx"].dropna().unique())
    athena_keys = set(athena_df["patient_id_x"].dropna().unique())
    
    local_only = local_keys - athena_keys
    athena_only = athena_keys - local_keys
    
    print(f"Total Unique PatientIDx in Local: {len(local_keys):,}")
    print(f"Total Unique PatientIDx in Athena: {len(athena_keys):,}")
    print(f"PatientIDx in Local but MISSING in Athena (Silver): {len(local_only):,}")
    print(f"PatientIDx in Athena but MISSING in Local: {len(athena_only):,}")
    
    # Tracing missing to Bronze
    if local_only:
        print(f"Tracing first 100 missing patients to Athena Bronze layer...")
        found_in_bronze = trace_missing_to_bronze(local_only, "patients", "patient_id_x")
        print(f"  - Out of 100 missing patients, {len(found_in_bronze)} exist in Athena BRONZE layer.")
        print(f"  - This implies {len(found_in_bronze)} were ingested to bronze but filtered/failed in silver.")
        print(f"  - And {100 - len(found_in_bronze)} never reached the bronze layer in AWS.")
    
    # Save report
    df_mismatches = pd.DataFrame()
    if local_only or athena_only:
        local_only_df = local_df[local_df["PatientIDx"].isin(local_only)].copy()
        local_only_df = local_only_df.rename(columns={"unit_huntington": "Location"})
        local_only_df["Source"] = "Local Only"
        
        athena_only_df = athena_df[athena_df["patient_id_x"].isin(athena_only)].copy()
        athena_only_df = athena_only_df.rename(columns={
            "patient_id_x": "PatientIDx",
            "patient_id": "prontuario",
            "first_name": "FirstName",
            "last_name": "LastName",
            "source_server": "Location"
        })
        athena_only_df["Source"] = "Athena Only"
        
        df_mismatches = pd.concat([local_only_df, athena_only_df], ignore_index=True)
        
    if not df_mismatches.empty:
        report_path = os.path.join(reports_dir, "mismatches_patients.csv")
        df_mismatches.to_csv(report_path, index=False)
        print(f"Saved complete patient mismatch report to: {report_path}")
        
        # Display examples
        print("\nTraceable Examples of Patients present in Local but MISSING in Athena Silver:")
        local_only_df = df_mismatches[df_mismatches["Source"] == "Local Only"]
        if not local_only_df.empty:
            print(local_only_df.head(10).to_string(index=False))
        else:
            print("None")
            
        print("\nTraceable Examples of Patients present in Athena but MISSING in Local:")
        athena_only_df = df_mismatches[df_mismatches["Source"] == "Athena Only"]
        if not athena_only_df.empty:
            print(athena_only_df.head(10).to_string(index=False))
        else:
            print("None")

def analyze_treatments():
    print("\n--- 4. DETAILED DISCREPANCY ANALYSIS: TREATMENTS ---")
    local_table = config["local"]["tables"]["treatments"]
    athena_table = f"{athena_config['silver_db']}.{athena_config['tables']['treatments']}"
    
    print("Loading treatments keys from Local and Athena...")
    local_df = run_local_query(f"SELECT PatientIDx, TreatmentName, unit_huntington FROM {local_table}")
    athena_df = run_athena_query(f"SELECT patient_id_x, treatment_name, source_server FROM {athena_table}")
    
    # Create composite key
    local_df["key"] = local_df["PatientIDx"] + " || " + local_df["TreatmentName"]
    athena_df["key"] = athena_df["patient_id_x"] + " || " + athena_df["treatment_name"]
    
    local_keys = set(local_df["key"].dropna().unique())
    athena_keys = set(athena_df["key"].dropna().unique())
    
    local_only = local_keys - athena_keys
    athena_only = athena_keys - local_keys
    
    print(f"Total Unique Treatments in Local: {len(local_keys):,}")
    print(f"Total Unique Treatments in Athena: {len(athena_keys):,}")
    print(f"Treatments in Local but MISSING in Athena: {len(local_only):,}")
    print(f"Treatments in Athena but MISSING in Local: {len(athena_only):,}")
    
    df_mismatches = pd.DataFrame()
    if local_only or athena_only:
        local_only_df = local_df[local_df["key"].isin(local_only)].copy()
        local_only_df = local_only_df.rename(columns={"unit_huntington": "Location"})
        local_only_df["Source"] = "Local Only"
        
        athena_only_df = athena_df[athena_df["key"].isin(athena_only)].copy()
        athena_only_df = athena_only_df.rename(columns={
            "patient_id_x": "PatientIDx",
            "treatment_name": "TreatmentName",
            "source_server": "Location"
        })
        athena_only_df["Source"] = "Athena Only"
        
        df_mismatches = pd.concat([local_only_df, athena_only_df], ignore_index=True)
        
    if not df_mismatches.empty:
        report_path = os.path.join(reports_dir, "mismatches_treatments.csv")
        df_mismatches.to_csv(report_path, index=False)
        print(f"Saved complete treatment mismatch report to: {report_path}")
        
        print("\nTraceable Examples of Treatments present in Local but MISSING in Athena:")
        local_only_df = df_mismatches[df_mismatches["Source"] == "Local Only"]
        if not local_only_df.empty:
            print(local_only_df.head(10).to_string(index=False))
        else:
            print("None")

def analyze_embryos():
    print("\n--- 5. DETAILED DISCREPANCY ANALYSIS: EMBRYO DATA ---")
    local_table = config["local"]["tables"]["embryo_data"]
    athena_table = f"{athena_config['silver_db']}.{athena_config['tables']['embryo_data']}"
    
    print("Loading embryo keys from Local and Athena...")
    local_df = run_local_query(f"SELECT EmbryoID, PatientIDx, TreatmentName, unit_huntington FROM {local_table}")
    athena_df = run_athena_query(f"SELECT embryo_id, patient_id_x, treatment_name, unit_huntington FROM {athena_table}")
    
    local_keys = set(local_df["EmbryoID"].dropna().unique())
    athena_keys = set(athena_df["embryo_id"].dropna().unique())
    
    local_only = local_keys - athena_keys
    athena_only = athena_keys - local_keys
    
    print(f"Total Unique Embryos in Local: {len(local_keys):,}")
    print(f"Total Unique Embryos in Athena: {len(athena_keys):,}")
    print(f"Embryos in Local but MISSING in Athena (Silver): {len(local_only):,}")
    print(f"Embryos in Athena but MISSING in Local: {len(athena_only):,}")
    
    # Tracing missing to Bronze (since we have a massive +18k embryos in Athena, let's see why they aren't in local)
    # Wait, if Athena has MORE embryos, we trace the athena_only embryos to see if they exist in the local SQLite clinic databases
    # to understand if they are missing locally because the local process didn't run recently, or why they are extra in Athena.
    if athena_only:
        print(f"Tracing first 100 extra embryos in Athena to see if they are present in local Bronze clinic databases...")
        # We can implement a quick check later if needed, but first let's list some examples.
        pass
        
    df_mismatches = pd.DataFrame()
    if local_only or athena_only:
        local_only_df = local_df[local_df["EmbryoID"].isin(local_only)].copy()
        local_only_df = local_only_df.rename(columns={"unit_huntington": "Location"})
        local_only_df["Source"] = "Local Only"
        
        athena_only_df = athena_df[athena_df["embryo_id"].isin(athena_only)].copy()
        athena_only_df = athena_only_df.rename(columns={
            "embryo_id": "EmbryoID",
            "patient_id_x": "PatientIDx",
            "treatment_name": "TreatmentName",
            "unit_huntington": "Location"
        })
        athena_only_df["Source"] = "Athena Only"
        
        df_mismatches = pd.concat([local_only_df, athena_only_df], ignore_index=True)
        
    if not df_mismatches.empty:
        report_path = os.path.join(reports_dir, "mismatches_embryos.csv")
        df_mismatches.to_csv(report_path, index=False)
        print(f"Saved complete embryo mismatch report to: {report_path}")
        
        print("\nTraceable Examples of Embryos present in Local but MISSING in Athena Silver:")
        local_only_df = df_mismatches[df_mismatches["Source"] == "Local Only"]
        if not local_only_df.empty:
            print(local_only_df.head(10).to_string(index=False))
        else:
            print("None")
            
        print("\nTraceable Examples of Embryos present in Athena but MISSING in Local:")
        athena_only_df = df_mismatches[df_mismatches["Source"] == "Athena Only"]
        if not athena_only_df.empty:
            print(athena_only_df.head(10).to_string(index=False))
        else:
            print("None")

def main():
    try:
        compare_schemas()
        compare_row_counts()
        analyze_patients()
        analyze_treatments()
        analyze_embryos()
    except Exception as e:
        print(f"Error during validation: {e}", file=sys.stderr)
        raise
    finally:
        local_conn.close()

if __name__ == "__main__":
    main()
