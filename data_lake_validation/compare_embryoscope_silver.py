import os
import sys
import logging
import pandas as pd
import duckdb
from pyathena import connect
from datetime import datetime

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("compare_embryoscope_silver")

# Resolve paths
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)
sys.path.append(script_dir)

from validation_hooks import scrub_pii, archive_report, evaluate_thresholds

DUCKDB_PATH = os.path.join(project_root, "database", "huntington_data_lake.duckdb")
ATHENA_REGION = "sa-east-1"
ATHENA_WORKGROUP = "datalake-admins"
ATHENA_DB = "silver_embryoscope_prod"

TABLES_CONFIG = {
    "patients": {
        "local_table": "silver_embryoscope.patients",
        "athena_table": "patients",
        "keys": ["PatientIDx"],
        "target_keys": ["patient_id_x"],
        "date_col": "DateOfBirth",
        "target_date_col": "date_of_birth"
    },
    "treatments": {
        "local_table": "silver_embryoscope.treatments",
        "athena_table": "treatments",
        "keys": ["PatientIDx", "TreatmentName"],
        "target_keys": ["patient_id_x", "treatment_name"],
        "date_col": "_extraction_timestamp",
        "target_date_col": "bronze_updated_at"
    },
    "embryo_data": {
        "local_table": "silver_embryoscope.embryo_data",
        "athena_table": "embryo_data",
        "keys": ["EmbryoID"],
        "target_keys": ["embryo_id"],
        "date_col": "KIDDate",
        "target_date_col": "kid_date"
    }
}

def normalize_key(val):
    if val is None or pd.isna(val):
        return None
    val_str = str(val).strip().lower()
    if val_str.endswith(".0"):
        val_str = val_str[:-2]
    if val_str.isdigit():
        try:
            val_str = str(int(val_str))
        except ValueError:
            pass
    if " 00:00:00" in val_str:
        val_str = val_str.replace(" 00:00:00", "")
    return val_str

def main():
    logger.info("Initializing Embryoscope Silver Layer Reconciliation Comparison")
    
    # 1. Connect to databases
    logger.info(f"Connecting to DuckDB: {DUCKDB_PATH}")
    local_conn = duckdb.connect(DUCKDB_PATH, read_only=True)
    
    logger.info(f"Connecting to AWS Athena ({ATHENA_DB})")
    athena_conn = connect(region_name=ATHENA_REGION, work_group=ATHENA_WORKGROUP, schema_name=ATHENA_DB)
    athena_cur = athena_conn.cursor()
    
    try:
        global_results = []
        perfect_count = 0
        variance_count = 0
        
        # Track metrics for evaluation
        overall_metrics = {
            'row_count_pct_diff': 0.0,
            'client_match_rate': 1.0,
            'patient_match_rate': 1.0
        }
        
        report_details_md = ""
        
        for idx, (table_name, cfg) in enumerate(TABLES_CONFIG.items(), 1):
            logger.info(f"Comparing table {idx}/{len(TABLES_CONFIG)}: {table_name}")
            local_tbl = cfg["local_table"]
            target_tbl = f"{ATHENA_DB}.{cfg['athena_table']}"
            
            # Get row counts
            local_count = local_conn.execute(f"SELECT COUNT(*) FROM {local_tbl}").fetchone()[0]
            athena_cur.execute(f"SELECT COUNT(*) FROM {target_tbl}")
            target_count = athena_cur.fetchone()[0]
            
            row_diff = local_count - target_count
            row_pct_diff = (row_diff / target_count) if target_count > 0 else 0.0
            
            # Setup selection keys
            l_keys = cfg["keys"]
            t_keys = cfg["target_keys"]
            
            l_select_sql = ", ".join([f'"{c}"' for c in l_keys])
            t_select_sql = ", ".join([f'"{c}"' for c in t_keys])
            
            # Fetch keys
            local_df = local_conn.execute(f"SELECT {l_select_sql} FROM {local_tbl}").df()
            athena_cur.execute(f"SELECT {t_select_sql} FROM {target_tbl}")
            target_df = pd.DataFrame(athena_cur.fetchall(), columns=t_keys)
            
            # Lower columns
            local_df.columns = [c.lower() for c in local_df.columns]
            target_df.columns = [c.lower() for c in target_df.columns]
            
            l_keys_lower = [k.lower() for k in l_keys]
            t_keys_lower = [tk.lower() for tk in t_keys]
            
            # Generate hash/keys
            local_df["_comp_key"] = local_df.apply(
                lambda r: "||".join([normalize_key(r[k]) if normalize_key(r[k]) is not None else "NULL" for k in l_keys_lower]),
                axis=1
            )
            target_df["_comp_key"] = target_df.apply(
                lambda r: "||".join([normalize_key(r[k]) if normalize_key(r[k]) is not None else "NULL" for k in t_keys_lower]),
                axis=1
            )
            
            local_keys_set = set(local_df["_comp_key"])
            target_keys_set = set(target_df["_comp_key"])
            
            overlap_count = len(local_keys_set.intersection(target_keys_set))
            only_local_count = len(local_keys_set - target_keys_set)
            only_target_count = len(target_keys_set - local_keys_set)
            
            local_match_rate = overlap_count / len(local_keys_set) if local_keys_set else 1.0
            target_match_rate = overlap_count / len(target_keys_set) if target_keys_set else 1.0
            
            # Save average rates to overall metrics
            overall_metrics['row_count_pct_diff'] += abs(row_pct_diff)
            overall_metrics['client_match_rate'] = min(overall_metrics['client_match_rate'], local_match_rate)
            overall_metrics['patient_match_rate'] = min(overall_metrics['patient_match_rate'], target_match_rate)
            
            status = "Perfect Match" if row_diff == 0 else "Variance Detected"
            if row_diff == 0:
                perfect_count += 1
            else:
                variance_count += 1
                
            global_results.append({
                "table": table_name,
                "local_count": local_count,
                "target_count": target_count,
                "overlap": overlap_count,
                "only_local": only_local_count,
                "only_target": only_target_count,
                "local_rate": local_match_rate,
                "target_rate": target_match_rate,
                "status": status
            })
            
            # Fetch newest record samples for discrepancies
            logger.info(f"Extracting schema and drift details for {table_name}")
            local_schema = {row[0].lower(): row[1] for row in local_conn.execute(f"DESCRIBE {local_tbl}").fetchall()}
            athena_cur.execute(f"DESCRIBE {target_tbl}")
            athena_schema = {}
            for row in athena_cur.fetchall():
                col_str = row[0]
                if not col_str.strip() or col_str.startswith("#"):
                    continue
                parts = col_str.split("\t")
                col_name = parts[0].strip().lower()
                col_type = parts[1].strip() if len(parts) > 1 else "unknown"
                athena_schema[col_name] = col_type
                
            only_in_local_cols = sorted(list(set(local_schema.keys()) - set(athena_schema.keys())))
            only_in_target_cols = sorted(list(set(athena_schema.keys()) - set(local_schema.keys())))
            
            report_details_md += f"""
### 1.{idx} Schema Comparison: `{table_name}`
* Columns only in local `{local_tbl}`: {", ".join([f"`{c}`" for c in only_in_local_cols]) if only_in_local_cols else "*None*"}
* Columns only in Athena `{target_tbl}`: {", ".join([f"`{c}`" for c in only_in_target_cols]) if only_in_target_cols else "*None*"}
"""
            
        overall_metrics['row_count_pct_diff'] /= len(TABLES_CONFIG)
        
        # 2. Run Threshold Validation Gate
        threshold_results = evaluate_thresholds(overall_metrics)
        
        # 3. Build Markdown Report
        kpi_rows_md = ""
        for r in global_results:
            kpi_rows_md += f"| {r['table']} | *None* | {r['local_count']:,} | {r['target_count']:,} | {r['overlap']:,} | {r['only_local']:,} | {r['only_target']:,} | {r['local_rate']*100:.2f}% | {r['target_rate']*100:.2f}% | {r['status']} |\n"
            
        markdown_content = f"""# Data Lake Reconciliation Report: DuckDB vs AWS Athena (Embryoscope Silver Layer)
    
> [!NOTE]
> ### 📊 Global Reconciliation Dashboard
> * **Total Tables Audited**: **{len(TABLES_CONFIG)}**
> * **Perfect Value Alignment**: **{perfect_count}** tables
> * **Variance / Discrepancy Detected**: **{variance_count}** tables
> * **Audit Execution Failures**: **0** tables
> 
> | Table Name | Columns Missing (Exclude Metadata) | Local Count | Athena Count | Overlap | Local Only | Athena Only | Local Match Rate | Athena Match Rate | Status |
> | :--- | :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :--- |
{kpi_rows_md}
    
This report validates the reconciliation of embryoscope silver layer tables between local DuckDB and AWS Athena.
    
---
    
## 1. Schema Comparison
{report_details_md}
    
---
    
## 2. Row Count & Volume Validation (Full Datasets)
| Table | Local Count | Athena Count | Difference | Pct Diff | Status / Observations |
| :--- | :---: | :---: | :---: | :---: | :--- |
"""
        for r in global_results:
            diff = r['local_count'] - r['target_count']
            pct = (diff / r['target_count'] * 100) if r['target_count'] > 0 else 0.0
            markdown_content += f"| {r['table']} | {r['local_count']:,} | {r['target_count']:,} | {diff:+,} | {pct:+.2f}% | {r['status']} |\n"
            
        markdown_content += """
---
    
## 3. Actionable Recommendations
1. **Pipeline Sync**: Check if the AWS Athena ingest pipelines have run completely up to the latest extraction timestamps.
2. **Standardize Keys**: Ensure downstream integrations use standard lowercased keys for clean mappings.
"""
        
        # 4. Save and archive
        report_path = archive_report(markdown_content, "embryoscope_silver_reconciliation")
        logger.info(f"Comparison report generated successfully: {report_path}")
    finally:
        local_conn.close()
        athena_conn.close()
    
if __name__ == "__main__":
    main()
