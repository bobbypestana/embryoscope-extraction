import os
import sys
import argparse
import logging
import re
import pandas as pd
import duckdb

# Add current directory to path to import hooks
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from validation_hooks import (
    pre_run_schema_safeguard,
    scrub_pii,
    archive_report,
    evaluate_thresholds,
    post_jira_comment
)

logger = logging.getLogger("generic_comparator")

def normalize_key(val):
    if val is None or pd.isna(val):
        return None
    val_str = str(val).strip().lower()
    # Remove decimals if integer-like float
    if val_str.endswith(".0"):
        val_str = val_str[:-2]
    # Strip leading zeros for numeric strings (e.g. "030101" -> "30101")
    if val_str.isdigit():
        try:
            val_str = str(int(val_str))
        except ValueError:
            pass
    # Strip trailing time from midnight timestamps for date alignment
    if " 00:00:00" in val_str:
        val_str = val_str.replace(" 00:00:00", "")
    return val_str

def get_columns_and_types(conn, table_name):
    cursor = conn.execute(f"SELECT * FROM {table_name} LIMIT 0")
    return {desc[0].lower(): (desc[0], str(desc[1])) for desc in cursor.description}

def run_comparison(local_db, local_table, keys, value_cols=None, 
                   athena_db=None, athena_table=None, target_local_table=None, 
                   target_local_db=None, target_keys=None, target_value_cols=None,
                   output_name="reconciliation", jira_ticket=None):
    
    logger.info(f"Starting comparison: {local_table} vs target")
    
    # 1. Connect to databases
    logger.info(f"Connecting to local DuckDB: {local_db}")
    local_conn = duckdb.connect(local_db, read_only=True)
    
    athena_conn = None
    athena_cur = None
    
    is_athena = athena_db is not None and athena_table is not None
    target_table_full = target_local_table
    
    if is_athena:
        logger.info(f"Connecting to AWS Athena...")
        from pyathena import connect
        region = "sa-east-1"
        workgroup = "datalake-admins"
        athena_conn = connect(region_name=region, work_group=workgroup)
        athena_cur = athena_conn.cursor()
        target_table_full = f"{athena_db}.{athena_table}"
        target_conn = None
    else:
        if target_local_db:
            logger.info(f"Connecting to target local DuckDB: {target_local_db}")
            target_conn = duckdb.connect(target_local_db, read_only=True)
        else:
            target_conn = local_conn
    
    # 2. Pre-run safeguard check
    logger.info("Executing pre-run safeguards...")
    if is_athena:
        try:
            local_conn.execute(f"SELECT 1 FROM {local_table} LIMIT 1")
        except Exception as e:
            raise ValueError(f"Local table '{local_table}' does not exist: {e}")
    else:
        # Full DuckDB schema safeguard
        pre_run_schema_safeguard(local_conn, local_table, target_local_table, keys, conn_b=target_conn, join_keys_b=target_keys)
        
    # 3. Schema Comparison
    logger.info("Comparing schemas...")
    local_schema = get_columns_and_types(local_conn, local_table)
    
    if is_athena:
        athena_cur.execute(f"DESCRIBE {target_table_full}")
        athena_desc = athena_cur.fetchall()
        target_schema = {}
        for row in athena_desc:
            col_str = row[0]
            if not col_str.strip() or col_str.startswith("#"):
                continue
            parts = col_str.split("\t")
            col_name = parts[0].strip().lower()
            col_type = parts[1].strip() if len(parts) > 1 else "unknown"
            target_schema[col_name] = (parts[0].strip(), col_type)
    else:
        target_schema = get_columns_and_types(target_conn, target_local_table)
        
    local_cols_set = set(local_schema.keys())
    target_cols_set = set(target_schema.keys())
    
    only_in_local = sorted([local_schema[c][0] for c in (local_cols_set - target_cols_set)])
    only_in_target = sorted([target_schema[c][0] for c in (target_cols_set - local_cols_set)])
    
    # 4. Row Counts
    logger.info("Auditing row counts...")
    local_count = local_conn.execute(f"SELECT COUNT(*) FROM {local_table}").fetchone()[0]
    
    if is_athena:
        athena_cur.execute(f"SELECT COUNT(*) FROM {target_table_full}")
        target_count = athena_cur.fetchone()[0]
    else:
        target_count = target_conn.execute(f"SELECT COUNT(*) FROM {target_local_table}").fetchone()[0]
        
    row_diff = local_count - target_count
    row_pct_diff = (row_diff / target_count) if target_count > 0 else 0.0
    
    # 5. Overlap & Key Reconciliation
    logger.info("Extracting data for key reconciliation...")
    # Setup key and value lists for local and target sides
    local_keys = list(keys)
    target_keys = list(target_keys) if target_keys else list(keys)
    
    local_value_cols = list(value_cols) if value_cols else []
    target_value_cols = list(target_value_cols) if target_value_cols else local_value_cols
    
    # Select columns
    local_select_cols = list(dict.fromkeys(local_keys + local_value_cols))
    target_select_cols = list(dict.fromkeys(target_keys + target_value_cols))
    
    local_select_sql = ", ".join([f'"{c}"' for c in local_select_cols])
    target_select_sql = ", ".join([f'"{c}"' for c in target_select_cols])
    
    local_data_df = local_conn.execute(f"SELECT {local_select_sql} FROM {local_table}").df()
    
    if is_athena:
        athena_cur.execute(f"SELECT {target_select_sql} FROM {target_table_full}")
        target_data_df = pd.DataFrame(athena_cur.fetchall(), columns=target_select_cols)
    else:
        target_data_df = target_conn.execute(f"SELECT {target_select_sql} FROM {target_local_table}").df()
        
    # Standardize column casing
    local_data_df.columns = [c.lower() for c in local_data_df.columns]
    target_data_df.columns = [c.lower() for c in target_data_df.columns]
    
    local_keys_lower = [k.lower() for k in local_keys]
    target_keys_lower = [tk.lower() for tk in target_keys]
    
    local_value_cols_lower = [v.lower() for v in local_value_cols]
    target_value_cols_lower = [tv.lower() for tv in target_value_cols]
    
    # Create composite key hashes/strings for comparison
    local_data_df["_comp_key"] = local_data_df.apply(
        lambda r: "||".join([normalize_key(r[k]) if normalize_key(r[k]) is not None else "NULL" for k in local_keys_lower]),
        axis=1
    )
    target_data_df["_comp_key"] = target_data_df.apply(
        lambda r: "||".join([normalize_key(r[k]) if normalize_key(r[k]) is not None else "NULL" for k in target_keys_lower]),
        axis=1
    )
    
    local_keys_set = set(local_data_df["_comp_key"])
    target_keys_set = set(target_data_df["_comp_key"])
    
    overlap_keys = local_keys_set.intersection(target_keys_set)
    local_only_keys = local_keys_set - target_keys_set
    target_only_keys = target_keys_set - local_keys_set
    
    overlap_count = len(overlap_keys)
    local_only_count = len(local_only_keys)
    target_only_count = len(target_only_keys)
    
    key_match_rate_local = (overlap_count / len(local_keys_set)) if local_keys_set else 1.0
    key_match_rate_target = (overlap_count / len(target_keys_set)) if target_keys_set else 1.0
    
    # 6. Statistical Value Check on Overlapping Data
    logger.info("Computing value metrics for overlap...")
    value_metrics = []
    if local_value_cols_lower:
        local_overlap = local_data_df[local_data_df["_comp_key"].isin(overlap_keys)]
        target_overlap = target_data_df[target_data_df["_comp_key"].isin(overlap_keys)]
        
        for idx, vc in enumerate(local_value_cols_lower):
            t_vc = target_value_cols_lower[idx]
            sum_local = pd.to_numeric(local_overlap[vc], errors='coerce').sum()
            sum_target = pd.to_numeric(target_overlap[t_vc], errors='coerce').sum()
            
            abs_diff = abs(sum_local - sum_target)
            alignment = (1.0 - (abs_diff / sum_target)) if sum_target > 0 else 1.0
            
            value_metrics.append({
                "column": vc,
                "target_column": t_vc,
                "sum_local": sum_local,
                "sum_target": sum_target,
                "difference": sum_local - sum_target,
                "alignment_rate": alignment
            })
            
    # 7. Collect Mismatch Examples (Anonymized)
    logger.info("Collecting mismatch samples...")
    local_sample_cols = [c.lower() for c in local_select_cols]
    target_sample_cols = [c.lower() for c in target_select_cols]
    
    local_only_samples = []
    if local_only_count > 0:
        raw_samples = local_data_df[local_data_df["_comp_key"].isin(local_only_keys)].head(15)[local_sample_cols]
        local_only_samples = raw_samples.to_dict(orient="records")
        
    target_only_samples = []
    if target_only_count > 0:
        raw_samples = target_data_df[target_data_df["_comp_key"].isin(target_only_keys)].head(15)[target_sample_cols]
        target_only_samples = raw_samples.to_dict(orient="records")
        
    # Close connections
    local_conn.close()
    if target_local_db:
        target_conn.close()
    if athena_conn:
        athena_conn.close()
        
    # 8. Run Threshold quality check
    logger.info("Evaluating quality gates...")
    # Calculate representative metric values
    metrics_eval = {
        'client_match_rate': key_match_rate_local,
        'patient_match_rate': key_match_rate_target,
        'row_count_pct_diff': row_pct_diff
    }
    if value_metrics:
        # Use first value column's alignment rate as financial rate
        metrics_eval['financial_alignment_rate'] = value_metrics[0]['alignment_rate']
        
    threshold_results = evaluate_thresholds(metrics_eval)
    
    # 9. Format Markdown Report
    logger.info("Generating markdown report...")
    source_name = local_table
    target_name = f"Athena {athena_db}.{athena_table}" if is_athena else target_local_table
    
    # KPI block values
    val_align_str = "N/A"
    if value_metrics:
        val_align_str = f"{value_metrics[0]['alignment_rate']*100:.3f}%"
        
    kpi_block = f"""> [!NOTE]
> ### 📊 Data Lake Ingestion & Promotion Quality KPI Dashboard
> * **Row Count Match Rate**: **{(1.0 - abs(row_pct_diff))*100:.3f}%** ({local_count:,} Local vs {target_count:,} Target - **{row_diff:+,} difference**)
> * **Local Key Overlap Rate**: **{key_match_rate_local*100:.3f}%**
> * **Target Key Overlap Rate**: **{key_match_rate_target*100:.3f}%**
> * **Overall Value Alignment**: **{val_align_str}**
"""

    report_md = f"""# Data Lake Reconciliation Report: {source_name} vs. {target_name}

{kpi_block}

This report presents a generalized reconciliation audit between **{source_name}** and **{target_name}** using automated keys verification.

---

## 1. Schema Comparison
* Local columns count: **{len(local_schema)}**
* Target columns count: **{len(target_schema)}**

### 1.1 Columns Only in Local
{", ".join([f"`{c}`" for c in only_in_local]) if only_in_local else "*None*"}

### 1.2 Columns Only in Target
{", ".join([f"`{c}`" for c in only_in_target]) if only_in_target else "*None*"}

---

## 2. Row Count & Volume Validation (Full Datasets)

| Table | Local Count | Target Count | Difference | Pct Diff | Status |
| :--- | :---: | :---: | :---: | :---: | :--- |
| **{local_table.split('.')[-1]}** | {local_count:,} | {target_count:,} | {row_diff:+,} | {row_pct_diff*100:+.2f}% | {"Perfect Match" if row_diff == 0 else "Variance Detected"} |

---

## 3. Key Overlap Summary (Joined on {", ".join(keys)})

* **Total Overlapping Keys**: **{overlap_count:,}**
* **Keys ONLY in Local**: **{local_only_count:,}**
* **Keys ONLY in Target**: **{target_only_count:,}**

"""

    if value_metrics:
        report_md += "### 3.1 Overlap Value Comparison\n"
        report_md += "| Column Name | Local Sum | Target Sum | Difference | Alignment Rate |\n"
        report_md += "| :--- | :---: | :---: | :---: | :---: |\n"
        for vm in value_metrics:
            report_md += f"| **{vm['column']}** | {vm['sum_local']:.2f} | {vm['sum_target']:.2f} | {vm['difference']:+.2f} | {vm['alignment_rate']*100:.4f}% |\n"
        report_md += "\n"

    # Formatting mismatch samples (anonymizing names/numbers)
    def render_sample_table(samples):
        if not samples:
            return "*No mismatch examples found.*\n"
        # Columns header
        cols = list(samples[0].keys())
        header = "| " + " | ".join([c.title() for c in cols]) + " |\n"
        separator = "| " + " | ".join([":---:" for _ in cols]) + " |\n"
        rows_str = ""
        for s in samples:
            vals = []
            for col in cols:
                val = s[col]
                # Scrub PII in string values
                if isinstance(val, str):
                    val = scrub_pii(val)
                vals.append(str(val))
            rows_str += "| " + " | ".join(vals) + " |\n"
        return header + separator + rows_str + "\n"

    report_md += "---\n\n## 4. Key Takeaways & Root Cause Analysis\n\n"
    
    if local_only_count > 0:
        report_md += f"### 4.1 Sample Records Only in Local ({local_only_count:,} total)\n"
        report_md += render_sample_table(local_only_samples)
        
    if target_only_count > 0:
        report_md += f"### 4.2 Sample Records Only in Target ({target_only_count:,} total)\n"
        report_md += render_sample_table(target_only_samples)
        
    report_md += f"""
---

## 5. Actionable Recommendations
1. **Deduplicate / Trace Gaps**: Inspect missing records in either local or remote target databases using the sample keys above.
2. **Handle Schema Drift**: If column mismatches exist, update downstream mapping logic or execute database alter statements.
"""

    # 10. Archive Report
    logger.info("Archiving report...")
    report_path = archive_report(report_md, output_name)
    print(f"\nReport archived to: {report_path}")
    
    # 11. Post Jira updates
    if jira_ticket:
        logger.info(f"Syncing comment with Jira ticket {jira_ticket}...")
        post_jira_comment(jira_ticket, metrics_eval, threshold_results, report_path)
        
    return report_path

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generic Data Lake Table Reconciliation Engine")
    parser.add_argument("--local-db", default="database/huntington_data_lake.duckdb", help="Local DuckDB path")
    parser.add_argument("--local-table", required=True, help="Local table name, e.g. gold.table")
    parser.add_argument("--keys", required=True, help="Comma-separated join key columns")
    parser.add_argument("--target-keys", help="Comma-separated target join key columns (if different from local)")
    parser.add_argument("--value-cols", help="Comma-separated numeric value columns to verify")
    parser.add_argument("--target-value-cols", help="Comma-separated target value columns (if different from local)")
    parser.add_argument("--athena-db", help="Athena Database name (if comparing to Athena)")
    parser.add_argument("--athena-table", help="Athena Table name (if comparing to Athena)")
    parser.add_argument("--target-local-table", help="Target table name in local DB (if not comparing to Athena)")
    parser.add_argument("--target-local-db", help="Target local DuckDB path (if different from local-db)")
    parser.add_argument("--output-name", default="reconciliation", help="Base name for the output report")
    parser.add_argument("--jira-ticket", help="Jira ticket ID to post updates to")
    
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    
    # Parse keys and value columns lists
    keys_list = [k.strip() for k in args.keys.split(",")]
    target_keys_list = [tk.strip() for tk in args.target_keys.split(",")] if args.target_keys else None
    
    value_cols_list = [v.strip() for v in args.value_cols.split(",")] if args.value_cols else None
    target_value_cols_list = [tv.strip() for tv in args.target_value_cols.split(",")] if args.target_value_cols else None
    
    run_comparison(
        local_db=args.local_db,
        local_table=args.local_table,
        keys=keys_list,
        value_cols=value_cols_list,
        athena_db=args.athena_db,
        athena_table=args.athena_table,
        target_local_table=args.target_local_table,
        target_local_db=args.target_local_db,
        target_keys=target_keys_list,
        target_value_cols=target_value_cols_list,
        output_name=args.output_name,
        jira_ticket=args.jira_ticket
    )
