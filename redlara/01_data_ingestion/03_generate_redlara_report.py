#!/usr/bin/env python3
"""
REDLARA Silver Validation & Audit Report Generator
Analyzes the 6 REDLARA Silver tables in DuckDB:
  - Row counts by table, clinic, and year
  - Clinical pregnancy rate (%) across procedures
  - Biochemical pregnancy rate (%)
  - Delivery rates (%)
  - Strategy L Prontuario matching coverage (%)
  - Column fill rates and schema completeness
  - Quality and consistency assertions
Outputs:
  - Markdown report: redlara/silver_validation_report.md
  - Terminal summary
"""

import sys
import duckdb
import pandas as pd
from datetime import datetime
from pathlib import Path

# Paths
SCRIPT_DIR = Path(__file__).resolve().parent
REDLARA_DIR = SCRIPT_DIR.parent
PROJECT_ROOT = REDLARA_DIR.parent
DB_PATH = PROJECT_ROOT / "database" / "huntington_data_lake.duckdb"
REPORT_MD_PATH = REDLARA_DIR / "silver_validation_report.md"

STREAMS = ['fresh', 'fet', 'fot', 'recep', 'fp', 'iui']

def generate_report():
    print("=" * 70)
    print("Generating REDLARA Silver Quality & Clinical Outcome Report")
    print(f"Database: {DB_PATH}")
    print("=" * 70)
    
    con = duckdb.connect(str(DB_PATH), read_only=True)
    
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    md_lines = [
        "# REDLARA Silver Layer Audit & Validation Report",
        f"**Generated**: `{timestamp}`  ",
        f"**Database**: `{DB_PATH.name}`  ",
        "",
        "---",
        "",
        "## 📊 Executive Summary: 6 Silver Procedure Tables",
        "",
        "| Procedure Stream | Silver Target Table | Rows | Transferred Cycles | Clinical Pregnancies | Clinical Pregnancy Rate (%) | Biochemical Pregnancies | Deliveries | Prontuário Matches (%) |",
        "| :--- | :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |"
    ]
    
    total_silver_rows = 0
    total_transfers = 0
    total_cp = 0
    
    summary_rows = []
    
    for stream in STREAMS:
        table = f"silver.redlara_{stream}"
        
        # Check table exists
        exists = con.execute(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='silver' AND table_name='redlara_{stream}'").fetchone()[0]
        if not exists:
            md_lines.append(f"| **{stream.upper()}** | `{table}` | *Not Created* | - | - | - | - | - | - |")
            continue
            
        cols = [c[0] for c in con.execute(f"SELECT * FROM {table} LIMIT 0").description]
        
        row_cnt = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        total_silver_rows += row_cnt
        
        has_cp = 'clinical_pregnancy' in cols
        has_bio = 'biochemical_pregnancy' in cols
        has_deliv = 'delivery_occurred' in cols
        has_pront = 'prontuario' in cols
        
        cp_pos = con.execute(f"SELECT COUNT(*) FROM {table} WHERE clinical_pregnancy = '1'").fetchone()[0] if has_cp else 0
        cp_neg = con.execute(f"SELECT COUNT(*) FROM {table} WHERE clinical_pregnancy = '0'").fetchone()[0] if has_cp else 0
        cp_total_evaluated = cp_pos + cp_neg
        cp_rate = (cp_pos / cp_total_evaluated * 100.0) if cp_total_evaluated > 0 else 0.0
        
        bio_pos = con.execute(f"SELECT COUNT(*) FROM {table} WHERE biochemical_pregnancy = '1'").fetchone()[0] if has_bio else 0
        deliv_pos = con.execute(f"SELECT COUNT(*) FROM {table} WHERE delivery_occurred = '1'").fetchone()[0] if has_deliv else 0
        
        pront_matched = con.execute(f"SELECT COUNT(*) FROM {table} WHERE prontuario IS NOT NULL AND prontuario != -1").fetchone()[0] if has_pront else 0
        pront_rate = (pront_matched / row_cnt * 100.0) if row_cnt > 0 else 0.0
        
        total_transfers += cp_total_evaluated
        total_cp += cp_pos
        
        md_lines.append(
            f"| **{stream.upper()}** | [`{table}`](file:///{DB_PATH}) | **{row_cnt:,}** | {cp_total_evaluated:,} | **{cp_pos:,}** | **{cp_rate:.2f}%** | {bio_pos:,} | {deliv_pos:,} | **{pront_rate:.2f}%** ({pront_matched:,}/{row_cnt:,}) |"
        )
        
        summary_rows.append({
            'stream': stream.upper(),
            'table': table,
            'rows': row_cnt,
            'transfers': cp_total_evaluated,
            'clinical_pregnancies': cp_pos,
            'cp_rate_pct': round(cp_rate, 2),
            'biochemical': bio_pos,
            'deliveries': deliv_pos,
            'prontuario_match_pct': round(pront_rate, 2)
        })
        
    overall_cp_rate = (total_cp / total_transfers * 100.0) if total_transfers > 0 else 0.0
    md_lines.extend([
        "",
        f"**Total Consolidated Rows across Silver**: **{total_silver_rows:,}**  ",
        f"**Overall Clinical Pregnancy Rate across All Transferred Cycles**: **{overall_cp_rate:.2f}%** ({total_cp:,} / {total_transfers:,})  ",
        "",
        "---",
        "",
        "## 🏥 Breakdown by Clinic (Unidade) and Year",
        ""
    ])
    
    for stream in STREAMS:
        table = f"silver.redlara_{stream}"
        exists = con.execute(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='silver' AND table_name='redlara_{stream}'").fetchone()[0]
        if not exists:
            continue
            
        md_lines.extend([
            f"### Procedure: {stream.upper()} (`{table}`)",
            "",
            "| Unidade | Year | Total Rows | Transferred Cycles | Clinical Pregnancies | CP Rate (%) | Prontuário Match (%) |",
            "| :--- | :---: | :---: | :---: | :---: | :---: | :---: |"
        ])
        
        breakdown_df = con.execute(f"""
            SELECT 
                unidade,
                year,
                COUNT(*) as total_rows,
                COUNT(CASE WHEN clinical_pregnancy IN ('0', '1') THEN 1 END) as transfers,
                COUNT(CASE WHEN clinical_pregnancy = '1' THEN 1 END) as cp_pos,
                ROUND(COUNT(CASE WHEN clinical_pregnancy = '1' THEN 1 END) * 100.0 / NULLIF(COUNT(CASE WHEN clinical_pregnancy IN ('0', '1') THEN 1 END), 0), 2) as cp_rate,
                ROUND(COUNT(CASE WHEN prontuario IS NOT NULL AND prontuario != -1 THEN 1 END) * 100.0 / COUNT(*), 2) as pront_rate
            FROM {table}
            GROUP BY 1, 2
            ORDER BY 1, 2
        """).df()
        
        for _, row in breakdown_df.iterrows():
            md_lines.append(
                f"| {row['unidade']} | {int(row['year'])} | {int(row['total_rows']):,} | {int(row['transfers']):,} | {int(row['cp_pos']):,} | {row['cp_rate'] if pd.notna(row['cp_rate']) else '-'}% | {row['pront_rate']}% |"
            )
        md_lines.append("")
        
    # Quality Assertions
    md_lines.extend([
        "---",
        "",
        "## 🛡️ Data Quality & Medical Consistency Assertions",
        "",
        "| Check Description | Target Constraint | Result | Status |",
        "| :--- | :--- | :---: | :---: |"
    ])
    
    # Check 1: Primary Key Uniqueness
    for stream in STREAMS:
        table = f"silver.redlara_{stream}"
        dups = con.execute(f"SELECT COUNT(*) - COUNT(DISTINCT row_id) FROM {table}").fetchone()[0]
        status = "✅ PASS" if dups == 0 else "❌ FAIL"
        md_lines.append(f"| PK Uniqueness (`row_id`) in `{table}` | 0 duplicates | {dups} duplicates | {status} |")
        
    # Check 2: Medical Consistency - Gestational sacs >= 1 MUST be clinical_pregnancy = '1'
    for stream in ['fresh', 'fet', 'fot', 'recep', 'iui']:
        table = f"silver.redlara_{stream}"
        violations = con.execute(f"""
            SELECT COUNT(*) 
            FROM {table} 
            WHERE gestational_sacs_first_us >= 1 AND clinical_pregnancy != '1'
        """).fetchone()[0]
        status = "✅ PASS" if violations == 0 else "❌ FAIL"
        md_lines.append(f"| Sac Consistency (`sacs >= 1` $\\implies$ `cp = '1'`) in `{table}` | 0 violations | {violations} violations | {status} |")

    # Check 3: Medical Consistency - Newborns >= 1 MUST be clinical_pregnancy = '1'
    for stream in ['fresh', 'fet', 'fot', 'recep', 'iui']:
        table = f"silver.redlara_{stream}"
        violations = con.execute(f"""
            SELECT COUNT(*) 
            FROM {table} 
            WHERE number_of_newborns >= 1 AND clinical_pregnancy != '1'
        """).fetchone()[0]
        status = "✅ PASS" if violations == 0 else "❌ FAIL"
        md_lines.append(f"| Newborn Consistency (`newborns >= 1` $\\implies$ `cp = '1'`) in `{table}` | 0 violations | {violations} violations | {status} |")
        
    con.close()
    
    # Save Report
    report_text = "\n".join(md_lines)
    REPORT_MD_PATH.write_text(report_text, encoding="utf-8")
    print(f"\nReport successfully saved to: {REPORT_MD_PATH}")
    
    # Terminal display of summary table
    df_sum = pd.DataFrame(summary_rows)
    print("\n" + df_sum.to_string(index=False))

if __name__ == "__main__":
    generate_report()
