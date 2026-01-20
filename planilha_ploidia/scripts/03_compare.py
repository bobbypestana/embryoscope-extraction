#!/usr/bin/env python3
"""
Refactored script to compare Excel data with gold.data_ploidia table.

This script demonstrates how to use the new config/ and utils/ modules
for cleaner, more maintainable code.
"""
import sys
import os
import pandas as pd
from datetime import datetime

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.database import DatabaseConfig
from config.language_maps import HELPER_COLUMNS
from utils.db_utils import get_connection
from utils.logging_utils import setup_logger
from utils.normalization import normalize_value, numbers_match

logger = setup_logger('compare')

# Ensure export directory exists
os.makedirs(DatabaseConfig.DATA_EXPORT_DIR, exist_ok=True)


def read_excel_data():
    """Read Excel file and extract Patient ID + Slide ID combinations"""
    logger.info("=" * 80)
    logger.info("READING EXCEL FILE")
    logger.info("=" * 80)
    logger.info(f"Excel file: {DatabaseConfig.EXCEL_FILE}")
    
    # Read Excel
    df = pd.read_excel(DatabaseConfig.EXCEL_FILE, sheet_name="Export")
    logger.info(f"Read {len(df)} rows from Excel")
    
    # Normalize Patient ID
    def normalize_patient_id_excel(x):
        if pd.isna(x):
            return None
        try:
            if isinstance(x, str):
                clean = x.replace('.', '').replace(',', '').strip()
                return int(clean)
            elif isinstance(x, float):
                return int(x * 1000)
            else:
                return int(x)
        except (ValueError, TypeError):
            return None
    
    df['_normalized_patient_id'] = df['Patient ID'].apply(normalize_patient_id_excel)
    df['_excel_slide_id'] = df['Slide ID'].astype(str) + '-' + df['Well'].astype(str)
    df['_filter_key'] = df['_normalized_patient_id'].astype(str) + '_' + df['_excel_slide_id']
    
    filter_keys = df['_filter_key'].dropna().unique().tolist()  # Convert to list
    logger.info(f"Extracted {len(filter_keys)} unique Patient ID + Slide ID combinations")
    
    return df, filter_keys


def filter_database_by_patient_slide(conn, filter_keys):
    """Filter gold.data_ploidia by Patient ID + Slide ID combinations"""
    logger.info("")
    logger.info("=" * 80)
    logger.info("FILTERING DATABASE")
    logger.info("=" * 80)
    
    # Create a temporary table with the filter keys for efficient filtering
    filter_keys_df = pd.DataFrame({'filter_key': filter_keys})
    conn.register('temp_filter_keys', filter_keys_df)
    
    # Query gold.data_ploidia filtered by Patient ID + Slide ID
    # Note: Filter key is Patient ID + '_' + Slide ID (Slide ID is the full embryo ID)
    query = """
    SELECT dp.*,
           CAST(dp."Patient ID" AS VARCHAR) || '_' || dp."Slide ID" as _filter_key
    FROM gold.data_ploidia dp
    WHERE CAST(dp."Patient ID" AS VARCHAR) || '_' || dp."Slide ID" IN (SELECT filter_key FROM temp_filter_keys)
    """
    
    logger.info("Executing filter query...")
    df_database = conn.execute(query).df()
    logger.info(f"Found {len(df_database)} rows in database matching Patient ID + Slide ID combinations")
    
    # Get unique filter keys from database
    db_filter_keys = df_database['_filter_key'].dropna().unique().tolist()
    logger.info(f"Found {len(db_filter_keys)} unique combinations in database")
    
    return df_database, db_filter_keys


def compare_datasets(df_excel, excel_filter_keys, df_database, db_filter_keys):
    """Compare Excel and database datasets"""
    logger.info("")
    logger.info("=" * 80)
    logger.info("COMPARING DATASETS")
    logger.info("=" * 80)
    
    report_lines = []
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Compare filter keys (convert to sets for set operations)
    excel_filter_keys_set = set(excel_filter_keys)
    db_filter_keys_set = set(db_filter_keys)
    only_in_excel = excel_filter_keys_set - db_filter_keys_set
    only_in_db = db_filter_keys_set - excel_filter_keys_set
    
    report_lines.append("PATIENT ID + SLIDE ID COMPARISON")
    report_lines.append("=" * 80)
    report_lines.append(f"In both: {len(excel_filter_keys_set & db_filter_keys_set)}")
    report_lines.append(f"Only in Excel: {len(only_in_excel)}")
    report_lines.append(f"Only in Database: {len(only_in_db)}")
    report_lines.append("")
    
    # Compare columns (exclude helper columns)
    excel_cols = set(df_excel.columns) - HELPER_COLUMNS
    db_cols = set(df_database.columns) - HELPER_COLUMNS
    common_cols = excel_cols & db_cols
    
    report_lines.append("COLUMN COMPARISON")
    report_lines.append("-" * 80)
    report_lines.append(f"Columns in both: {len(common_cols)}")
    report_lines.append(f"Columns only in Excel: {len(excel_cols - db_cols)}")
    report_lines.append(f"Columns only in Database: {len(db_cols - excel_cols)}")
    report_lines.append("")
    
    # Value comparison
    report_lines.append("VALUE COMPARISON")
    report_lines.append("-" * 80)
    
    # Merge datasets
    df_merged = df_excel.merge(
        df_database,
        on='_filter_key',
        suffixes=('_excel', '_db'),
        how='inner'
    )
    
    logger.info(f"Comparing {len(df_merged)} matching rows")
    
    # Compare values
    exclude_cols = {'Video ID', 'Patient Comments'} | HELPER_COLUMNS
    compare_cols = [col for col in common_cols if col not in exclude_cols]
    
    differences = []
    for idx, row in df_merged.iterrows():
        row_diffs = []
        for col in compare_cols:
            excel_val = row.get(f'{col}_excel')
            db_val = row.get(f'{col}_db')
            
            # Normalize
            excel_norm = normalize_value(excel_val, col)
            db_norm = normalize_value(db_val, col)
            
            # Compare
            if pd.isna(excel_norm) and pd.isna(db_norm):
                continue
            elif pd.isna(excel_norm) or pd.isna(db_norm):
                if not (pd.isna(excel_norm) and pd.isna(db_norm)):
                    row_diffs.append(f"{col}: Excel={excel_val}, DB={db_val}")
            elif isinstance(excel_norm, (int, float)) and isinstance(db_norm, (int, float)):
                if not numbers_match(excel_norm, db_norm):
                    row_diffs.append(f"{col}: Excel={excel_val}, DB={db_val}")
            elif excel_norm != db_norm:
                row_diffs.append(f"{col}: Excel={excel_val}, DB={db_val}")
        
        if row_diffs:
            differences.append((row['_filter_key'], row_diffs))
    
    if differences:
        report_lines.append(f"Found {len(differences)} rows with differences:")
        report_lines.append("")
        for filter_key, diffs in differences[:10]:  # Show first 10
            report_lines.append(f"Row: {filter_key}")
            for diff in diffs:
                report_lines.append(f"  - {diff}")
            report_lines.append("")
    else:
        report_lines.append("No differences found!")
    
    # Save report
    report_path = os.path.join(DatabaseConfig.DATA_EXPORT_DIR, f'comparison_report_{timestamp}.txt')
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(report_lines))
    
    logger.info(f"Comparison report saved: {report_path}")
    return report_path


def export_filtered_data(df_database):
    """Export filtered database data to Excel"""
    logger.info("")
    logger.info("=" * 80)
    logger.info("EXPORTING FILTERED DATA")
    logger.info("=" * 80)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    export_path = os.path.join(DatabaseConfig.DATA_EXPORT_DIR, f'filtered_data_ploidia_{timestamp}.xlsx')
    
    # Remove helper columns
    export_df = df_database.drop(columns=['_filter_key'], errors='ignore')
    export_df.to_excel(export_path, index=False)
    
    logger.info(f"Exported {len(export_df)} rows to: {export_path}")
    return export_path


def main():
    """Main function"""
    logger.info("=" * 80)
    logger.info("EXCEL vs DATABASE COMPARISON")
    logger.info("=" * 80)
    logger.info(f"Timestamp: {datetime.now()}")
    logger.info("")
    
    # Read Excel
    df_excel, excel_filter_keys = read_excel_data()
    
    # Filter database
    conn = get_connection(read_only=True)
    try:
        df_database, db_filter_keys = filter_database_by_patient_slide(conn, excel_filter_keys)
    finally:
        conn.close()
    
    # Compare
    report_path = compare_datasets(df_excel, excel_filter_keys, df_database, db_filter_keys)
    
    # Export
    export_path = export_filtered_data(df_database)
    
    logger.info("")
    logger.info("=" * 80)
    logger.info("COMPLETED SUCCESSFULLY")
    logger.info("=" * 80)
    logger.info(f"Comparison report: {report_path}")
    logger.info(f"Exported data: {export_path}")


if __name__ == "__main__":
    main()
