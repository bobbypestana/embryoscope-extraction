#!/usr/bin/env python3
"""
Compare Excel data with gold.data_ploidia table and export filtered results

This script:
1. Reads the "Export" sheet from the Excel file
2. Extracts Patient ID + Slide ID combinations for filtering
3. Filters gold.data_ploidia by those combinations
4. Compares both datasets and generates a report (excluding Video ID which differs by construction)
5. Exports the filtered data to Excel
"""

import duckdb as db
import pandas as pd
from datetime import datetime
import os
import logging
import re

# Setup logging
LOGS_DIR = os.path.join(os.path.dirname(__file__), 'logs')
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

# File paths
EXCEL_FILE = os.path.join(
    os.path.dirname(__file__), 
    'planilhas_exemplo', 
    'Planilha IA ploidia - 344 embrioes (1).xlsx'
)
DATA_EXPORT_DIR = os.path.join(os.path.dirname(__file__), 'data_export')
os.makedirs(DATA_EXPORT_DIR, exist_ok=True)

def get_database_connection(read_only=True):
    """Create and return a connection to the huntington_data_lake database"""
    repo_root = os.path.dirname(os.path.dirname(__file__))
    path_to_db = os.path.join(repo_root, 'database', 'huntington_data_lake.duckdb')
    conn = db.connect(path_to_db, read_only=read_only)
    logger.info(f"Connected to database: {path_to_db} (read_only={read_only})")
    return conn

def read_excel_data():
    """Read the Excel file and extract Patient ID + Slide ID combinations"""
    logger.info("=" * 80)
    logger.info("READING EXCEL FILE")
    logger.info("=" * 80)
    logger.info(f"Excel file: {EXCEL_FILE}")
    
    # Read the Export sheet
    df_excel = pd.read_excel(EXCEL_FILE, sheet_name='Export')
    logger.info(f"Read {len(df_excel)} rows from Excel")
    logger.info(f"Columns: {df_excel.columns.tolist()}")
    
    # Check required columns
    required_cols = ['Patient ID', 'Slide ID', 'Well']
    missing_cols = [col for col in required_cols if col not in df_excel.columns]
    if missing_cols:
        raise ValueError(f"Required columns missing in Excel file: {missing_cols}")
    
    # Normalize Patient ID: Excel has different formats
    # - Comma format: '825,960' -> 825960
    # - Decimal format: 154.562 -> 154562 (multiply by 1000)
    def normalize_patient_id(x):
        if pd.isna(x):
            return None
        x_str = str(x)
        # Remove commas
        x_str = x_str.replace(',', '')
        # Convert to float then int
        x_float = float(x_str)
        # If it's a small decimal (< 1000), it's in format 154.562, multiply by 1000
        if x_float < 1000:
            return int(x_float * 1000)
        else:
            return int(x_float)
    
    df_excel['_normalized_patient_id'] = df_excel['Patient ID'].apply(normalize_patient_id)
    
    # Create Excel Slide ID by concatenating Slide ID + Well (e.g., "D2025.11.23_S03920_I3253_P" + "-1" = "D2025.11.23_S03920_I3253_P-1")
    df_excel['_excel_slide_id'] = df_excel['Slide ID'].astype(str) + '-' + df_excel['Well'].astype(str)
    
    # Create filter keys (Normalized Patient ID + Excel Slide ID)
    df_excel['_filter_key'] = df_excel['_normalized_patient_id'].astype(str) + '_' + df_excel['_excel_slide_id']
    filter_keys = df_excel['_filter_key'].dropna().unique().tolist()
    logger.info(f"Found {len(filter_keys)} unique Patient ID + Slide ID combinations in Excel")
    logger.info(f"Sample combinations: {filter_keys[:5]}")
    
    return df_excel, filter_keys

def filter_database_by_patient_slide(conn, filter_keys):
    """Filter gold.data_ploidia by Patient ID + Slide ID combinations"""
    logger.info("=" * 80)
    logger.info("FILTERING DATABASE")
    logger.info("=" * 80)
    
    # Create a temporary table with the filter keys for efficient filtering
    filter_keys_df = pd.DataFrame({'filter_key': filter_keys})
    conn.register('temp_filter_keys', filter_keys_df)
    
    # Query gold.data_ploidia filtered by Patient ID + Slide ID
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

def normalize_value(value, column_name=None):
    """
    Normalize values to ignore language differences, rounding, and type differences
    
    Handles:
    - All numeric values: round to 1 decimal place
    - Language differences for Diagnosis and Oocyte Source
    - Case-insensitive string comparison
    - Patient ID: Remove dots and convert to integer
    - Video ID: Remove well number suffix
    """
    if pd.isna(value):
        return value
    
    # Special handling for Patient ID: remove dots/commas and convert to integer
    if column_name == "Patient ID":
        try:
            # Handle different formats:
            # - String with comma: '825,960' -> 825960
            # - Float with decimal: 107.805 -> 107805 (decimal represents thousands)
            if isinstance(value, str):
                # Remove both '.' and ',' which can be used as thousands separators
                clean_value = value.replace('.', '').replace(',', '').strip()
                return int(clean_value)
            elif isinstance(value, float):
                # For floats like 107.805, multiply by 1000 to get 107805
                # This handles the case where Excel stores it as a decimal number
                return int(value * 1000)
            else:
                return int(value)
        except (ValueError, TypeError):
            return value
    
    # Special handling for Video ID: remove well number suffix (e.g., -10, -2)
    # Remove well number suffix from Video ID and Slide ID columns
    if column_name in ["Video ID", "Slide ID"]:
        if isinstance(value, str):
            # Remove the well number suffix (everything after the last hyphen followed by digits)
            # Pattern: remove "-" followed by digits at the end
            normalized = re.sub(r'-\d+$', '', value)
            return normalized
        return value
    
    # Try to convert to numeric and truncate to 1 decimal place
    # This handles ALL numeric columns uniformly
    # Using truncation instead of rounding to avoid false differences
    # e.g., 22.86 and 22.8 both truncate to 22.8
    try:
        numeric_value = float(value)
        # Truncate to 1 decimal place: multiply by 10, floor, divide by 10
        import math
        return math.floor(numeric_value * 10) / 10
    except (ValueError, TypeError):
        # Not a number, continue with string normalization
        pass
    
    # Normalize string: lowercase, strip whitespace, normalize spacing
    if isinstance(value, str):
        normalized = value.lower().strip()
        # Normalize spacing around commas and slashes
        normalized = re.sub(r'\s*,\s*', ', ', normalized)
        normalized = re.sub(r'\s*/\s*', '/', normalized)
        
        # Comprehensive language mappings
        language_map = {
            # Oocyte Source mappings
            'fresco': 'fresh',
            'fresh': 'fresh',
            'autólogo': 'autologous',
            'autologous': 'autologous',
            'homólogo': 'autologous',  # Portuguese for autologous
            'homologous': 'autologous',
            
            # Unit mappings
            'ibirapuera': 'ibi',
            'ibi': 'ibi',
            
            # Diagnosis mappings
            'fator masculino': 'male factor',
            'male factor': 'male factor',
            'fator feminino': 'female factor',
            'female factor': 'female factor',
            'fator misto': 'mixed factor',
            'mixed factor': 'mixed factor',
            'idiopático': 'idiopathic',
            'idiopathic': 'idiopathic',
            'outros': 'other',
            'other': 'other',
            'endometriose': 'endometriosis',
            'endometriosis': 'endometriosis',
            'inexplicado': 'unknown',
            'unknown': 'unknown',
        }
        
        return language_map.get(normalized, normalized)
    
    return value


def numbers_match(val1, val2):
    """
    Check if two numbers match when either truncated or rounded to 1 decimal place.
    Returns True if they match via either method.
    """
    try:
        num1 = float(val1)
        num2 = float(val2)
        
        import math
        
        # Truncate to 1 decimal
        trunc1 = math.floor(num1 * 10) / 10
        trunc2 = math.floor(num2 * 10) / 10
        
        # Round to 1 decimal
        round1 = round(num1, 1)
        round2 = round(num2, 1)
        
        # Match if either truncated or rounded values are equal
        return (trunc1 == trunc2) or (round1 == round2)
    except (ValueError, TypeError):
        return False




def compare_datasets(df_excel, excel_filter_keys, df_database, db_filter_keys):
    """Compare Excel and database datasets and generate report"""
    logger.info("=" * 80)
    logger.info("COMPARING DATASETS")
    logger.info("=" * 80)
    
    # Convert to sets for comparison
    excel_ids_set = set(excel_filter_keys)
    db_ids_set = set(db_filter_keys)
    
    # Find differences
    only_in_excel = excel_ids_set - db_ids_set
    only_in_db = db_ids_set - excel_ids_set
    in_both = excel_ids_set & db_ids_set
    
    logger.info(f"Patient ID + Slide ID combinations in both: {len(in_both)}")
    logger.info(f"Combinations only in Excel: {len(only_in_excel)}")
    logger.info(f"Combinations only in Database: {len(only_in_db)}")
    
    # Generate detailed comparison report
    report_lines = []
    report_lines.append("=" * 80)
    report_lines.append("DATA COMPARISON REPORT")
    report_lines.append("=" * 80)
    report_lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report_lines.append("")
    
    report_lines.append("SUMMARY")
    report_lines.append("-" * 80)
    report_lines.append(f"Excel file: {EXCEL_FILE}")
    report_lines.append(f"Excel rows: {len(df_excel)}")
    report_lines.append(f"Excel unique Patient ID + Slide ID combinations: {len(excel_filter_keys)}")
    report_lines.append("")
    report_lines.append(f"Database table: gold.data_ploidia")
    report_lines.append(f"Database rows (filtered): {len(df_database)}")
    report_lines.append(f"Database unique combinations (filtered): {len(db_filter_keys)}")
    report_lines.append("")
    
    report_lines.append("PATIENT ID + SLIDE ID COMPARISON")
    report_lines.append("-" * 80)
    report_lines.append(f"Combinations in both datasets: {len(in_both)}")
    report_lines.append(f"Combinations only in Excel: {len(only_in_excel)}")
    report_lines.append(f"Combinations only in Database: {len(only_in_db)}")
    report_lines.append("")
    
    
    if only_in_excel:
        report_lines.append("PATIENT ID + SLIDE ID COMBINATIONS ONLY IN EXCEL (not in database):")
        report_lines.append("-" * 80)
        
        # Extract prontuarios (Patient IDs) from the Video IDs
        # Video ID format is "PRONTUARIO - EMBRYO_ID"
        prontuario_groups = {}
        for slide_id in only_in_excel:
            # Split by " - " to get prontuario and embryo ID
            parts = slide_id.split(' - ')
            if len(parts) == 2:
                prontuario = parts[0]
                embryo_id = parts[1]
                if prontuario not in prontuario_groups:
                    prontuario_groups[prontuario] = []
                prontuario_groups[prontuario].append(embryo_id)
        
        # Summary by prontuario
        report_lines.append(f"Total missing Video IDs: {len(only_in_excel)}")
        report_lines.append(f"Unique Prontuarios (Patient IDs): {len(prontuario_groups)}")
        report_lines.append("")
        report_lines.append("Breakdown by Prontuario:")
        report_lines.append("")
        
        for prontuario in sorted(prontuario_groups.keys()):
            embryo_ids = prontuario_groups[prontuario]
            report_lines.append(f"  Prontuario {prontuario}: {len(embryo_ids)} embryos")
            for embryo_id in sorted(embryo_ids):
                report_lines.append(f"    - {prontuario} - {embryo_id}")
            report_lines.append("")
        
        # Also list all missing slide IDs in simple format
        report_lines.append("Complete list of missing Video IDs:")
        for slide_id in sorted(only_in_excel):
            report_lines.append(f"  - {slide_id}")
        report_lines.append("")
    
    if only_in_db:
        report_lines.append("PATIENT ID + SLIDE ID COMBINATIONS ONLY IN DATABASE (not in Excel):")
        report_lines.append("-" * 80)
        for slide_id in sorted(only_in_db):
            report_lines.append(f"  - {slide_id}")
        report_lines.append("")
    
    # Compare columns (exclude internal helper columns)
    helper_columns = {'_excel_slide_id', '_normalized_patient_id', '_filter_key'}
    excel_cols = set(df_excel.columns) - helper_columns
    db_cols = set(df_database.columns) - helper_columns
    common_cols = excel_cols & db_cols
    only_in_excel_cols = excel_cols - db_cols
    only_in_db_cols = db_cols - excel_cols
    
    report_lines.append("COLUMN COMPARISON")
    report_lines.append("-" * 80)
    report_lines.append(f"Columns in both: {len(common_cols)}")
    report_lines.append(f"Columns only in Excel: {len(only_in_excel_cols)}")
    report_lines.append(f"Columns only in Database: {len(only_in_db_cols)}")
    report_lines.append("")
    
    if only_in_excel_cols:
        report_lines.append("COLUMNS ONLY IN EXCEL:")
        for col in sorted(only_in_excel_cols):
            report_lines.append(f"  - {col}")
        report_lines.append("")
    
    if only_in_db_cols:
        report_lines.append("COLUMNS ONLY IN DATABASE:")
        for col in sorted(only_in_db_cols):
            report_lines.append(f"  - {col}")
        report_lines.append("")
    
    # Compare data for matching Video IDs
    if in_both and common_cols:
        report_lines.append("DATA VALUE COMPARISON (for matching Video IDs)")
        report_lines.append("-" * 80)
        report_lines.append("Note: Language differences are ignored (Fresh=Fresco, IBI=Ibirapuera, etc.)")
        report_lines.append("      Timing values, BMI, and Age are rounded to 1 decimal place before comparison")
        report_lines.append("      Numeric type differences are ignored (e.g., '0' string vs 0 integer)")
        report_lines.append("      Patient ID: thousands separators removed (dots/commas) and converted to integer")
        report_lines.append("      Video ID: well number suffix removed (e.g., -10, -2)")
        report_lines.append("")
        
        # Compare all matching Video IDs
        all_ids = sorted(list(in_both))
        
        # Track combinations with differences
        combinations_with_differences = 0
        
        # First pass: count differences
        differences_list = []
        for slide_id in all_ids:
            excel_row = df_excel[df_excel['_filter_key'] == slide_id].iloc[0]
            db_row = df_database[df_database['_filter_key'] == slide_id].iloc[0]
            
            differences = []
            for col in sorted(common_cols):
                excel_val = excel_row[col]
                db_val = db_row[col]
                
                # Skip Video ID comparison (always different by construction)
                if col == "Video ID":
                    continue
                
                # Skip Patient Comments (not relevant for comparison)
                if col == "Patient Comments":
                    continue
                
                # Handle NaN comparisons first (before normalization)
                if pd.isna(excel_val) and pd.isna(db_val):
                    continue
                elif pd.isna(excel_val) or pd.isna(db_val):
                    differences.append(f"    {col}: Excel={excel_val}, DB={db_val}")
                    continue
                
                # Special handling for Patient ID (has dots/commas that need normalization)
                if col == "Patient ID":
                    excel_val_norm = normalize_value(excel_val, col)
                    db_val_norm = normalize_value(db_val, col)
                    if excel_val_norm != db_val_norm:
                        differences.append(f"    {col}: Excel={excel_val}, DB={db_val}")
                    continue
                
                # For numeric values, check if they match via truncation or rounding using ORIGINAL values
                try:
                    # Try to treat as numbers
                    excel_num = float(excel_val)
                    db_num = float(db_val)
                    # If both are numeric, use numbers_match on original values
                    if numbers_match(excel_num, db_num):
                        continue  # Numbers match, skip
                    else:
                        differences.append(f"    {col}: Excel={excel_val}, DB={db_val}")
                except (ValueError, TypeError):
                    # Not numeric, use normalized string comparison
                    excel_val_norm = normalize_value(excel_val, col)
                    db_val_norm = normalize_value(db_val, col)
                    
                    if excel_val_norm != db_val_norm:
                        differences.append(f"    {col}: Excel={excel_val}, DB={db_val}")
            
            if differences:
                combinations_with_differences += 1
                differences_list.append((slide_id, differences))
        
        # Show summary first
        report_lines.append(f"Comparing all {len(all_ids)} matching Patient ID + Slide ID combinations...")
        report_lines.append(f"Combinations with differences: {combinations_with_differences}")
        report_lines.append(f"Combinations with no differences: {len(all_ids) - combinations_with_differences}")
        report_lines.append("")
        
        # Only show combinations with differences
        if combinations_with_differences > 0:
            report_lines.append("COMBINATIONS WITH DIFFERENCES:")
            report_lines.append("-" * 80)
            for slide_id, differences in differences_list:
                report_lines.append(f"  Patient ID + Slide ID: {slide_id}")
                report_lines.extend(differences)
                report_lines.append("")
        
    
    report_lines.append("=" * 80)
    report_lines.append("END OF REPORT")
    report_lines.append("=" * 80)
    
    # Save report to file
    report_path = os.path.join(DATA_EXPORT_DIR, f'comparison_report_{timestamp}.txt')
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(report_lines))
    
    logger.info(f"Comparison report saved to: {report_path}")
    
    # Print report to console
    for line in report_lines:
        logger.info(line)
    
    return report_path

def export_filtered_data(df_database):
    """Export filtered database data to Excel"""
    logger.info("=" * 80)
    logger.info("EXPORTING FILTERED DATA")
    logger.info("=" * 80)
    
    export_path = os.path.join(DATA_EXPORT_DIR, f'filtered_data_ploidia_{timestamp}.xlsx')
    df_database.to_excel(export_path, index=False, sheet_name='Filtered Data')
    logger.info(f"Exported {len(df_database)} rows to: {export_path}")
    
    return export_path

def main():
    """Main function"""
    logger.info("=" * 80)
    logger.info("EXCEL vs DATABASE COMPARISON")
    logger.info("=" * 80)
    logger.info(f"Timestamp: {datetime.now()}")
    logger.info("")
    
    try:
        # Step 1: Read Excel data
        df_excel, excel_slide_ids = read_excel_data()
        
        # Step 2: Connect to database and filter by Patient ID + Slide ID
        conn = get_database_connection(read_only=True)
        df_database, db_filter_keys = filter_database_by_patient_slide(conn, excel_slide_ids)
        
        # Step 3: Compare datasets
        report_path = compare_datasets(df_excel, excel_slide_ids, df_database, db_filter_keys)
        
        # Step 4: Export filtered data
        export_path = export_filtered_data(df_database)
        
        logger.info("")
        logger.info("=" * 80)
        logger.info("COMPLETED SUCCESSFULLY")
        logger.info("=" * 80)
        logger.info(f"Comparison report: {report_path}")
        logger.info(f"Exported data: {export_path}")
        
    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()
