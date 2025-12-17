#!/usr/bin/env python3
"""
Compare Excel data with gold.data_ploidia table and export filtered results

This script:
1. Reads the "Export" sheet from the Excel file
2. Extracts Video IDs from the "Video ID" column
3. Filters gold.data_ploidia by those Video IDs
4. Compares both datasets and generates a report
5. Exports the filtered data to Excel
"""

import duckdb as db
import pandas as pd
from datetime import datetime
import os
import logging

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
    """Read the Excel file and extract Video IDs"""
    logger.info("=" * 80)
    logger.info("READING EXCEL FILE")
    logger.info("=" * 80)
    logger.info(f"Excel file: {EXCEL_FILE}")
    
    # Read the Export sheet
    df_excel = pd.read_excel(EXCEL_FILE, sheet_name='Export')
    logger.info(f"Read {len(df_excel)} rows from Excel")
    logger.info(f"Columns: {df_excel.columns.tolist()}")
    
    # Extract Video IDs from Video ID column
    if 'Video ID' not in df_excel.columns:
        raise ValueError("'Video ID' column not found in Excel file")
    
    slide_ids = df_excel['Video ID'].dropna().unique().tolist()
    logger.info(f"Found {len(slide_ids)} unique Video IDs in Excel")
    logger.info(f"Sample Video IDs: {slide_ids[:5]}")
    
    return df_excel, slide_ids

def filter_database_by_slide_ids(conn, slide_ids):
    """Filter gold.data_ploidia by Video IDs"""
    logger.info("=" * 80)
    logger.info("FILTERING DATABASE")
    logger.info("=" * 80)
    
    # Create a temporary table with the Video IDs for efficient filtering
    slide_ids_df = pd.DataFrame({'video_id': slide_ids})
    conn.register('temp_video_ids', slide_ids_df)
    
    # Query gold.data_ploidia filtered by Video IDs
    # Note: Video ID is now populated in the database table
    query = """
    SELECT dp.*
    FROM gold.data_ploidia dp
    WHERE dp."Video ID" IN (SELECT video_id FROM temp_video_ids)
    """
    
    logger.info("Executing filter query...")
    df_database = conn.execute(query).df()
    logger.info(f"Found {len(df_database)} rows in database matching Video IDs")
    
    # Get unique Video IDs from database
    db_slide_ids = df_database['Video ID'].dropna().unique().tolist()
    logger.info(f"Found {len(db_slide_ids)} unique Video IDs in database")
    
    return df_database, db_slide_ids

def normalize_value(value, column_name=None):
    """
    Normalize values to ignore language differences, rounding, and type differences
    
    Handles:
    - Fresh/Fresco -> fresh
    - IBI/Ibirapuera -> ibi
    - Autologous/Autólogo -> autologous
    - Case-insensitive string comparison
    - Timing columns (t2, t3, etc.): round to 1 decimal place
    - Numeric type conversion: "0" (string) -> 0 (number)
    - Patient ID: Remove dots and convert to integer (107.805 -> 107805)
    - Video ID: Remove well number suffix (D2024.07.27_S03829_I3027_P-10 -> D2024.07.27_S03829_I3027_P)
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
            import re
            # Pattern: remove "-" followed by digits at the end
            normalized = re.sub(r'-\d+$', '', value)
            return normalized
        return value
    
    
    # Round timing columns and BMI to 1 decimal place
    rounded_columns = ['t2', 't3', 't4', 't5', 't8', 'tB', 'tEB', 'tHB', 'tM', 'tPNa', 'tPNf', 'tSB', 'tSC', 'BMI', 'Age']
    if column_name and column_name in rounded_columns:
        try:
            return round(float(value), 1)
        except (ValueError, TypeError):
            return value
    
    # Try to convert numeric strings to numbers for proper comparison
    # This handles cases like "0" (string) vs 0 (int) or "22" (string) vs 22 (int)
    if isinstance(value, str):
        # First check if it's a numeric string
        try:
            # Try integer conversion first
            if '.' not in value:
                return int(value)
            else:
                return float(value)
        except (ValueError, TypeError):
            # Not a number, continue with string normalization
            pass
        
        # String normalization for language differences
        normalized = value.lower().strip()
        
        # Language mappings
        language_map = {
            'fresco': 'fresh',
            'ibirapuera': 'ibi',
            'autólogo': 'autologous',
            'autologous': 'autologous',
        }
        
        return language_map.get(normalized, normalized)
    
    return value


def compare_datasets(df_excel, excel_slide_ids, df_database, db_slide_ids):
    """Compare Excel and database datasets and generate report"""
    logger.info("=" * 80)
    logger.info("COMPARING DATASETS")
    logger.info("=" * 80)
    
    # Convert to sets for comparison
    excel_ids_set = set(excel_slide_ids)
    db_ids_set = set(db_slide_ids)
    
    # Find differences
    only_in_excel = excel_ids_set - db_ids_set
    only_in_db = db_ids_set - excel_ids_set
    in_both = excel_ids_set & db_ids_set
    
    logger.info(f"Video IDs in both: {len(in_both)}")
    logger.info(f"Video IDs only in Excel: {len(only_in_excel)}")
    logger.info(f"Video IDs only in Database: {len(only_in_db)}")
    
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
    report_lines.append(f"Excel unique Video IDs: {len(excel_slide_ids)}")
    report_lines.append("")
    report_lines.append(f"Database table: gold.data_ploidia")
    report_lines.append(f"Database rows (filtered): {len(df_database)}")
    report_lines.append(f"Database unique Video IDs (filtered): {len(db_slide_ids)}")
    report_lines.append("")
    
    report_lines.append("VIDEO ID COMPARISON")
    report_lines.append("-" * 80)
    report_lines.append(f"Video IDs in both datasets: {len(in_both)}")
    report_lines.append(f"Video IDs only in Excel: {len(only_in_excel)}")
    report_lines.append(f"Video IDs only in Database: {len(only_in_db)}")
    report_lines.append("")
    
    
    if only_in_excel:
        report_lines.append("VIDEO IDs ONLY IN EXCEL (not in database):")
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
        report_lines.append("VIDEO IDs ONLY IN DATABASE (not in Excel):")
        report_lines.append("-" * 80)
        for slide_id in sorted(only_in_db):
            report_lines.append(f"  - {slide_id}")
        report_lines.append("")
    
    # Compare columns
    excel_cols = set(df_excel.columns)
    db_cols = set(df_database.columns)
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
        report_lines.append(f"Comparing all {len(all_ids)} matching Video IDs...")
        report_lines.append("")
        
        for slide_id in all_ids:
            excel_row = df_excel[df_excel['Video ID'] == slide_id].iloc[0]
            db_row = df_database[df_database['Video ID'] == slide_id].iloc[0]
            
            differences = []
            for col in sorted(common_cols):
                excel_val = excel_row[col]
                db_val = db_row[col]
                
                # Normalize values for comparison (pass column name for timing rounding)
                excel_val_norm = normalize_value(excel_val, col)
                db_val_norm = normalize_value(db_val, col)
                
                # Handle NaN comparisons
                if pd.isna(excel_val_norm) and pd.isna(db_val_norm):
                    continue
                elif pd.isna(excel_val_norm) or pd.isna(db_val_norm):
                    differences.append(f"    {col}: Excel={excel_val}, DB={db_val}")
                elif excel_val_norm != db_val_norm:
                    # Only report if normalized values are different
                    differences.append(f"    {col}: Excel={excel_val}, DB={db_val}")
            
            if differences:
                report_lines.append(f"  Video ID: {slide_id}")
                report_lines.extend(differences)
                report_lines.append("")
            else:
                report_lines.append(f"  Video ID: {slide_id} - No differences found")
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
        
        # Step 2: Connect to database and filter by Video IDs
        conn = get_database_connection(read_only=True)
        df_database, db_slide_ids = filter_database_by_slide_ids(conn, excel_slide_ids)
        
        # Step 3: Compare datasets
        report_path = compare_datasets(df_excel, excel_slide_ids, df_database, db_slide_ids)
        
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
