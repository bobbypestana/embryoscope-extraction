#!/usr/bin/env python3
"""
04_export_gold_to_excel.py
==========================
Exports the three Gold layer tables from the Huntington Data Lake to styled Excel (.xlsx) files:
1. gold.protheus_pedidos_a_faturar      -> protheus_pedidos_a_faturar.xlsx
2. gold.protheus_vendas_consolidadas    -> protheus_vendas_consolidadas.xlsx
3. gold.protheus_notas_faturadas        -> protheus_notas_faturadas.xlsx

Features:
- High-throughput streaming via DuckDB fetchmany() and xlsxwriter in constant_memory mode.
- Local staging in temp directory to avoid Google Drive virtual filesystem latency during writes.
- Automatic human-readable date/time formatting.
- Preservation of leading zeros and alphanumeric identifiers (stored as VARCHAR).
- Styled headers with autofilters enabled across all data columns.
"""

import sys
import os
import time
import shutil
import tempfile
import logging
import yaml
import duckdb
import xlsxwriter
from datetime import datetime

# Setup logging
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LOGS_DIR = os.path.join(SCRIPT_DIR, 'logs')
os.makedirs(LOGS_DIR, exist_ok=True)
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
script_name = os.path.splitext(os.path.basename(__file__))[0]
LOG_PATH = os.path.join(LOGS_DIR, f'{script_name}_{timestamp}.log')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.FileHandler(LOG_PATH, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load parameters
PARAMS_PATH = os.path.join(SCRIPT_DIR, 'params.yml')
with open(PARAMS_PATH, 'r') as f:
    config = yaml.safe_load(f)

DUCKDB_PATH = config['duckdb_path']
PROTHEUS_DIR = os.path.dirname(SCRIPT_DIR)
EXPORT_DIR = os.path.join(PROTHEUS_DIR, 'data_export')
os.makedirs(EXPORT_DIR, exist_ok=True)

TABLES_TO_EXPORT = [
    {
        'table_name': 'gold.protheus_pedidos_a_faturar',
        'sheet_name': 'pedidos_a_faturar',
        'output_filename': 'protheus_pedidos_a_faturar.xlsx',
    },
    {
        'table_name': 'gold.protheus_vendas_consolidadas',
        'sheet_name': 'vendas_consolidadas',
        'output_filename': 'protheus_vendas_consolidadas.xlsx',
    },
    {
        'table_name': 'gold.protheus_notas_faturadas',
        'sheet_name': 'notas_faturadas',
        'output_filename': 'protheus_notas_faturadas.xlsx',
    },
]


def export_table_to_excel(con, table_info):
    table_name = table_info['table_name']
    sheet_name = table_info['sheet_name']
    output_filename = table_info['output_filename']
    final_path = os.path.join(EXPORT_DIR, output_filename)

    logger.info(f"--- Exporting {table_name} -> {output_filename} ---")
    t_start = time.time()

    # Get total row count
    total_rows = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    logger.info(f"Total rows in {table_name}: {total_rows:,}")

    # Use a local temporary file for high-speed writes
    temp_dir = tempfile.gettempdir()
    local_temp_file = os.path.join(temp_dir, f"_export_{output_filename}")
    if os.path.exists(local_temp_file):
        os.remove(local_temp_file)

    workbook = xlsxwriter.Workbook(
        local_temp_file,
        {
            'constant_memory': True,
            'default_date_format': 'yyyy-mm-dd hh:mm:ss',
        }
    )

    header_format = workbook.add_format({
        'bold': True,
        'bg_color': '#D9E1F2',
        'border': 1,
        'border_color': '#D3D3D3',
    })

    worksheet = workbook.add_worksheet(sheet_name)

    # Query table with streaming cursor
    cur = con.cursor()
    cur.execute(f"SELECT * FROM {table_name}")
    col_names = [desc[0] for desc in cur.description]
    num_cols = len(col_names)

    # Write header
    worksheet.write_row(0, 0, col_names, header_format)

    # Stream batches
    batch_size = 25000
    row_idx = 1
    last_log_time = time.time()

    while True:
        batch = cur.fetchmany(batch_size)
        if not batch:
            break
        for row in batch:
            worksheet.write_row(row_idx, 0, row)
            row_idx += 1

        # Periodic log
        if (row_idx - 1) % 100000 < batch_size or time.time() - last_log_time >= 15:
            pct = (row_idx - 1) / total_rows * 100 if total_rows else 100
            logger.info(f"  Progress: {row_idx - 1:,}/{total_rows:,} rows written ({pct:.1f}%)...")
            last_log_time = time.time()

    # Enable Autofilter
    if total_rows > 0:
        worksheet.autofilter(0, 0, total_rows, num_cols - 1)

    # Close workbook to flush all data
    logger.info("  Closing and compressing workbook...")
    workbook.close()
    cur.close()

    # Copy to destination in Google Drive
    file_size_mb = os.path.getsize(local_temp_file) / (1024 * 1024)
    logger.info(f"  Local file built ({file_size_mb:.2f} MB). Transferring to: {final_path}...")
    shutil.copy2(local_temp_file, final_path)
    os.remove(local_temp_file)

    elapsed = time.time() - t_start
    logger.info(f"Successfully exported {table_name}: {row_idx - 1:,} rows | Size: {file_size_mb:.2f} MB | Time: {elapsed:.1f}s")
    return {
        'table': table_name,
        'filename': output_filename,
        'rows': row_idx - 1,
        'size_mb': round(file_size_mb, 2),
        'elapsed_s': round(elapsed, 1),
        'destination': final_path,
    }


def main():
    logger.info("=== STARTING EXPORT OF GOLD TABLES TO EXCEL ===")
    logger.info(f"Source Database: {DUCKDB_PATH}")
    logger.info(f"Destination Folder: {EXPORT_DIR}")

    results = []
    t_total_start = time.time()

    try:
        with duckdb.connect(DUCKDB_PATH, read_only=True) as con:
            for tbl in TABLES_TO_EXPORT:
                res = export_table_to_excel(con, tbl)
                results.append(res)
    except Exception as e:
        logger.error(f"Excel Export Failed: {e}", exc_info=True)
        raise

    total_time = time.time() - t_total_start
    logger.info("=== EXPORT FINISHED SUCCESSFULLY ===")
    logger.info(f"Total time elapsed: {total_time:.1f}s")
    for r in results:
        logger.info(f"  - {r['filename']}: {r['rows']:,} rows | {r['size_mb']} MB in {r['elapsed_s']}s")


if __name__ == "__main__":
    main()
