import os
from datetime import datetime
import duckdb
import pandas as pd

DB_PATH = 'database/huntington_data_lake.duckdb'
EXPORT_DIR = 'protheus/data_export'

def main():
    print("Creating export directory if it doesn't exist...")
    os.makedirs(EXPORT_DIR, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    export_file = os.path.join(EXPORT_DIR, f'protheus_mesclada_vendas_{timestamp}.xlsx')
    
    print(f"Connecting to DuckDB: {DB_PATH}...")
    conn = duckdb.connect(DB_PATH)
    
    print("Fetching data from gold.protheus_mesclada_vendas...")
    # Read entire table
    df = conn.execute("SELECT * FROM gold.protheus_mesclada_vendas").df()
    print(f"Loaded {len(df):,} rows.")
    
    print(f"Exporting to Excel file: {export_file}...")
    print("This might take a couple of minutes due to the file size (~600k rows)...")
    
    # Using openpyxl or xlsxwriter. xlsxwriter is usually faster and uses less memory.
    # Let's use xlsxwriter as engine.
    df.to_excel(export_file, index=False, engine='xlsxwriter')
    
    conn.close()
    print("Export completed successfully!")

if __name__ == "__main__":
    main()

