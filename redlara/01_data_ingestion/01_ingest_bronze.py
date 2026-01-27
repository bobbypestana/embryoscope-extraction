import pandas as pd
import duckdb
from pathlib import Path
import re
import os

# Configuration
INPUT_DIR = Path(r"g:\My Drive\projetos_individuais\Huntington\redlara\data_input")
DB_PATH = Path(r"g:\My Drive\projetos_individuais\Huntington\database\huntington_data_lake.duckdb")
TABLE_NAME = "bronze.redlara_data"

def get_metadata_from_path(file_path):
    """
    Extracts year and unidade.
    Prioritizes filename format: redlara_{unidade}_{year}.xlsx
    Falls back to path structure.
    """
    filename = file_path.name
    
    # Try parsing filename: redlara_unidade_year.xlsx
    # Regex: redlara_(.+)_(\d{4})\.xlsx
    match = re.search(r'redlara_(.+)_(20\d{2})\.xlsx', filename, re.IGNORECASE)
    
    if match:
        unidade_raw = match.group(1)
        year_raw = match.group(2)
        return int(year_raw), unidade_raw.replace('_', ' ').title()

    # Fallback to path logic
    parts = file_path.parts
    year = None
    unidade = None
    
    # Extract Year (looking for 20xx)
    year_match = re.search(r'(20\d{2})', str(file_path))
    if year_match:
        year = int(year_match.group(1))
        
    # Extract Unidade
    known_units = ['Ibirapuera', 'Santa Joana', 'Vila Mariana', 'Campinas']
    
    for part in parts:
        if part in known_units:
            unidade = part
            break
            
    if not unidade:
        parent = file_path.parent.name
        if re.match(r'^\d{4}$', parent):
            unidade = file_path.parent.parent.name
        else:
            unidade = parent
            
    return year, unidade

def ingest_data():
    conn = duckdb.connect(str(DB_PATH))
    conn.execute("CREATE SCHEMA IF NOT EXISTS bronze")
    
    # Cleanup existing tables
    print("Cleaning up existing bronze.redlara_% tables...")
    tables = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='bronze' AND table_name LIKE 'redlara_%'").fetchall()
    for t in tables:
        conn.execute(f"DROP TABLE IF EXISTS bronze.{t[0]}")
    print(f"Dropped {len(tables)} tables.")

    # 1. list all xlsx files
    files = list(INPUT_DIR.rglob("*.xlsx"))
    print(f"Found {len(files)} files.")
    
    for i, file_path in enumerate(files):
        print(f"Processing ({i+1}/{len(files)}): {file_path.name}")
        
        try:
            # 2. Extract metadata
            year, unidade = get_metadata_from_path(file_path)
            print(f"  Metadata - Year: {year}, Unidade: {unidade}")
            
            # 3. Read Excel (find 'FET' sheet)
            try:
                xls = pd.ExcelFile(file_path, engine='openpyxl')
                sheet_names = xls.sheet_names
                
                # Find sheet containing FET (case insensitive)
                fet_sheet = next((s for s in sheet_names if 'FET' in s.upper()), None)
                
                if fet_sheet:
                    print(f"  Found 'FET' sheet: '{fet_sheet}'")
                    
                    # Dynamic Header Detection
                    # Read first 20 rows without header to find the true header row
                    df_preview = pd.read_excel(file_path, sheet_name=fet_sheet, engine='openpyxl', header=None, nrows=20)
                    
                    header_row_idx = 0 # Default
                    keywords = ['outcome', 'date', 'unidade', 'folder', 'chart', 'pin', 'patient']
                    
                    found_header = False
                    for idx, row in df_preview.iterrows():
                        # Convert row to string and check for keywords
                        row_str = " ".join([str(x).lower() for x in row.values])
                        matches = sum(1 for k in keywords if k in row_str)
                        if matches >= 2: # Threshold: at least 2 keywords found
                            header_row_idx = idx
                            print(f"  Detected header at row index: {idx}")
                            found_header = True
                            break
                    
                    if not found_header:
                        print("  Warning: Could not detect header row with keywords. Defaulting to 0.")

                    # Read actual data with detected header
                    df = pd.read_excel(file_path, sheet_name=fet_sheet, engine='openpyxl', header=header_row_idx)
                else:
                    print(f"  Warning: No sheet containing 'FET' found in {file_path.name}. Available: {sheet_names}")
                    continue

            except Exception as e:
                print(f"  Error reading {file_path.name}: {e}")
                continue
            
            # 4. Add metadata
            df['year'] = year
            df['unidade'] = unidade
            
            # Clean column names
            # df.columns = [str(c).lower().strip().replace(' ', '_').replace('/', '_').replace('-', '_') for c in df.columns]
            # User requested original headers
            df.columns = [str(c).strip() for c in df.columns]
            
            # Cast to string
            df = df.astype(str)
            df = df.replace('nan', None)

            # 5. Generate Table Name
            # bronze.redlara_<filename without extension, sanitized>
            safe_name = file_path.stem.lower().strip().replace(' ', '_').replace('.', '_').replace('-', '_')
            
            # Avoid double prefix if filename already starts with redlara_
            if safe_name.startswith('redlara_'):
                table_name = f"bronze.{safe_name}"
            else:
                table_name = f"bronze.redlara_{safe_name}"
            
            print(f"  Writing to table: {table_name}")

            # 6. Write to DuckDB
            conn.register('df_view', df)
            conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df_view")
            conn.unregister('df_view')
            print("  Successfully created table.")
            
        except Exception as e:
            print(f"  Error processing {file_path.name}: {e}")

    conn.close()

if __name__ == "__main__":
    ingest_data()
