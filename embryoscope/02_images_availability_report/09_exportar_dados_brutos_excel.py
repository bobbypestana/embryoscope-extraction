
import duckdb
import pandas as pd
from pathlib import Path
from datetime import datetime
import os

# Configuration
DB_PATH = Path('G:/My Drive/projetos_individuais/Huntington/database/huntington_data_lake.duckdb')
OUTPUT_DIR = Path('G:/My Drive/projetos_individuais/Huntington/embryoscope/report/exports')
os.makedirs(OUTPUT_DIR, exist_ok=True)

def main():
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    print("Iniciando exportação de dados brutos (SILVER) para Excel...")
    
    # Connect and Load
    conn = duckdb.connect(str(DB_PATH), read_only=True)
    df = conn.execute("SELECT * FROM silver.embryo_image_availability_latest").df()
    conn.close()
    
    print(f"Dados carregados: {len(df)} linhas.")
    
    # Sort for better readability
    # Checking if columns exist to avoid errors
    sort_cols = []
    if 'patient_unit_huntington' in df.columns: sort_cols.append('patient_unit_huntington')
    if 'embryo_EmbryoDate' in df.columns: sort_cols.append('embryo_EmbryoDate')
    
    if sort_cols:
        df = df.sort_values(sort_cols, ascending=[True, False])

    # Export
    filename = f"dados_brutos_disponibilidade_{timestamp}.xlsx"
    filepath = OUTPUT_DIR / filename
    
    print(f"Salvando arquivo Excel...")
    df.to_excel(filepath, index=False, engine='openpyxl')
    
    print(f"\n✅ Exportação concluída com sucesso!")
    print(f"📍 Localização: {filepath.resolve()}")

if __name__ == "__main__":
    main()
