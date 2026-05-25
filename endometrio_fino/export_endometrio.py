import duckdb
import pandas as pd
import os

# Database path
db_path = r"g:\My Drive\projetos_individuais\Huntington\database\clinisys_all.duckdb"
output_file = "endometrio_inadequado_export.xlsx"

# SQL Query
query = """
SELECT 
    vt.*,
    vm.nome as medico,
    vm.tipo_medico,
    vu.nome as unidade_huntigton
FROM silver.view_tratamentos vt 
LEFT JOIN silver.view_medicos vm ON vm.id = vt.responsavel_informacoes 
LEFT JOIN silver.view_unidades  vu ON vu.id = vt.unidade 
WHERE  motivo_cancelamento_hcg = 'Endométrio inadequado'
"""

def main():
    print(f"Connecting to database at {db_path}...")
    if not os.path.exists(db_path):
        print(f"Error: Database file not found at {db_path}")
        return

    try:
        # Use duckdb to fetch data
        con = duckdb.connect(db_path, read_only=True)
        print("Executing query...")
        df = con.execute(query).df()
        con.close()

        print(f"Found {len(df)} records.")
        
        # Export to Excel
        print(f"Exporting to {output_file}...")
        df.to_excel(output_file, index=False)
        print("Export completed successfully.")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
