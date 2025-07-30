import duckdb
import pandas as pd

# Connect to database
db = duckdb.connect('../database/clinisys_all.duckdb')

# Primary keys for each table
primary_keys = {
    'view_micromanipulacao': 'codigo_ficha',
    'view_micromanipulacao_oocitos': 'id',
    'view_tratamentos': 'id',
    'view_medicamentos_prescricoes': 'id',
    'view_pacientes': 'codigo',
    'view_medicos': 'id',
    'view_unidades': 'id',
    'view_medicamentos': 'id',
    'view_procedimentos_financas': 'id',
    'view_orcamentos': 'id',
    'view_extrato_atendimentos_central': 'agendamento_id',
    'view_congelamentos_embrioes': 'id',
    'view_congelamentos_ovulos': 'id',
    'view_descongelamentos_embrioes': 'id',
    'view_descongelamentos_ovulos': 'id',
    'view_embrioes_congelados': 'id'
}

print("=== Verifying Deduplication Across All Tables ===")

for table, primary_key in primary_keys.items():
    print(f"\n--- {table} (Primary Key: {primary_key}) ---")
    
    try:
        # Check for duplicates in silver table
        df_silver = db.execute(f"""
            SELECT {primary_key}, COUNT(*) as count
            FROM silver.{table}
            GROUP BY {primary_key}
            HAVING COUNT(*) > 1
            ORDER BY count DESC
            LIMIT 5
        """).df()
        
        if df_silver.empty:
            print(f"✓ No duplicates found in silver.{table}")
        else:
            print(f"✗ Found {len(df_silver)} duplicate primary keys in silver.{table}")
            print(df_silver)
        
        # Check total records
        total_silver = db.execute(f"SELECT COUNT(*) as count FROM silver.{table}").df()['count'].iloc[0]
        total_bronze = db.execute(f"SELECT COUNT(*) as count FROM bronze.{table}").df()['count'].iloc[0]
        
        print(f"  Records: Bronze={total_bronze}, Silver={total_silver}")
        
        # Check if we kept the most recent records
        if total_silver < total_bronze:
            print(f"  ✓ Deduplication reduced records from {total_bronze} to {total_silver}")
        else:
            print(f"  ? No deduplication effect (same count)")
            
    except Exception as e:
        print(f"  Error checking {table}: {e}")

# Special check for the specific case mentioned
print(f"\n=== Special Check: prontuario=160751, codigo_ficha=26549 ===")
df_specific = db.execute("""
    SELECT codigo_ficha, prontuario, extraction_timestamp
    FROM silver.view_micromanipulacao 
    WHERE prontuario = 160751 AND codigo_ficha = 26549
    ORDER BY extraction_timestamp DESC
""").df()

if len(df_specific) == 1:
    print("✓ SUCCESS: Only one record found for codigo_ficha=26549 (most recent kept)")
    print(f"  Kept record: {df_specific.iloc[0]['extraction_timestamp']}")
elif len(df_specific) == 0:
    print("? No records found for codigo_ficha=26549")
else:
    print(f"✗ ERROR: {len(df_specific)} records found for codigo_ficha=26549 (should be 1)")
    print(df_specific)

db.close() 