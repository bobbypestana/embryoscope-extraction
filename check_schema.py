import duckdb
import pandas as pd

db_path = 'database/huntington_data_lake.duckdb'
conn = duckdb.connect(db_path)

try:
    schema_df = conn.execute("PRAGMA table_info('gold.planilha_embryoscope_combined');").df()
    target_cols = ['outcome_type', 'fet_gravidez_clinica', 'trat2_resultado_tratamento', 'trat1_id', 'embryo_EmbryoID']
    print(schema_df[schema_df['name'].isin(target_cols)])

except Exception as e:

    print(f"Error: {e}")
finally:
    conn.close()
