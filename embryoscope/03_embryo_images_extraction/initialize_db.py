import duckdb
import os

db_path = r"g:\My Drive\projetos_individuais\Huntington\database\huntington_data_lake.duckdb"
sql_path = r"g:\My Drive\projetos_individuais\Huntington\embryoscope\02_embryo_images_extraction\01_create_metadata_table.sql"

print(f"Connecting to {db_path}...")
conn = duckdb.connect(db_path)

print(f"Executing {sql_path}...")
with open(sql_path, 'r') as f:
    sql = f.read()
    conn.execute(sql)

print("Done.")
conn.close()
