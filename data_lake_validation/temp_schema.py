import duckdb
from pyathena import connect

print("DuckDB silver.view_pacientes schema:")
local_conn = duckdb.connect('database/clinisys_all.duckdb', read_only=True)
desc_local = local_conn.execute("DESCRIBE silver.view_pacientes").fetchall()
local_cols = {row[0]: row[1] for row in desc_local}
for col, typ in sorted(local_cols.items()):
    print(f"  {col}: {typ}")

print("\nAthena silver_clinisys_staging.view_pacientes schema:")
athena_conn = connect(region_name="sa-east-1", work_group="datalake-admins")
athena_cur = athena_conn.cursor()
athena_cur.execute("DESCRIBE silver_clinisys_staging.view_pacientes")
athena_desc = athena_cur.fetchall()
athena_cols = {}
for row in athena_desc:
    col_str = row[0]
    if not col_str.strip() or col_str.startswith("#"):
        continue
    parts = col_str.split("\t")
    col_name = parts[0].strip()
    col_type = parts[1].strip() if len(parts) > 1 else "unknown"
    athena_cols[col_name] = col_type

for col, typ in sorted(athena_cols.items()):
    print(f"  {col}: {typ}")

local_conn.close()
athena_conn.close()
