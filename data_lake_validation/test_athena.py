import sys
import os
from pyathena import connect

region = "sa-east-1"
workgroup = "datalake-admins"
athena_db = "silver_clinisys_staging"

print("Connecting to AWS Athena...")
try:
    athena_conn = connect(region_name=region, work_group=workgroup)
    cur = athena_conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {athena_db}.view_pacientes")
    count = cur.fetchone()[0]
    print(f"Athena count for view_pacientes: {count}")
    
    cur.execute(f"DESCRIBE {athena_db}.view_pacientes")
    desc = cur.fetchall()
    print("Columns in Athena view_pacientes:")
    for row in desc:
        print(f"  {row[0]}")
except Exception as e:
    print(f"Error: {e}")
