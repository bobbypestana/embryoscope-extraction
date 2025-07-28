import duckdb

# Connect to database
db_path = "../database/huntington_data_lake.duckdb"
conn = duckdb.connect(db_path)

print("CHECKING EMBRYOSCOPE SILVER TABLE SCHEMA")
print("=" * 50)

# Check column names in embryoscope silver table
query = """
DESCRIBE silver_embryoscope.embryo_data
"""

result = conn.execute(query).df()
print("Embryoscope silver table columns:")
print(result.to_string(index=False))

# Check sample data
query2 = """
SELECT * FROM silver_embryoscope.embryo_data LIMIT 3
"""

result2 = conn.execute(query2).df()
print("\nSample embryoscope silver data:")
print(result2.to_string(index=False))

conn.close() 