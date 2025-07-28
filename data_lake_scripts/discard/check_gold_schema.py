import duckdb

# Connect to database
db_path = "../database/huntington_data_lake.duckdb"
conn = duckdb.connect(db_path)

print("CHECKING EMBRYOSCOPE GOLD TABLE SCHEMA")
print("=" * 50)

# Check column names in embryoscope gold table
query = """
DESCRIBE gold.embryoscope_embrioes
"""

result = conn.execute(query).df()
print("Embryoscope gold table columns:")
print(result.to_string(index=False))

# Check sample data
query2 = """
SELECT * FROM gold.embryoscope_embrioes LIMIT 3
"""

result2 = conn.execute(query2).df()
print("\nSample embryoscope gold data:")
print(result2.to_string(index=False))

conn.close() 