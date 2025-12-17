import duckdb
import os

# Connect to database
repo_root = os.path.dirname(os.path.dirname(__file__))
path_to_db = os.path.join(repo_root, 'database', 'huntington_data_lake.duckdb')
conn = duckdb.connect(path_to_db, read_only=True)

# Check Video ID values
result = conn.execute('''
    SELECT "Video ID", "Patient ID", "Well", "Embryo ID" 
    FROM gold.data_ploidia 
    LIMIT 10
''').df()

print("Sample Video IDs from database:")
print(result.to_string())
print(f"\nTotal rows in gold.data_ploidia: {conn.execute('SELECT COUNT(*) FROM gold.data_ploidia').fetchone()[0]}")

conn.close()
