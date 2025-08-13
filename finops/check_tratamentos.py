import duckdb as db

# Connect to database
conn = db.connect('database/clinisys_all.duckdb', read_only=True)

# Get sample data to see columns
sample = conn.execute("SELECT * FROM silver.view_tratamentos LIMIT 1").fetchdf()
print("Available columns:")
for col in sample.columns:
    print(f"  - {col}")

# Check if data_inicio_inducao exists
if 'data_inicio_inducao' in sample.columns:
    print("\n✅ data_inicio_inducao column found!")
    
    # Get some data with both dates
    data = conn.execute("""
        SELECT prontuario, tentativa, data_procedimento, data_inicio_inducao 
        FROM silver.view_tratamentos 
        WHERE data_procedimento IS NOT NULL AND data_inicio_inducao IS NOT NULL
        LIMIT 10
    """).fetchdf()
    
    print(f"\nFound {len(data)} records with both dates:")
    print(data)
    
else:
    print("\n❌ data_inicio_inducao column not found")
    print("Date-related columns:")
    for col in sample.columns:
        if 'data' in col.lower():
            print(f"  - {col}")

conn.close()

