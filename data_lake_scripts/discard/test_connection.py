import duckdb
import sys

print("Testing database connection...")
print(f"Python version: {sys.version}")

try:
    db_path = "../database/huntington_data_lake.duckdb"
    print(f"Connecting to: {db_path}")
    
    conn = duckdb.connect(db_path)
    print("✅ Connection successful!")
    
    # Test basic query
    result = conn.execute("SELECT COUNT(*) FROM silver_embryoscope.embryo_data").df()
    print(f"✅ Silver layer has {result.iloc[0,0]} records")
    
    # Test patient 874649
    result2 = conn.execute("SELECT COUNT(*) FROM silver_embryoscope.embryo_data WHERE PatientIDx LIKE '%874649%'").df()
    print(f"✅ Found {result2.iloc[0,0]} records for patient 874649")
    
    if result2.iloc[0,0] > 0:
        # Show sample data
        result3 = conn.execute("""
            SELECT PatientIDx, TreatmentName, EmbryoDescriptionID, embryo_number 
            FROM silver_embryoscope.embryo_data 
            WHERE PatientIDx LIKE '%874649%' 
            LIMIT 5
        """).df()
        print("✅ Sample data:")
        print(result3.to_string(index=False))
    
    conn.close()
    print("✅ Test completed successfully!")
    
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc() 