import duckdb
import traceback

try:
    print("Testing database connection...")
    db_path = "../database/huntington_data_lake.duckdb"
    conn = duckdb.connect(db_path)
    
    print("Connection successful!")
    
    # Test basic query
    print("Testing basic query...")
    result = conn.execute("SELECT COUNT(*) FROM gold.embryoscope_embrioes").df()
    print(f"Total records in gold.embryoscope_embrioes: {result.iloc[0,0]}")
    
    # Test patient 874649 query
    print("Testing patient 874649 query...")
    query = """
    SELECT COUNT(*) 
    FROM gold.embryoscope_embrioes 
    WHERE patient_PatientID = 874649
    """
    result = conn.execute(query).df()
    print(f"Records for patient 874649: {result.iloc[0,0]}")
    
    if result.iloc[0,0] > 0:
        print("Patient 874649 found! Testing embryo_number...")
        query2 = """
        SELECT embryo_embryo_number
        FROM gold.embryoscope_embrioes 
        WHERE patient_PatientID = 874649
        ORDER BY embryo_embryo_number
        LIMIT 10
        """
        result2 = conn.execute(query2).df()
        print("First 10 embryo numbers:")
        print(result2.to_string(index=False))
    
    conn.close()
    print("Debug completed successfully!")
    
except Exception as e:
    print(f"Error: {e}")
    print("Traceback:")
    traceback.print_exc() 