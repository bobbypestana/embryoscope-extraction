import os
import duckdb
import pandas as pd
from embryoscope.02_bronze_to_silver import process_database

def test_silver_patients_creation():
    # Setup: create a test DuckDB database
    test_db = 'test_patients.db'
    if os.path.exists(test_db):
        os.remove(test_db)
    con = duckdb.connect(test_db)
    # Create a patients table with a raw_json column
    sample_json = '{"Patients": [{"PatientIDx": "NEXTGEN_43895.4511009838", "PatientID": "123", "FirstName": "katia"}]}'
    df = pd.DataFrame({'raw_json': [sample_json]})
    con.register('df', df)
    con.execute('CREATE TABLE patients AS SELECT * FROM df')
    con.unregister('df')
    con.close()

    # Run the silver layer creation
    process_database(test_db)

    # Check that silver_patients table exists and has expected columns
    con = duckdb.connect(test_db)
    result = con.execute('SELECT * FROM silver_patients').fetchdf()
    assert 'PatientIDx' in result.columns
    assert 'PatientID' in result.columns
    assert 'FirstName' in result.columns
    print('Test passed: silver_patients table created with expected columns.')
    con.close()
    os.remove(test_db)

if __name__ == '__main__':
    test_silver_patients_creation() 