#!/usr/bin/env python3
"""
Test for 03_create_finops_summary.py
Verifies that patient 162173 returns the exact expected results
"""

import duckdb as db
import pandas as pd
import sys
import os
from datetime import datetime

def get_database_connection():
    """Create and return a connection to the huntington_data_lake database"""
    # Get the directory where this script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    # Go up two levels to project root, then into database folder
    path_to_db = os.path.join(script_dir, '..', '..', 'database', 'huntington_data_lake.duckdb')
    
    # Convert to absolute path and normalize
    path_to_db = os.path.abspath(path_to_db)
    
    if not os.path.exists(path_to_db):
        print(f"❌ Database file not found: {path_to_db}")
        raise FileNotFoundError(f"Database file not found: {path_to_db}")
    
    conn = db.connect(path_to_db)
    print(f"✅ Connected to database: {path_to_db}")
    return conn

def test_patient_162173():
    """Test that patient 162173 returns the exact expected results"""
    
    print("🧪 Testing 03_create_finops_summary.py for patient 162173...")
    print(f"Timestamp: {datetime.now()}")
    print()
    
    try:
        # Connect to database
        conn = get_database_connection()
        
        # First, run the finops summary creation
        print("📊 Creating finops_summary table...")
        
        # Drop table if it exists
        conn.execute("DROP TABLE IF EXISTS gold.finops_summary")
        
        # Create the table with FIV cycle data AND billing data
        create_table_query = """
        CREATE TABLE gold.finops_summary AS
        WITH timeline_summary AS (
            SELECT
                prontuario,
                COUNT(CASE
                    WHEN LOWER(COALESCE(resultado_tratamento, '')) NOT IN ('no transfer', '') THEN 1
                END) AS cycle_with_transfer,
                COUNT(CASE
                    WHEN LOWER(COALESCE(resultado_tratamento, '')) IN (
                        'no transfer',
                        'sem transferencia',
                        'sem transferência',
                        'sem transfer',
                        ''
                    ) THEN 1
                END) AS cycle_without_transfer,
                MIN(event_date) AS timeline_first_date,
                MAX(event_date) AS timeline_last_date
            FROM gold.recent_patients_timeline
            WHERE reference_value IN ('Ciclo a Fresco FIV', 'Ciclo de Congelados')
                AND flag_date_estimated = FALSE
            GROUP BY prontuario
        ),
        billing_summary AS (
            SELECT
                prontuario,
                -- FIV Initial billing
                COUNT(CASE WHEN "Cta-Ctbl" = '31110100001 - RECEITA DE FIV' THEN 1 END) AS fiv_initial_paid_count,
                SUM(CASE WHEN "Cta-Ctbl" = '31110100001 - RECEITA DE FIV' THEN "Total" ELSE 0 END) AS fiv_initial_paid_total,
                -- FIV Extra billing
                COUNT(CASE WHEN "Descricao" = 'COLETA - DUOSTIM' THEN 1 END) AS fiv_extra_paid_count,
                SUM(CASE WHEN "Descricao" = 'COLETA - DUOSTIM' THEN "Total" ELSE 0 END) AS fiv_extra_paid_total,
                -- Date ranges for billing
                MIN(CASE WHEN "Cta-Ctbl" = '31110100001 - RECEITA DE FIV' OR "Descricao" = 'COLETA - DUOSTIM' THEN "DT Emissao" END) AS billing_first_date,
                MAX(CASE WHEN "Cta-Ctbl" = '31110100001 - RECEITA DE FIV' OR "Descricao" = 'COLETA - DUOSTIM' THEN "DT Emissao" END) AS billing_last_date
            FROM silver.diario_vendas
            WHERE prontuario IS NOT NULL
            GROUP BY prontuario
        )
        SELECT 
            COALESCE(t.prontuario, b.prontuario) AS prontuario,
            COALESCE(t.cycle_with_transfer, 0) AS cycle_with_transfer,
            COALESCE(t.cycle_without_transfer, 0) AS cycle_without_transfer,
            COALESCE(t.cycle_with_transfer, 0) + COALESCE(t.cycle_without_transfer, 0) AS cycle_total,
            COALESCE(b.fiv_initial_paid_count, 0) AS fiv_initial_paid_count,
            COALESCE(b.fiv_initial_paid_total, 0) AS fiv_initial_paid_total,
            COALESCE(b.fiv_extra_paid_count, 0) AS fiv_extra_paid_count,
            COALESCE(b.fiv_extra_paid_total, 0) AS fiv_extra_paid_total,
            -- Combined totals
            COALESCE(b.fiv_initial_paid_count, 0) + COALESCE(b.fiv_extra_paid_count, 0) AS fiv_paid_count,
            COALESCE(b.fiv_initial_paid_total, 0) + COALESCE(b.fiv_extra_paid_total, 0) AS fiv_paid_total,
            -- Date ranges
            t.timeline_first_date,
            t.timeline_last_date,
            b.billing_first_date,
            b.billing_last_date
        FROM timeline_summary t
        FULL OUTER JOIN billing_summary b ON t.prontuario = b.prontuario
        ORDER BY COALESCE(t.prontuario, b.prontuario)
        """
        
        conn.execute(create_table_query)
        print("✅ Finops summary table created successfully")
        
        # Query the result for patient 162173
        print("\n🔍 Querying finops_summary for patient 162173...")
        result = conn.execute("""
            SELECT *
            FROM gold.finops_summary
            WHERE prontuario = 162173
        """).fetchdf()
        
        print("\n📋 Actual result for patient 162173:")
        print(result.to_string(index=False))
        
        # Define expected result
        expected_data = {
            'prontuario': [162173.0],
            'cycle_with_transfer': [1],
            'cycle_without_transfer': [4],
            'cycle_total': [5],
            'fiv_initial_paid_count': [4],
            'fiv_initial_paid_total': [75093.5],
            'fiv_extra_paid_count': [1],
            'fiv_extra_paid_total': [10355.0],
            'fiv_paid_count': [5],
            'fiv_paid_total': [85448.5],
            'timeline_first_date': ['2023-08-15'],
            'timeline_last_date': ['2025-02-21'],
            'billing_first_date': ['2023-08-12'],
            'billing_last_date': ['2024-10-02']
        }
        
        expected_df = pd.DataFrame(expected_data)
        
        print("\n🎯 Expected result for patient 162173:")
        print(expected_df.to_string(index=False))
        
        # Compare results
        print("\n🔍 Comparing actual vs expected results...")
        
        if result.empty:
            print("❌ ERROR: No data found for patient 162173")
            return False
        
        if len(result) != 1:
            print(f"❌ ERROR: Expected 1 row, got {len(result)} rows")
            return False
        
        # Convert date columns to string for comparison
        actual_row = result.iloc[0].copy()
        expected_row = expected_df.iloc[0]
        
        # Convert date columns to string format for comparison
        date_columns = ['timeline_first_date', 'timeline_last_date', 'billing_first_date', 'billing_last_date']
        for col in date_columns:
            if col in actual_row and actual_row[col] is not None:
                # Extract just the date part (YYYY-MM-DD) from timestamp
                date_str = str(actual_row[col])
                if ' ' in date_str:
                    actual_row[col] = date_str.split(' ')[0]
                else:
                    actual_row[col] = date_str
        
        # Compare each column
        all_match = True
        for col in expected_df.columns:
            actual_val = actual_row[col]
            expected_val = expected_row[col]
            
            # Handle float comparison for numeric values
            if isinstance(expected_val, (int, float)) and isinstance(actual_val, (int, float)):
                if abs(actual_val - expected_val) > 0.01:  # Allow small floating point differences
                    print(f"❌ MISMATCH in {col}: expected {expected_val}, got {actual_val}")
                    all_match = False
                else:
                    print(f"✅ {col}: {actual_val} (matches expected {expected_val})")
            else:
                if actual_val != expected_val:
                    print(f"❌ MISMATCH in {col}: expected '{expected_val}', got '{actual_val}'")
                    all_match = False
                else:
                    print(f"✅ {col}: '{actual_val}' (matches expected)")
        
        if all_match:
            print("\n🎉 SUCCESS: All values match expected results for patient 162173!")
            return True
        else:
            print("\n❌ FAILURE: Some values do not match expected results for patient 162173")
            return False
            
    except Exception as e:
        print(f"❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        if 'conn' in locals():
            conn.close()

def test_patient_825890():
    """Test that patient 825890 returns the exact expected results"""
    
    print("🧪 Testing 03_create_finops_summary.py for patient 825890...")
    print(f"Timestamp: {datetime.now()}")
    print()
    
    try:
        # Connect to database
        conn = get_database_connection()
        
        # Query the result for patient 825890
        print("\n🔍 Querying finops_summary for patient 825890...")
        result = conn.execute("""
            SELECT *
            FROM gold.finops_summary
            WHERE prontuario = 825890
        """).fetchdf()
        
        print("\n📋 Actual result for patient 825890:")
        print(result.to_string(index=False))
        
        # Define expected result
        expected_data = {
            'prontuario': [825890.0],
            'cycle_with_transfer': [0],
            'cycle_without_transfer': [1],
            'cycle_total': [1],
            'fiv_initial_paid_count': [1],
            'fiv_initial_paid_total': [24700.0],
            'fiv_extra_paid_count': [0],
            'fiv_extra_paid_total': [0.0],
            'fiv_paid_count': [1],
            'fiv_paid_total': [24700.0],
            'timeline_first_date': ['2024-01-24'],
            'timeline_last_date': ['2024-01-24'],
            'billing_first_date': ['2023-12-23'],
            'billing_last_date': ['2023-12-23']
        }
        
        expected_df = pd.DataFrame(expected_data)
        
        print("\n🎯 Expected result for patient 825890:")
        print(expected_df.to_string(index=False))
        
        # Compare results
        print("\n🔍 Comparing actual vs expected results...")
        
        if result.empty:
            print("❌ ERROR: No data found for patient 825890")
            return False
        
        if len(result) != 1:
            print(f"❌ ERROR: Expected 1 row, got {len(result)} rows")
            return False
        
        # Convert date columns to string for comparison
        actual_row = result.iloc[0].copy()
        expected_row = expected_df.iloc[0]
        
        # Convert date columns to string format for comparison
        date_columns = ['timeline_first_date', 'timeline_last_date', 'billing_first_date', 'billing_last_date']
        for col in date_columns:
            if col in actual_row and actual_row[col] is not None:
                # Extract just the date part (YYYY-MM-DD) from timestamp
                date_str = str(actual_row[col])
                if ' ' in date_str:
                    actual_row[col] = date_str.split(' ')[0]
                else:
                    actual_row[col] = date_str
        
        # Compare each column
        all_match = True
        for col in expected_df.columns:
            actual_val = actual_row[col]
            expected_val = expected_row[col]
            
            # Handle float comparison for numeric values
            if isinstance(expected_val, (int, float)) and isinstance(actual_val, (int, float)):
                if abs(actual_val - expected_val) > 0.01:  # Allow small floating point differences
                    print(f"❌ MISMATCH in {col}: expected {expected_val}, got {actual_val}")
                    all_match = False
                else:
                    print(f"✅ {col}: {actual_val} (matches expected {expected_val})")
            else:
                if actual_val != expected_val:
                    print(f"❌ MISMATCH in {col}: expected '{expected_val}', got '{actual_val}'")
                    all_match = False
                else:
                    print(f"✅ {col}: '{actual_val}' (matches expected)")
        
        if all_match:
            print("\n🎉 SUCCESS: All values match expected results for patient 825890!")
            return True
        else:
            print("\n❌ FAILURE: Some values do not match expected results for patient 825890")
            return False
            
    except Exception as e:
        print(f"❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        if 'conn' in locals():
            conn.close()

def test_patient_175583():
    """Test that patient 175583 returns the exact expected results"""
    
    print("🧪 Testing 03_create_finops_summary.py for patient 175583...")
    print(f"Timestamp: {datetime.now()}")
    print()
    
    try:
        # Connect to database
        conn = get_database_connection()
        
        # Query the result for patient 175583
        print("\n🔍 Querying finops_summary for patient 175583...")
        result = conn.execute("""
            SELECT *
            FROM gold.finops_summary
            WHERE prontuario = 175583
        """).fetchdf()
        
        print("\n📋 Actual result for patient 175583:")
        print(result.to_string(index=False))
        
        # Define expected result
        expected_data = {
            'prontuario': [175583.0],
            'cycle_with_transfer': [0],
            'cycle_without_transfer': [2],
            'cycle_total': [2],
            'fiv_initial_paid_count': [1],
            'fiv_initial_paid_total': [28830.0],
            'fiv_extra_paid_count': [1],
            'fiv_extra_paid_total': [13049.0],
            'fiv_paid_count': [2],
            'fiv_paid_total': [41879.0],
            'timeline_first_date': ['2024-10-26'],
            'timeline_last_date': ['2024-11-14'],
            'billing_first_date': ['2024-10-26'],
            'billing_last_date': ['2024-11-13']
        }
        
        expected_df = pd.DataFrame(expected_data)
        
        print("\n🎯 Expected result for patient 175583:")
        print(expected_df.to_string(index=False))
        
        # Compare results
        print("\n🔍 Comparing actual vs expected results...")
        
        if result.empty:
            print("❌ ERROR: No data found for patient 175583")
            return False
        
        if len(result) != 1:
            print(f"❌ ERROR: Expected 1 row, got {len(result)} rows")
            return False
        
        # Convert date columns to string for comparison
        actual_row = result.iloc[0].copy()
        expected_row = expected_df.iloc[0]
        
        # Convert date columns to string format for comparison
        date_columns = ['timeline_first_date', 'timeline_last_date', 'billing_first_date', 'billing_last_date']
        for col in date_columns:
            if col in actual_row and actual_row[col] is not None:
                # Extract just the date part (YYYY-MM-DD) from timestamp
                date_str = str(actual_row[col])
                if ' ' in date_str:
                    actual_row[col] = date_str.split(' ')[0]
                else:
                    actual_row[col] = date_str
        
        # Compare each column
        all_match = True
        for col in expected_df.columns:
            actual_val = actual_row[col]
            expected_val = expected_row[col]
            
            # Handle float comparison for numeric values
            if isinstance(expected_val, (int, float)) and isinstance(actual_val, (int, float)):
                if abs(actual_val - expected_val) > 0.01:  # Allow small floating point differences
                    print(f"❌ MISMATCH in {col}: expected {expected_val}, got {actual_val}")
                    all_match = False
                else:
                    print(f"✅ {col}: {actual_val} (matches expected {expected_val})")
            else:
                if actual_val != expected_val:
                    print(f"❌ MISMATCH in {col}: expected '{expected_val}', got '{actual_val}'")
                    all_match = False
                else:
                    print(f"✅ {col}: '{actual_val}' (matches expected)")
        
        if all_match:
            print("\n🎉 SUCCESS: All values match expected results for patient 175583!")
            return True
        else:
            print("\n❌ FAILURE: Some values do not match expected results for patient 175583")
            return False
            
    except Exception as e:
        print(f"❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        if 'conn' in locals():
            conn.close()

def test_table_structure():
    """Test that the finops_summary table has the correct structure"""
    
    print("\n🔧 Testing finops_summary table structure...")
    
    try:
        conn = get_database_connection()
        
        # Check table exists
        table_exists = conn.execute("""
            SELECT COUNT(*) as count
            FROM information_schema.tables 
            WHERE table_schema = 'gold' AND table_name = 'finops_summary'
        """).fetchdf()
        
        if table_exists['count'].iloc[0] == 0:
            print("❌ ERROR: gold.finops_summary table does not exist")
            return False
        
        # Check column structure
        columns = conn.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns 
            WHERE table_schema = 'gold' AND table_name = 'finops_summary'
            ORDER BY ordinal_position
        """).fetchdf()
        
        print("📋 Table structure:")
        print(columns.to_string(index=False))
        
        expected_columns = [
            'prontuario', 'cycle_with_transfer', 'cycle_without_transfer', 'cycle_total',
            'fiv_initial_paid_count', 'fiv_initial_paid_total', 'fiv_extra_paid_count', 'fiv_extra_paid_total',
            'fiv_paid_count', 'fiv_paid_total', 'timeline_first_date', 'timeline_last_date',
            'billing_first_date', 'billing_last_date'
        ]
        
        actual_columns = columns['column_name'].tolist()
        
        if actual_columns == expected_columns:
            print("✅ Table structure is correct")
            return True
        else:
            print(f"❌ Table structure mismatch. Expected: {expected_columns}")
            print(f"   Got: {actual_columns}")
            return False
            
    except Exception as e:
        print(f"❌ ERROR: {e}")
        return False
    finally:
        if 'conn' in locals():
            conn.close()

def main():
    """Run all tests"""
    
    print("=" * 60)
    print("🧪 TESTING 03_create_finops_summary.py")
    print("=" * 60)
    
    # Test table structure
    structure_ok = test_table_structure()
    
    # Test patient 162173
    patient_162173_ok = test_patient_162173()
    
    # Test patient 825890
    patient_825890_ok = test_patient_825890()
    
    # Test patient 175583
    patient_175583_ok = test_patient_175583()
    
    print("\n" + "=" * 60)
    print("📊 TEST RESULTS SUMMARY")
    print("=" * 60)
    print(f"Table structure test: {'✅ PASSED' if structure_ok else '❌ FAILED'}")
    print(f"Patient 162173 test: {'✅ PASSED' if patient_162173_ok else '❌ FAILED'}")
    print(f"Patient 825890 test: {'✅ PASSED' if patient_825890_ok else '❌ FAILED'}")
    print(f"Patient 175583 test: {'✅ PASSED' if patient_175583_ok else '❌ FAILED'}")
    
    if structure_ok and patient_162173_ok and patient_825890_ok and patient_175583_ok:
        print("\n🎉 ALL TESTS PASSED!")
        sys.exit(0)
    else:
        print("\n❌ SOME TESTS FAILED!")
        sys.exit(1)

if __name__ == "__main__":
    main()
