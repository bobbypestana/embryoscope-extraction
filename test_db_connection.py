#!/usr/bin/env python3
"""
Test database connection and check required tables
"""

import duckdb as db
import pandas as pd

def test_connection():
    """Test database connection and basic queries"""
    
    print("Testing database connection...")
    
    try:
        # Connect to database
        path_to_db = 'database/huntington_data_lake.duckdb'
        conn = db.connect(path_to_db)
        print(f"✅ Connected to database: {path_to_db}")
        
        # Check if required tables exist
        tables_query = """
        SELECT table_schema, table_name 
        FROM information_schema.tables 
        WHERE table_schema IN ('gold', 'silver')
        AND table_name IN ('recent_patients_timeline', 'diario_vendas', 'finops_summary')
        ORDER BY table_schema, table_name
        """
        
        tables = conn.execute(tables_query).fetchdf()
        print("\n📋 Available tables:")
        print(tables.to_string(index=False))
        
        # Check sample data from diario_vendas
        sample_billing = conn.execute("""
            SELECT "Descricao", COUNT(*) as count, SUM("Total") as total_amount
            FROM silver.diario_vendas 
            WHERE "Descricao" IN ('FIV - MEDICOS INTERNOS', 'COLETA - DUOSTIM')
            GROUP BY "Descricao"
        """).fetchdf()
        
        print("\n💰 Sample billing data:")
        print(sample_billing.to_string(index=False))
        
        conn.close()
        print("\n✅ Database connection test completed successfully")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        raise

if __name__ == "__main__":
    test_connection()

