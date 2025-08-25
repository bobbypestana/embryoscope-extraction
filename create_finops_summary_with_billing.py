#!/usr/bin/env python3
"""
Create gold.finops_summary table with patient-level FIV cycle analysis AND billing data
"""

import duckdb as db
import pandas as pd
from datetime import datetime

def get_database_connection():
    """Create and return a connection to the huntington_data_lake database"""
    path_to_db = 'database/huntington_data_lake.duckdb'
    conn = db.connect(path_to_db)
    
    print(f"Connected to database: {path_to_db}")
    return conn

def create_finops_summary_with_billing_table(conn):
    """Create the gold.finops_summary table with billing data"""
    
    print("Creating gold.finops_summary table with billing data...")
    
    # Drop table if it exists
    conn.execute("DROP TABLE IF EXISTS gold.finops_summary")
    
    # Create the table with FIV cycle data AND billing data
    create_table_query = """
    CREATE TABLE gold.finops_summary AS
    WITH fiv_events AS (
        SELECT
            prontuario,
            -- Extract possible JSON keys
            COALESCE(
                json_extract_string(additional_info, '$."ResultadoTratamento"'),
                json_extract_string(additional_info, '$."Resultado Tratamento"'),
                json_extract_string(additional_info, '$."Resultado do Tratamento"')
            ) AS resultado_raw
        FROM gold.recent_patients_timeline
        WHERE reference_value = 'Ciclo a Fresco FIV'
            AND flag_date_estimated = FALSE
    ),
    normalized AS (
        SELECT
            prontuario,
            -- normalize: trim spaces, lower, collapse multiple spaces
            LOWER(
                regexp_replace(
                    COALESCE(resultado_raw, ''),
                    '\\s+',
                    ' '
                )
            ) AS resultado_norm
        FROM fiv_events
    ),
    timeline_summary AS (
        SELECT
            prontuario,
            COUNT(CASE
                WHEN resultado_norm NOT IN ('no transfer', '') THEN 1
            END) AS cycle_with_transfer,
            COUNT(CASE
                WHEN resultado_norm IN (
                    'no transfer',
                    'sem transferencia',
                    'sem transferência',
                    'sem transfer',
                    ''
                ) THEN 1
            END) AS cycle_without_transfer
        FROM normalized
        GROUP BY prontuario
    ),
    billing_summary AS (
        SELECT
            prontuario,
            -- FIV Initial billing
            COUNT(CASE WHEN "Descricao" = 'FIV - MEDICOS INTERNOS' THEN 1 END) AS fiv_initial_paid_count,
            SUM(CASE WHEN "Descricao" = 'FIV - MEDICOS INTERNOS' THEN "Total" ELSE 0 END) AS fiv_initial_paid_total,
            -- FIV Extra billing
            COUNT(CASE WHEN "Descricao" = 'COLETA - DUOSTIM' THEN 1 END) AS fiv_extra_paid_count,
            SUM(CASE WHEN "Descricao" = 'COLETA - DUOSTIM' THEN "Total" ELSE 0 END) AS fiv_extra_paid_total
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
        COALESCE(b.fiv_initial_paid_total, 0) + COALESCE(b.fiv_extra_paid_total, 0) AS fiv_paid_total
    FROM timeline_summary t
    FULL OUTER JOIN billing_summary b ON t.prontuario = b.prontuario
    ORDER BY COALESCE(t.prontuario, b.prontuario)
    """
    
    conn.execute(create_table_query)
    
    # Verify the table was created
    table_stats = conn.execute("""
        SELECT 
            COUNT(*) as total_patients,
            SUM(cycle_with_transfer) as total_cycles_with_transfer,
            SUM(cycle_without_transfer) as total_cycles_without_transfer,
            SUM(fiv_initial_paid_count) as total_fiv_initial_paid_count,
            SUM(fiv_initial_paid_total) as total_fiv_initial_paid_total,
            SUM(fiv_extra_paid_count) as total_fiv_extra_paid_count,
            SUM(fiv_extra_paid_total) as total_fiv_extra_paid_total,
            SUM(fiv_paid_count) as total_fiv_paid_count,
            SUM(fiv_paid_total) as total_fiv_paid_total,
            COUNT(CASE WHEN cycle_with_transfer > 0 OR cycle_without_transfer > 0 THEN 1 END) as patients_with_timeline,
            COUNT(CASE WHEN fiv_paid_count > 0 THEN 1 END) as patients_with_billing
        FROM gold.finops_summary
    """).fetchdf()
    
    print(f"✅ Table created successfully!")
    print(f"📊 Table Statistics:")
    print(f"   - Total patients: {table_stats['total_patients'].iloc[0]:,}")
    print(f"   - Patients with timeline data: {table_stats['patients_with_timeline'].iloc[0]:,}")
    print(f"   - Patients with billing data: {table_stats['patients_with_billing'].iloc[0]:,}")
    print(f"   - Total FIV cycles (timeline): {table_stats['total_cycles_with_transfer'].iloc[0] + table_stats['total_cycles_without_transfer'].iloc[0]:,}")
    print(f"   - Total FIV billing items: {table_stats['total_fiv_paid_count'].iloc[0]:,}")
    print(f"   - Total FIV billing amount: R$ {table_stats['total_fiv_paid_total'].iloc[0]:,.2f}")
    
    return table_stats

def analyze_finops_summary_data(conn):
    """Analyze the data in the finops_summary table"""
    
    print("\n🔍 Analyzing finops_summary data...")
    
    # Sample data
    sample_data = conn.execute("""
        SELECT *
        FROM gold.finops_summary
        WHERE (cycle_with_transfer > 0 OR cycle_without_transfer > 0) OR (fiv_paid_count > 0)
        ORDER BY (cycle_with_transfer + cycle_without_transfer + fiv_paid_count) DESC
        LIMIT 10
    """).fetchdf()
    
    print("\n📋 Sample Data (Top 10 patients by total activity):")
    print(sample_data.to_string(index=False))
    
    # Distribution analysis
    distribution = conn.execute("""
        SELECT 
            CASE 
                WHEN (cycle_with_transfer > 0 OR cycle_without_transfer > 0) AND fiv_paid_count > 0 THEN 'Timeline + Billing'
                WHEN cycle_with_transfer > 0 OR cycle_without_transfer > 0 THEN 'Timeline only'
                WHEN fiv_paid_count > 0 THEN 'Billing only'
                ELSE 'No activity'
            END as patient_type,
            COUNT(*) as patient_count,
            SUM(cycle_with_transfer) as total_transfer_cycles,
            SUM(cycle_without_transfer) as total_no_transfer_cycles,
            SUM(fiv_paid_count) as total_billing_items,
            SUM(fiv_paid_total) as total_billing_amount
        FROM gold.finops_summary
        GROUP BY 
            CASE 
                WHEN (cycle_with_transfer > 0 OR cycle_without_transfer > 0) AND fiv_paid_count > 0 THEN 'Timeline + Billing'
                WHEN cycle_with_transfer > 0 OR cycle_without_transfer > 0 THEN 'Timeline only'
                WHEN fiv_paid_count > 0 THEN 'Billing only'
                ELSE 'No activity'
            END
        ORDER BY patient_count DESC
    """).fetchdf()
    
    print("\n📈 Patient Distribution by Data Type:")
    print(distribution.to_string(index=False))

def main():
    """Main function to create the finops_summary table with billing data"""
    
    print("=== CREATING FINOPS SUMMARY TABLE WITH BILLING DATA ===")
    print(f"Timestamp: {datetime.now()}")
    print()
    
    try:
        # Connect to database
        conn = get_database_connection()
        
        # Create the table
        table_stats = create_finops_summary_with_billing_table(conn)
        
        # Analyze the data
        analyze_finops_summary_data(conn)
        
        # Quick check for the reported patient
        row = conn.execute("SELECT * FROM gold.finops_summary WHERE prontuario = 175583").fetchdf()
        if not row.empty:
            print("\n🔎 Check prontuario 175583:")
            print(row.to_string(index=False))
        
        print(f"\n✅ Successfully created gold.finops_summary table with billing data")
        print(f"📊 Table contains {table_stats['total_patients'].iloc[0]:,} patients")
        print(f"🔄 Total FIV cycles: {table_stats['total_cycles_with_transfer'].iloc[0] + table_stats['total_cycles_without_transfer'].iloc[0]:,}")
        print(f"💰 Total FIV billing: R$ {table_stats['total_fiv_paid_total'].iloc[0]:,.2f}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()
