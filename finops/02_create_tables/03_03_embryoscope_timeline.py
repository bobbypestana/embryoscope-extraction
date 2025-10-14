#!/usr/bin/env python3
"""
Create gold.embryoscope_timeline table with monthly embryoscope usage vs billing tracking per prontuario
"""

import duckdb as db
import pandas as pd
from datetime import datetime
import os

def get_database_connection():
    """Create and return a connection to the huntington_data_lake database"""
    # Resolve DB path relative to repository root
    repo_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    path_to_db = os.path.join(repo_root, 'database', 'huntington_data_lake.duckdb')
    conn = db.connect(path_to_db)
    
    print(f"Connected to database: {path_to_db}")
    return conn

def create_embryoscope_timeline_table(conn):
    """Create the gold.embryoscope_timeline table"""
    
    print("Creating gold.embryoscope_timeline table...")
    
    # Drop table if it exists
    conn.execute("DROP TABLE IF EXISTS gold.embryoscope_timeline")
    
    # Create the table with monthly embryoscope usage vs billing tracking
    create_table_query = """
    CREATE TABLE gold.embryoscope_timeline AS
    WITH
    -- Get embryoscope usage data by month and prontuario
    -- Count distinct treatment_TreatmentName per month using embryo_KIDDate, fallback to embryo_FertilizationTime
    embryoscope_usage AS (
        SELECT 
            prontuario as prontuario,
            STRFTIME(COALESCE(embryo_KIDDate, embryo_FertilizationTime), '%Y-%m') as period_month,
            COUNT(DISTINCT treatment_TreatmentName) as embryoscope_usage_count
        FROM gold.embryoscope_embrioes
        WHERE prontuario IS NOT NULL 
            AND (embryo_KIDDate IS NOT NULL OR embryo_FertilizationTime IS NOT NULL)
            AND COALESCE(embryo_KIDDate, embryo_FertilizationTime) <= CURRENT_DATE
            AND treatment_TreatmentName IS NOT NULL
        GROUP BY prontuario, STRFTIME(COALESCE(embryo_KIDDate, embryo_FertilizationTime), '%Y-%m')
    ),
    
    -- Get embryoscope billing data by month and prontuario
    embryoscope_billing AS (
        SELECT 
            prontuario,
            STRFTIME("DT Emissao", '%Y-%m') as period_month,
            COUNT(*) as billing_events_count,
            SUM(Total) as total_billing_amount
        FROM silver.mesclada_vendas
        WHERE prontuario IS NOT NULL 
            AND prontuario != -1
            AND "DT Emissao" IS NOT NULL
            AND "DT Emissao" <= CURRENT_DATE
            AND "Descrição Gerencial" = 'Embryoscope'
        GROUP BY prontuario, STRFTIME("DT Emissao", '%Y-%m')
    ),
    
    -- Get all unique prontuario-month combinations
    all_periods AS (
        SELECT prontuario, period_month FROM embryoscope_usage
        UNION
        SELECT prontuario, period_month FROM embryoscope_billing
    ),
    
    -- Combine usage and billing data
    monthly_data AS (
        SELECT 
            ap.prontuario,
            ap.period_month,
            COALESCE(eu.embryoscope_usage_count, 0) as embryoscope_usage_count,
            COALESCE(eb.billing_events_count, 0) as billing_events_count,
            COALESCE(eb.total_billing_amount, 0) as total_billing_amount
        FROM all_periods ap
        LEFT JOIN embryoscope_usage eu ON ap.prontuario = eu.prontuario AND ap.period_month = eu.period_month
        LEFT JOIN embryoscope_billing eb ON ap.prontuario = eb.prontuario AND ap.period_month = eb.period_month
    ),
    
    -- Calculate cumulative totals per prontuario
    timeline_with_cumulative AS (
        SELECT 
            prontuario,
            period_month,
            embryoscope_usage_count,
            billing_events_count,
            total_billing_amount,
            -- Cumulative totals per prontuario
            SUM(embryoscope_usage_count) OVER (
                PARTITION BY prontuario
                ORDER BY period_month 
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) as cumulative_embryoscope_usage,
            SUM(billing_events_count) OVER (
                PARTITION BY prontuario
                ORDER BY period_month 
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) as cumulative_billing_events,
            SUM(total_billing_amount) OVER (
                PARTITION BY prontuario
                ORDER BY period_month 
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) as cumulative_billing_amount
        FROM monthly_data
        ORDER BY prontuario, period_month
    )
    
    SELECT 
        prontuario,
        period_month,
        -- Usage columns
        embryoscope_usage_count,
        cumulative_embryoscope_usage,
        -- Billing columns
        billing_events_count,
        total_billing_amount,
        cumulative_billing_events,
        cumulative_billing_amount,
        -- Missing payment analysis: cumulative usage - cumulative billing events
        cumulative_embryoscope_usage - cumulative_billing_events as embryoscope_missing_payment
    FROM timeline_with_cumulative
    WHERE embryoscope_usage_count > 0 OR billing_events_count > 0
    ORDER BY prontuario, period_month DESC
    """
    
    conn.execute(create_table_query)
    
    # Get statistics
    table_stats = conn.execute("""
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT prontuario) as unique_patients,
            MIN(period_month) as earliest_month,
            MAX(period_month) as latest_month,
            SUM(embryoscope_usage_count) as total_embryoscope_usage,
            SUM(billing_events_count) as total_billing_events,
            SUM(total_billing_amount) as total_billing_amount,
            MAX(embryoscope_missing_payment) as max_embryoscope_missing_payment,
            MIN(embryoscope_missing_payment) as min_embryoscope_missing_payment,
            COUNT(CASE WHEN embryoscope_missing_payment > 0 THEN 1 END) as records_with_missing_payment
        FROM gold.embryoscope_timeline
    """).fetchdf()
    
    print("Table created successfully.")
    print("Table Statistics:")
    print(f"   - Total records: {table_stats['total_records'].iloc[0]:,}")
    print(f"   - Unique patients: {table_stats['unique_patients'].iloc[0]:,}")
    print(f"   - Period range: {table_stats['earliest_month'].iloc[0]} to {table_stats['latest_month'].iloc[0]}")
    print(f"   - Total embryoscope usage: {table_stats['total_embryoscope_usage'].iloc[0]:,}")
    print(f"   - Total billing events: {table_stats['total_billing_events'].iloc[0]:,}")
    print(f"   - Total billing amount: R$ {table_stats['total_billing_amount'].iloc[0]:,.2f}")
    print(f"   - Max embryoscope missing payment: {table_stats['max_embryoscope_missing_payment'].iloc[0]:,}")
    print(f"   - Min embryoscope missing payment: {table_stats['min_embryoscope_missing_payment'].iloc[0]:,}")
    print(f"   - Records with missing payment: {table_stats['records_with_missing_payment'].iloc[0]:,}")
    
    return table_stats

def analyze_embryoscope_timeline_data(conn):
    """Analyze the data in the embryoscope_timeline table"""
    
    print("\nAnalyzing embryoscope_timeline data...")
    
    # Sample data for recent months
    sample_data = conn.execute("""
        SELECT *
        FROM gold.embryoscope_timeline
        ORDER BY period_month DESC, prontuario
        LIMIT 15
    """).fetchdf()
    
    print("\nSample Data (First 15 records, recent months):")
    print(sample_data.to_string(index=False))
    
    # Patients with missing payments
    missing_payment_patients = conn.execute("""
        SELECT 
            prontuario,
            period_month,
            embryoscope_usage_count,
            billing_events_count,
            embryoscope_missing_payment,
            cumulative_billing_amount
        FROM gold.embryoscope_timeline
        WHERE embryoscope_missing_payment > 0
        ORDER BY embryoscope_missing_payment DESC
        LIMIT 15
    """).fetchdf()
    
    print(f"\nPatients with Missing Payments (Top 15):")
    print(missing_payment_patients.to_string(index=False))
    
    # Monthly activity summary for last 12 months
    monthly_summary = conn.execute("""
        SELECT 
            period_month,
            SUM(embryoscope_usage_count) as total_embryoscope_usage,
            SUM(billing_events_count) as total_billing_events,
            SUM(total_billing_amount) as total_billing_amount,
            COUNT(DISTINCT prontuario) as active_patients
        FROM gold.embryoscope_timeline
        WHERE period_month >= STRFTIME(CURRENT_DATE - INTERVAL 12 MONTH, '%Y-%m')
        GROUP BY period_month
        ORDER BY period_month DESC
        LIMIT 12
    """).fetchdf()
    
    print(f"\nMonthly Activity Summary (Last 12 months):")
    print(monthly_summary.to_string(index=False))
    
    # Patients with highest usage
    top_usage_patients = conn.execute("""
        SELECT 
            prontuario,
            MAX(cumulative_embryoscope_usage) as total_embryoscope_usage,
            MAX(cumulative_billing_events) as total_billing_events,
            MAX(cumulative_billing_amount) as total_billing_amount,
            MAX(embryoscope_missing_payment) as max_missing_payment,
            COUNT(*) as months_with_activity
        FROM gold.embryoscope_timeline
        GROUP BY prontuario
        ORDER BY total_embryoscope_usage DESC
        LIMIT 10
    """).fetchdf()
    
    print(f"\nTop 10 Patients by Total Embryoscope Usage:")
    print(top_usage_patients.to_string(index=False))

def create_resumed_embryoscope_timeline_table(conn):
    """Create a resumed timeline table with only the most recent record per patient"""
    print("Creating gold.resumed_embryoscope_timeline table...")
    
    # Drop table if it exists
    conn.execute("DROP TABLE IF EXISTS gold.resumed_embryoscope_timeline")
    
    # Create the resumed timeline table
    create_table_query = """
    CREATE TABLE gold.resumed_embryoscope_timeline AS
    WITH latest_records AS (
        SELECT 
            prontuario,
            MAX(period_month) as latest_month
        FROM gold.embryoscope_timeline
        GROUP BY prontuario
    )
    SELECT 
        et.*
    FROM gold.embryoscope_timeline et
    INNER JOIN latest_records lr ON et.prontuario = lr.prontuario AND et.period_month = lr.latest_month
    ORDER BY et.prontuario
    """
    
    conn.execute(create_table_query)
    
    # Get statistics
    table_stats = conn.execute("""
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT prontuario) as unique_patients,
            MIN(period_month) as earliest_month,
            MAX(period_month) as latest_month,
            SUM(cumulative_embryoscope_usage) as total_embryoscope_usage,
            SUM(cumulative_billing_events) as total_billing_events,
            SUM(cumulative_billing_amount) as total_billing_amount,
            SUM(embryoscope_missing_payment) as total_embryoscope_missing_payment,
            MAX(cumulative_embryoscope_usage) as max_embryoscope_usage_per_patient,
            MAX(embryoscope_missing_payment) as max_missing_payment_per_patient
        FROM gold.resumed_embryoscope_timeline
    """).fetchdf()
    
    print("Table created successfully.")
    print("Table Statistics:")
    print(f"   - Total records: {table_stats['total_records'].iloc[0]:,}")
    print(f"   - Unique patients: {table_stats['unique_patients'].iloc[0]:,}")
    print(f"   - Period range: {table_stats['earliest_month'].iloc[0]} to {table_stats['latest_month'].iloc[0]}")
    print(f"   - Total embryoscope usage: {table_stats['total_embryoscope_usage'].iloc[0]:,}")
    print(f"   - Total billing events: {table_stats['total_billing_events'].iloc[0]:,}")
    print(f"   - Total billing amount: R$ {table_stats['total_billing_amount'].iloc[0]:,.2f}")
    print(f"   - Total embryoscope missing payments: {table_stats['total_embryoscope_missing_payment'].iloc[0]:,}")
    print(f"   - Max embryoscope usage per patient: {table_stats['max_embryoscope_usage_per_patient'].iloc[0]:,}")
    print(f"   - Max missing payment per patient: {table_stats['max_missing_payment_per_patient'].iloc[0]:,}")
    
    return table_stats

def analyze_resumed_embryoscope_timeline_data(conn):
    """Analyze the data in the resumed embryoscope timeline table"""
    print("\nAnalyzing resumed embryoscope timeline data...")
    
    # Sample data
    sample_data = conn.execute("""
        SELECT 
            prontuario,
            period_month,
            cumulative_embryoscope_usage,
            cumulative_billing_events,
            cumulative_billing_amount,
            embryoscope_missing_payment
        FROM gold.resumed_embryoscope_timeline
        ORDER BY embryoscope_missing_payment DESC
        LIMIT 15
    """).fetchdf()
    
    print("\nSample Data (Top 15 patients by missing payments):")
    print(sample_data.to_string(index=False))
    
    # Patients with highest missing payments
    high_missing = conn.execute("""
        SELECT 
            prontuario,
            cumulative_embryoscope_usage as total_embryoscope_usage,
            cumulative_billing_events as total_billing_events,
            cumulative_billing_amount as total_billing_amount,
            embryoscope_missing_payment,
            period_month as latest_month
        FROM gold.resumed_embryoscope_timeline
        WHERE embryoscope_missing_payment > 0
        ORDER BY embryoscope_missing_payment DESC
        LIMIT 20
    """).fetchdf()
    
    print(f"\nTop 20 Patients with Missing Payments (Resumed View):")
    print(high_missing.to_string(index=False))
    
    # Monthly distribution of latest records
    monthly_dist = conn.execute("""
        SELECT 
            period_month,
            COUNT(*) as patients_with_latest_record,
            SUM(cumulative_embryoscope_usage) as total_embryoscope_usage,
            SUM(cumulative_billing_events) as total_billing_events,
            SUM(embryoscope_missing_payment) as total_missing_payment
        FROM gold.resumed_embryoscope_timeline
        GROUP BY period_month
        ORDER BY period_month DESC
        LIMIT 12
    """).fetchdf()
    
    print(f"\nMonthly Distribution of Latest Records (Last 12 months):")
    print(monthly_dist.to_string(index=False))

def main():
    """Main function to create the embryoscope timeline tables"""
    
    print("=== CREATING EMBRYOSCOPE TIMELINE TABLES ===")
    print(f"Timestamp: {datetime.now()}")
    print()
    
    try:
        # Connect to database
        print("Connecting to database...")
        conn = get_database_connection()
        
        # Create the embryoscope timeline table
        print("Creating embryoscope timeline table...")
        table_stats = create_embryoscope_timeline_table(conn)
        
        # Analyze the data
        analyze_embryoscope_timeline_data(conn)
        
        print(f"\nSuccessfully created gold.embryoscope_timeline table")
        print(f"Table contains {table_stats['unique_patients'].iloc[0]:,} unique patients")
        print(f"Total timeline records: {table_stats['total_records'].iloc[0]:,}")
        print(f"Period: {table_stats['earliest_month'].iloc[0]} to {table_stats['latest_month'].iloc[0]}")
        print(f"Total embryoscope usage: {table_stats['total_embryoscope_usage'].iloc[0]:,}")
        print(f"Total billing amount: R$ {table_stats['total_billing_amount'].iloc[0]:,.2f}")
        print(f"Records with missing payments: {table_stats['records_with_missing_payment'].iloc[0]:,}")
        
        print("\n" + "="*80)
        
        # Create resumed timeline (most recent record per patient)
        print("Creating resumed embryoscope timeline table...")
        resumed_stats = create_resumed_embryoscope_timeline_table(conn)
        
        # Analyze the resumed data
        analyze_resumed_embryoscope_timeline_data(conn)
        
        print(f"\nSuccessfully created gold.resumed_embryoscope_timeline table")
        print(f"Table contains {resumed_stats['unique_patients'].iloc[0]:,} unique patients")
        print(f"Total records: {resumed_stats['total_records'].iloc[0]:,}")
        print(f"Total embryoscope missing payments: {resumed_stats['total_embryoscope_missing_payment'].iloc[0]:,}")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()
