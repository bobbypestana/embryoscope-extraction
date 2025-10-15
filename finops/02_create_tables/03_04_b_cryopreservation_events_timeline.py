#!/usr/bin/env python3
"""
Create gold.cryopreservation_events_timeline table with monthly cryopreservation events vs billing
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

def create_cryopreservation_events_timeline_table(conn):
    """Create the gold.cryopreservation_events_timeline table"""
    
    print("Creating gold.cryopreservation_events_timeline table...")
    
    # Attach clinisys_all database
    repo_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    clinisys_db_path = os.path.join(repo_root, 'database', 'clinisys_all.duckdb')
    conn.execute(f"ATTACH '{clinisys_db_path}' AS clinisys_all")
    print(f"Attached clinisys_all database: {clinisys_db_path}")
    
    # Drop table if it exists
    conn.execute("DROP TABLE IF EXISTS gold.cryopreservation_events_timeline")
    
    # Create the table with monthly cryopreservation events vs billing
    create_table_query = """
    CREATE TABLE gold.cryopreservation_events_timeline AS
    WITH
    -- Get all freezing events by month and prontuario (embryos, eggs, and semen)
    freezing_events AS (
        SELECT 
            prontuario,
            period_month,
            SUM(freezing_events_count) as freezing_events_count
        FROM (
            -- Embryo freezing events
            SELECT 
                prontuario,
                STRFTIME(Data, '%Y-%m') as period_month,
                COUNT(DISTINCT CodCongelamento) as freezing_events_count
            FROM clinisys_all.silver.view_congelamentos_embrioes
            WHERE prontuario IS NOT NULL 
                AND Data IS NOT NULL
                AND Data <= CURRENT_DATE
                AND NEmbrioes IS NOT NULL
                AND NEmbrioes > 0
            GROUP BY prontuario, STRFTIME(Data, '%Y-%m')
            
            UNION ALL
            
            -- Egg freezing events
            SELECT 
                prontuario,
                STRFTIME(Data, '%Y-%m') as period_month,
                COUNT(DISTINCT CodCongelamento) as freezing_events_count
            FROM clinisys_all.silver.view_congelamentos_ovulos
            WHERE prontuario IS NOT NULL 
                AND Data IS NOT NULL
                AND Data <= CURRENT_DATE
                AND NOvulos IS NOT NULL
                AND NOvulos > 0
            GROUP BY prontuario, STRFTIME(Data, '%Y-%m')
            
            UNION ALL
            
            -- Semen freezing events
            SELECT 
                prontuario,
                STRFTIME(Data, '%Y-%m') as period_month,
                COUNT(DISTINCT CodCongelamento) as freezing_events_count
            FROM clinisys_all.silver.view_congelamentos_semen
            WHERE prontuario IS NOT NULL 
                AND Data IS NOT NULL
                AND Data <= CURRENT_DATE
            GROUP BY prontuario, STRFTIME(Data, '%Y-%m')
        ) all_freezing_events
        GROUP BY prontuario, period_month
    ),
    
    -- Get actual billing events for cryopreservation services by month and prontuario
    actual_billing AS (
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
            AND Descricao LIKE 'CRIOPRESERVACAO%'
        GROUP BY prontuario, STRFTIME("DT Emissao", '%Y-%m')
    ),
    
    -- Get historical billing events (assumed to exist for all freezing events before 2022-01-01)
    historical_billing AS (
        SELECT 
            prontuario,
            period_month,
            SUM(freezing_events_count) as billing_events_count,
            -- Assume average billing amount for historical events (can be adjusted)
            SUM(freezing_events_count) * 2500.0 as total_billing_amount
        FROM (
            -- Historical embryo freezing events
            SELECT 
                prontuario,
                STRFTIME(Data, '%Y-%m') as period_month,
                COUNT(DISTINCT CodCongelamento) as freezing_events_count
            FROM clinisys_all.silver.view_congelamentos_embrioes
            WHERE prontuario IS NOT NULL 
                AND Data IS NOT NULL
                AND Data < '2022-01-01'
                AND Data <= CURRENT_DATE
                AND NEmbrioes IS NOT NULL
                AND NEmbrioes > 0
            GROUP BY prontuario, STRFTIME(Data, '%Y-%m')
            
            UNION ALL
            
            -- Historical egg freezing events
            SELECT 
                prontuario,
                STRFTIME(Data, '%Y-%m') as period_month,
                COUNT(DISTINCT CodCongelamento) as freezing_events_count
            FROM clinisys_all.silver.view_congelamentos_ovulos
            WHERE prontuario IS NOT NULL 
                AND Data IS NOT NULL
                AND Data < '2022-01-01'
                AND Data <= CURRENT_DATE
                AND NOvulos IS NOT NULL
                AND NOvulos > 0
            GROUP BY prontuario, STRFTIME(Data, '%Y-%m')
            
            UNION ALL
            
            -- Historical semen freezing events
            SELECT 
                prontuario,
                STRFTIME(Data, '%Y-%m') as period_month,
                COUNT(DISTINCT CodCongelamento) as freezing_events_count
            FROM clinisys_all.silver.view_congelamentos_semen
            WHERE prontuario IS NOT NULL 
                AND Data IS NOT NULL
                AND Data < '2022-01-01'
                AND Data <= CURRENT_DATE
            GROUP BY prontuario, STRFTIME(Data, '%Y-%m')
        ) all_historical_freezing_events
        GROUP BY prontuario, period_month
    ),
    
    -- Combine actual and historical billing events
    cryopreservation_billing AS (
        SELECT 
            prontuario,
            period_month,
            SUM(billing_events_count) as billing_events_count,
            SUM(total_billing_amount) as total_billing_amount
        FROM (
            SELECT prontuario, period_month, billing_events_count, total_billing_amount FROM actual_billing
            UNION ALL
            SELECT prontuario, period_month, billing_events_count, total_billing_amount FROM historical_billing
        ) combined_billing
        GROUP BY prontuario, period_month
    ),
    
    -- Get all unique prontuario-month combinations from both datasets
    all_periods AS (
        SELECT prontuario, period_month FROM freezing_events
        UNION
        SELECT prontuario, period_month FROM cryopreservation_billing
    ),
    
    -- Combine freezing and billing data
    monthly_data AS (
        SELECT 
            ap.prontuario,
            ap.period_month,
            COALESCE(fe.freezing_events_count, 0) as freezing_events_count,
            COALESCE(cb.billing_events_count, 0) as billing_events_count,
            COALESCE(cb.total_billing_amount, 0) as total_billing_amount
        FROM all_periods ap
        LEFT JOIN freezing_events fe ON ap.prontuario = fe.prontuario AND ap.period_month = fe.period_month
        LEFT JOIN cryopreservation_billing cb ON ap.prontuario = cb.prontuario AND ap.period_month = cb.period_month
    ),
    
    -- Calculate cumulative totals and balance per prontuario
    timeline_with_cumulative AS (
        SELECT 
            prontuario,
            period_month,
            freezing_events_count,
            billing_events_count,
            total_billing_amount,
            -- Cumulative totals per prontuario
            SUM(freezing_events_count) OVER (
                PARTITION BY prontuario
                ORDER BY period_month 
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) as cumulative_freezing_events,
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
        -- Freezing columns
        freezing_events_count,
        cumulative_freezing_events,
        -- Billing columns
        billing_events_count,
        total_billing_amount,
        cumulative_billing_events,
        cumulative_billing_amount,
        -- Balance: cumulative freezing events - cumulative billing events
        cumulative_freezing_events - cumulative_billing_events as events_missing_payment
    FROM timeline_with_cumulative
    WHERE freezing_events_count > 0 OR billing_events_count > 0
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
            SUM(freezing_events_count) as total_freezing_events,
            SUM(billing_events_count) as total_billing_events,
            SUM(total_billing_amount) as total_billing_amount,
            MAX(events_missing_payment) as max_events_missing_payment,
            MIN(events_missing_payment) as min_events_missing_payment
        FROM gold.cryopreservation_events_timeline
    """).fetchdf()
    
    print("Table created successfully.")
    print("Table Statistics:")
    print(f"   - Total records: {table_stats['total_records'].iloc[0]:,}")
    print(f"   - Unique patients: {table_stats['unique_patients'].iloc[0]:,}")
    print(f"   - Period range: {table_stats['earliest_month'].iloc[0]} to {table_stats['latest_month'].iloc[0]}")
    print(f"   - Total freezing events: {table_stats['total_freezing_events'].iloc[0]:,}")
    print(f"   - Total billing events: {table_stats['total_billing_events'].iloc[0]:,} (includes assumed historical billing)")
    print(f"   - Total billing amount: R$ {table_stats['total_billing_amount'].iloc[0]:,.2f} (includes assumed historical amounts)")
    print(f"   - Max events missing payment: {table_stats['max_events_missing_payment'].iloc[0]:,}")
    print(f"   - Min events missing payment: {table_stats['min_events_missing_payment'].iloc[0]:,}")
    print("   - Note: Historical freezing events (pre-2022) assumed to have corresponding billing events")
    
    return table_stats

def analyze_cryopreservation_timeline_data(conn):
    """Analyze the data in the cryopreservation_events_timeline table"""
    
    print("\nAnalyzing cryopreservation_events_timeline data...")
    
    # Sample data for recent months
    sample_data = conn.execute("""
        SELECT *
        FROM gold.cryopreservation_events_timeline
        ORDER BY period_month DESC, prontuario
        LIMIT 15
    """).fetchdf()
    
    print("\nSample Data (First 15 records, recent months):")
    print(sample_data.to_string(index=False))
    
    # Patients with significant missing payments
    imbalance_data = conn.execute("""
        SELECT 
            prontuario,
            period_month,
            freezing_events_count,
            billing_events_count,
            events_missing_payment,
            cumulative_billing_amount
        FROM gold.cryopreservation_events_timeline
        WHERE ABS(events_missing_payment) > 3
        ORDER BY ABS(events_missing_payment) DESC
        LIMIT 15
    """).fetchdf()
    
    print(f"\nPatients with Significant Missing Payments (Top 15):")
    print(imbalance_data.to_string(index=False))
    
    # Monthly activity summary for last 12 months (aggregated across all patients)
    monthly_summary = conn.execute("""
        SELECT 
            period_month,
            SUM(freezing_events_count) as total_freezing_events,
            SUM(billing_events_count) as total_billing_events,
            SUM(total_billing_amount) as total_billing_amount,
            COUNT(DISTINCT prontuario) as active_patients
        FROM gold.cryopreservation_events_timeline
        WHERE period_month >= STRFTIME(CURRENT_DATE - INTERVAL 12 MONTH, '%Y-%m')
        GROUP BY period_month
        ORDER BY period_month DESC
        LIMIT 12
    """).fetchdf()
    
    print(f"\nMonthly Activity Summary (Last 12 months, aggregated):")
    print(monthly_summary.to_string(index=False))

def create_resumed_cryopreservation_events_timeline_table(conn):
    """Create the gold.resumed_cryopreservation_events_timeline table with only the most recent record per patient"""
    
    print("\nCreating gold.resumed_cryopreservation_events_timeline table...")
    
    # Drop table if it exists
    conn.execute("DROP TABLE IF EXISTS gold.resumed_cryopreservation_events_timeline")
    
    # Create the resumed table with only the latest record per patient
    create_table_query = """
    CREATE TABLE gold.resumed_cryopreservation_events_timeline AS
    WITH latest_records AS (
        SELECT 
            prontuario,
            MAX(period_month) as latest_month
        FROM gold.cryopreservation_events_timeline
        GROUP BY prontuario
    )
    SELECT 
        ct.*
    FROM gold.cryopreservation_events_timeline ct
    INNER JOIN latest_records lr ON ct.prontuario = lr.prontuario AND ct.period_month = lr.latest_month
    ORDER BY ct.prontuario
    """
    
    conn.execute(create_table_query)
    
    # Get statistics
    table_stats = conn.execute("""
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT prontuario) as unique_patients,
            MIN(period_month) as earliest_month,
            MAX(period_month) as latest_month,
            SUM(cumulative_freezing_events) as total_cumulative_freezing_events,
            SUM(cumulative_billing_events) as total_cumulative_billing_events,
            SUM(cumulative_billing_amount) as total_cumulative_billing_amount,
            MAX(events_missing_payment) as max_events_missing_payment,
            MIN(events_missing_payment) as min_events_missing_payment,
            AVG(events_missing_payment) as avg_events_missing_payment
        FROM gold.resumed_cryopreservation_events_timeline
    """).fetchdf()
    
    print("Resumed table created successfully.")
    print("Resumed Table Statistics:")
    print(f"   - Total records (one per patient): {table_stats['total_records'].iloc[0]:,}")
    print(f"   - Unique patients: {table_stats['unique_patients'].iloc[0]:,}")
    print(f"   - Period range: {table_stats['earliest_month'].iloc[0]} to {table_stats['latest_month'].iloc[0]}")
    print(f"   - Total cumulative freezing events: {table_stats['total_cumulative_freezing_events'].iloc[0]:,.0f}")
    print(f"   - Total cumulative billing events: {table_stats['total_cumulative_billing_events'].iloc[0]:,.0f} (includes assumed historical)")
    print(f"   - Total cumulative billing amount: R$ {table_stats['total_cumulative_billing_amount'].iloc[0]:,.2f}")
    print(f"   - Max events missing payment: {table_stats['max_events_missing_payment'].iloc[0]:,.0f}")
    print(f"   - Min events missing payment: {table_stats['min_events_missing_payment'].iloc[0]:,.0f}")
    print(f"   - Avg events missing payment: {table_stats['avg_events_missing_payment'].iloc[0]:,.2f}")
    
    # Show sample data
    sample_data = conn.execute("""
        SELECT *
        FROM gold.resumed_cryopreservation_events_timeline
        ORDER BY events_missing_payment DESC
        LIMIT 10
    """).fetchdf()
    
    print(f"\nSample Data (Top 10 patients by events_missing_payment):")
    print(sample_data.to_string(index=False))
    
    return table_stats

def main():
    print("=== CREATING CRYOPRESERVATION EVENTS TIMELINE TABLES ===")
    print(f'Timestamp: {datetime.now()}')
    
    conn = get_database_connection()
    
    try:
        # Create the cryopreservation events timeline table
        table_stats = create_cryopreservation_events_timeline_table(conn)
        
        # Analyze the data
        analyze_cryopreservation_timeline_data(conn)
        
        # Create the resumed table (most recent record per patient)
        resumed_stats = create_resumed_cryopreservation_events_timeline_table(conn)
        
        print("\n=== CRYOPRESERVATION EVENTS TIMELINE TABLES COMPLETED ===")
        
    except Exception as e:
        print(f"Error: {e}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    main()
