#!/usr/bin/env python3
"""
Create gold.consultas_timeline table with monthly consultation events vs billing tracking per prontuario
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

def create_consultas_timeline_table(conn):
    """Create the gold.consultas_timeline table"""
    
    print("Creating gold.consultas_timeline table...")
    
    # Attach clinisys_all database
    repo_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    clinisys_db_path = os.path.join(repo_root, 'database', 'clinisys_all.duckdb')
    conn.execute(f"ATTACH '{clinisys_db_path}' AS clinisys_all")
    print(f"Attached clinisys_all database: {clinisys_db_path}")
    
    # Drop table if it exists
    conn.execute("DROP TABLE IF EXISTS gold.consultas_timeline")
    
    # Create the table with monthly consultation events vs billing tracking
    create_table_query = """
    CREATE TABLE gold.consultas_timeline AS
    WITH
    -- Get consultation events by month and prontuario
    -- Count occurrences with curated list of consultation procedures from view_extrato_atendimentos_central
    consultas_events AS (
        SELECT 
            prontuario,
            STRFTIME(data, '%Y-%m') as period_month,
            COUNT(*) as consultas_events_count
        FROM clinisys_all.silver.view_extrato_atendimentos_central
        WHERE prontuario IS NOT NULL 
            AND data IS NOT NULL
            AND data <= CURRENT_DATE
            AND procedimento_nome IN (
                '1ª Consulta de Reprodução Humana',
                'Consulta de Reprodução Humana',
                'Consulta Psicologia ***',
                'Consulta Reprodução Humana ***',
                'Consulta Pré Anestésica',
                '1ª Consulta Psicologia ***',
                '1ª Consulta Psicologia',
                'Consulta Psicologia',
                '1ª Consulta Reprodução Humana - IB ***',
                'Consulta Ginecologia',
                '1ª Consulta de Reprodução Humana Telemedicina - IB ***',
                '1ª Consulta Urologia',
                '1ª Consulta Ginecologia',
                'Consulta Pré Anestesica ***',
                '1ª Consulta Preservação da Fertilidade',
                'Consulta de Acompanhamento Psicológico ***',
                'Consulta Obstétrica',
                '1ª Consulta Genética',
                '1ª Consulta Nutricionista',
                '1ª Consulta Urológica ***',
                'Consulta Histeroscopia',
                'Consulta Urologia',
                'Consulta Urológica ***',
                'Consulta Nutricionista',
                'Consulta Preservação da Fertilidade',
                'Consulta Genética',
                'Consulta para Apoio Psicológico por Telemedicina ***',
                '1ª Consulta Obstétrica',
                'Consulta Pré-operatória',
                'Consulta Via Telemedicina ***',
                'Consulta Infectologista',
                'Consulta de preservação da fertilidade ***'
            )
        GROUP BY prontuario, STRFTIME(data, '%Y-%m')
    ),
    
    -- Get consultation billing data by month and prontuario
    -- Count payments with Descrição Gerencial = 'Consultas de CRH' OR 'Outras Consultas'
    consultas_billing AS (
        SELECT 
            prontuario,
            STRFTIME("DT Emissao", '%Y-%m') as period_month,
            COUNT(*) as billing_events_count,
            SUM(Total) as total_billing_amount,
            SUM("Qntd.") as total_quantity
        FROM silver.mesclada_vendas
        WHERE prontuario IS NOT NULL 
            AND prontuario != -1
            AND "DT Emissao" IS NOT NULL
            AND "DT Emissao" <= CURRENT_DATE
            AND "Descrição Gerencial" IN ('Consultas de CRH', 'Outras Consultas')
        GROUP BY prontuario, STRFTIME("DT Emissao", '%Y-%m')
    ),
    
    -- Get all unique prontuario-month combinations
    all_periods AS (
        SELECT prontuario, period_month FROM consultas_events
        UNION
        SELECT prontuario, period_month FROM consultas_billing
    ),
    
    -- Combine consultation events and billing data
    monthly_data AS (
        SELECT 
            ap.prontuario,
            ap.period_month,
            COALESCE(ce.consultas_events_count, 0) as consultas_events_count,
            COALESCE(cb.billing_events_count, 0) as billing_events_count,
            COALESCE(cb.total_billing_amount, 0) as total_billing_amount,
            COALESCE(cb.total_quantity, 0) as total_quantity
        FROM all_periods ap
        LEFT JOIN consultas_events ce ON ap.prontuario = ce.prontuario AND ap.period_month = ce.period_month
        LEFT JOIN consultas_billing cb ON ap.prontuario = cb.prontuario AND ap.period_month = cb.period_month
    ),
    
    -- Calculate cumulative totals per prontuario
    timeline_with_cumulative AS (
        SELECT 
            prontuario,
            period_month,
            consultas_events_count,
            billing_events_count,
            total_billing_amount,
            total_quantity,
            -- Cumulative totals per prontuario
            SUM(consultas_events_count) OVER (
                PARTITION BY prontuario
                ORDER BY period_month 
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) as cumulative_consultas_events,
            SUM(billing_events_count) OVER (
                PARTITION BY prontuario
                ORDER BY period_month 
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) as cumulative_billing_events,
            SUM(total_billing_amount) OVER (
                PARTITION BY prontuario
                ORDER BY period_month 
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) as cumulative_billing_amount,
            SUM(total_quantity) OVER (
                PARTITION BY prontuario
                ORDER BY period_month 
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) as cumulative_quantity
        FROM monthly_data
        ORDER BY prontuario, period_month
    )
    
    SELECT 
        prontuario,
        period_month,
        -- Events columns
        consultas_events_count,
        cumulative_consultas_events,
        -- Billing columns
        billing_events_count,
        total_billing_amount,
        total_quantity,
        cumulative_billing_events,
        cumulative_billing_amount,
        cumulative_quantity,
        -- Missing payment analysis: cumulative events - cumulative billing quantity
        cumulative_consultas_events - cumulative_quantity as consultas_missing_payment
    FROM timeline_with_cumulative
    WHERE consultas_events_count > 0 OR billing_events_count > 0
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
            SUM(consultas_events_count) as total_consultas_events,
            SUM(billing_events_count) as total_billing_events,
            SUM(total_billing_amount) as total_billing_amount,
            SUM(total_quantity) as total_quantity,
            MAX(consultas_missing_payment) as max_consultas_missing_payment,
            MIN(consultas_missing_payment) as min_consultas_missing_payment,
            COUNT(CASE WHEN consultas_missing_payment > 0 THEN 1 END) as records_with_missing_payment
        FROM gold.consultas_timeline
    """).fetchdf()
    
    print("Table created successfully.")
    print("Table Statistics:")
    print(f"   - Total records: {table_stats['total_records'].iloc[0]:,}")
    print(f"   - Unique patients: {table_stats['unique_patients'].iloc[0]:,}")
    print(f"   - Period range: {table_stats['earliest_month'].iloc[0]} to {table_stats['latest_month'].iloc[0]}")
    print(f"   - Total consultation events: {table_stats['total_consultas_events'].iloc[0]:,}")
    print(f"   - Total billing events: {table_stats['total_billing_events'].iloc[0]:,}")
    print(f"   - Total billing amount: R$ {table_stats['total_billing_amount'].iloc[0]:,.2f}")
    print(f"   - Total quantity: {table_stats['total_quantity'].iloc[0]:,}")
    print(f"   - Max consultation missing payment: {table_stats['max_consultas_missing_payment'].iloc[0]:,}")
    print(f"   - Min consultation missing payment: {table_stats['min_consultas_missing_payment'].iloc[0]:,}")
    print(f"   - Records with missing payment: {table_stats['records_with_missing_payment'].iloc[0]:,}")
    
    return table_stats

def analyze_consultas_timeline_data(conn):
    """Analyze the data in the consultas_timeline table"""
    
    print("\nAnalyzing consultas_timeline data...")
    
    # Sample data for recent months
    sample_data = conn.execute("""
        SELECT *
        FROM gold.consultas_timeline
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
            consultas_events_count,
            billing_events_count,
            consultas_missing_payment,
            cumulative_billing_amount
        FROM gold.consultas_timeline
        WHERE consultas_missing_payment > 0
        ORDER BY consultas_missing_payment DESC
        LIMIT 15
    """).fetchdf()
    
    print(f"\nPatients with Missing Payments (Top 15):")
    print(missing_payment_patients.to_string(index=False))
    
    # Monthly activity summary for last 12 months
    monthly_summary = conn.execute("""
        SELECT 
            period_month,
            SUM(consultas_events_count) as total_consultas_events,
            SUM(billing_events_count) as total_billing_events,
            SUM(total_billing_amount) as total_billing_amount,
            SUM(total_quantity) as total_quantity,
            COUNT(DISTINCT prontuario) as active_patients
        FROM gold.consultas_timeline
        WHERE period_month >= STRFTIME(CURRENT_DATE - INTERVAL 12 MONTH, '%Y-%m')
        GROUP BY period_month
        ORDER BY period_month DESC
        LIMIT 12
    """).fetchdf()
    
    print(f"\nMonthly Activity Summary (Last 12 months):")
    print(monthly_summary.to_string(index=False))
    
    # Patients with highest consultation activity
    top_consultation_patients = conn.execute("""
        SELECT 
            prontuario,
            MAX(cumulative_consultas_events) as total_consultas_events,
            MAX(cumulative_billing_events) as total_billing_events,
            MAX(cumulative_billing_amount) as total_billing_amount,
            MAX(consultas_missing_payment) as max_missing_payment,
            COUNT(*) as months_with_activity
        FROM gold.consultas_timeline
        GROUP BY prontuario
        ORDER BY total_consultas_events DESC
        LIMIT 10
    """).fetchdf()
    
    print(f"\nTop 10 Patients by Total Consultation Events:")
    print(top_consultation_patients.to_string(index=False))

def create_resumed_consultas_timeline_table(conn):
    """Create a resumed timeline table with only the most recent record per patient"""
    print("Creating gold.resumed_consultas_timeline table...")
    
    # Drop table if it exists
    conn.execute("DROP TABLE IF EXISTS gold.resumed_consultas_timeline")
    
    # Create the resumed timeline table
    create_table_query = """
    CREATE TABLE gold.resumed_consultas_timeline AS
    WITH latest_records AS (
        SELECT 
            prontuario,
            MAX(period_month) as latest_month
        FROM gold.consultas_timeline
        GROUP BY prontuario
    )
    SELECT 
        ct.*
    FROM gold.consultas_timeline ct
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
            SUM(cumulative_consultas_events) as total_consultas_events,
            SUM(cumulative_billing_events) as total_billing_events,
            SUM(cumulative_billing_amount) as total_billing_amount,
            SUM(cumulative_quantity) as total_quantity,
            SUM(consultas_missing_payment) as total_consultas_missing_payment,
            MAX(cumulative_consultas_events) as max_consultas_events_per_patient,
            MAX(consultas_missing_payment) as max_missing_payment_per_patient
        FROM gold.resumed_consultas_timeline
    """).fetchdf()
    
    print("Table created successfully.")
    print("Table Statistics:")
    print(f"   - Total records: {table_stats['total_records'].iloc[0]:,}")
    print(f"   - Unique patients: {table_stats['unique_patients'].iloc[0]:,}")
    print(f"   - Period range: {table_stats['earliest_month'].iloc[0]} to {table_stats['latest_month'].iloc[0]}")
    print(f"   - Total consultation events: {table_stats['total_consultas_events'].iloc[0]:,}")
    print(f"   - Total billing events: {table_stats['total_billing_events'].iloc[0]:,}")
    print(f"   - Total billing amount: R$ {table_stats['total_billing_amount'].iloc[0]:,.2f}")
    print(f"   - Total quantity: {table_stats['total_quantity'].iloc[0]:,}")
    print(f"   - Total consultation missing payments: {table_stats['total_consultas_missing_payment'].iloc[0]:,}")
    print(f"   - Max consultation events per patient: {table_stats['max_consultas_events_per_patient'].iloc[0]:,}")
    print(f"   - Max missing payment per patient: {table_stats['max_missing_payment_per_patient'].iloc[0]:,}")
    
    return table_stats

def analyze_resumed_consultas_timeline_data(conn):
    """Analyze the data in the resumed consultas timeline table"""
    print("\nAnalyzing resumed consultas timeline data...")
    
    # Sample data
    sample_data = conn.execute("""
        SELECT 
            prontuario,
            period_month,
            cumulative_consultas_events,
            cumulative_billing_events,
            cumulative_billing_amount,
            cumulative_quantity,
            consultas_missing_payment
        FROM gold.resumed_consultas_timeline
        ORDER BY consultas_missing_payment DESC
        LIMIT 15
    """).fetchdf()
    
    print("\nSample Data (Top 15 patients by missing payments):")
    print(sample_data.to_string(index=False))
    
    # Patients with highest missing payments
    high_missing = conn.execute("""
        SELECT 
            prontuario,
            cumulative_consultas_events as total_consultas_events,
            cumulative_billing_events as total_billing_events,
            cumulative_billing_amount as total_billing_amount,
            cumulative_quantity as total_quantity,
            consultas_missing_payment,
            period_month as latest_month
        FROM gold.resumed_consultas_timeline
        WHERE consultas_missing_payment > 0
        ORDER BY consultas_missing_payment DESC
        LIMIT 20
    """).fetchdf()
    
    print(f"\nTop 20 Patients with Missing Payments (Resumed View):")
    print(high_missing.to_string(index=False))
    
    # Monthly distribution of latest records
    monthly_dist = conn.execute("""
        SELECT 
            period_month,
            COUNT(*) as patients_with_latest_record,
            SUM(cumulative_consultas_events) as total_consultas_events,
            SUM(cumulative_billing_events) as total_billing_events,
            SUM(consultas_missing_payment) as total_missing_payment
        FROM gold.resumed_consultas_timeline
        GROUP BY period_month
        ORDER BY period_month DESC
        LIMIT 12
    """).fetchdf()
    
    print(f"\nMonthly Distribution of Latest Records (Last 12 months):")
    print(monthly_dist.to_string(index=False))

def main():
    """Main function to create the consultas timeline tables"""
    
    print("=== CREATING CONSULTAS TIMELINE TABLES ===")
    print(f"Timestamp: {datetime.now()}")
    print()
    
    try:
        # Connect to database
        print("Connecting to database...")
        conn = get_database_connection()
        
        # Create the consultas timeline table
        print("Creating consultas timeline table...")
        table_stats = create_consultas_timeline_table(conn)
        
        # Analyze the data
        analyze_consultas_timeline_data(conn)
        
        print(f"\nSuccessfully created gold.consultas_timeline table")
        print(f"Table contains {table_stats['unique_patients'].iloc[0]:,} unique patients")
        print(f"Total timeline records: {table_stats['total_records'].iloc[0]:,}")
        print(f"Period: {table_stats['earliest_month'].iloc[0]} to {table_stats['latest_month'].iloc[0]}")
        print(f"Total consultation events: {table_stats['total_consultas_events'].iloc[0]:,}")
        print(f"Total billing amount: R$ {table_stats['total_billing_amount'].iloc[0]:,.2f}")
        print(f"Records with missing payments: {table_stats['records_with_missing_payment'].iloc[0]:,}")
        
        print("\n" + "="*80)
        
        # Create resumed timeline (most recent record per patient)
        print("Creating resumed consultas timeline table...")
        resumed_stats = create_resumed_consultas_timeline_table(conn)
        
        # Analyze the resumed data
        analyze_resumed_consultas_timeline_data(conn)
        
        print(f"\nSuccessfully created gold.resumed_consultas_timeline table")
        print(f"Table contains {resumed_stats['unique_patients'].iloc[0]:,} unique patients")
        print(f"Total records: {resumed_stats['total_records'].iloc[0]:,}")
        print(f"Total consultation missing payments: {resumed_stats['total_consultas_missing_payment'].iloc[0]:,}")
        
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
