#!/usr/bin/env python3
"""
Create gold.biopsy_pgta_timeline table with monthly biopsy and PGT-A test tracking per prontuario
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

def create_biopsy_pgta_timeline_table(conn):
    """Create the gold.biopsy_pgta_timeline table"""
    
    print("Creating gold.biopsy_pgta_timeline table...")
    
    # Attach clinisys_all database
    repo_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    clinisys_db_path = os.path.join(repo_root, 'database', 'clinisys_all.duckdb')
    conn.execute(f"ATTACH '{clinisys_db_path}' AS clinisys_all")
    print(f"Attached clinisys_all database: {clinisys_db_path}")
    
    # Drop table if it exists
    conn.execute("DROP TABLE IF EXISTS gold.biopsy_pgta_timeline")
    
    # Create the table with monthly biopsy and PGT-A test tracking
    create_table_query = """
    CREATE TABLE gold.biopsy_pgta_timeline AS
    WITH
    -- Get biopsy data by month and prontuario
    -- Count distinct id where PGD = 'Sim' in view_micromanipulacao_oocitos
    biopsy_events AS (
        SELECT 
            m.prontuario,
            STRFTIME(CAST(m.Data_DL AS VARCHAR)::DATE, '%Y-%m') as period_month,
            COUNT(DISTINCT o.id) as biopsy_count
        FROM clinisys_all.silver.view_micromanipulacao m
        INNER JOIN clinisys_all.silver.view_micromanipulacao_oocitos o
            ON m.codigo_ficha = o.id_micromanipulacao
        WHERE m.prontuario IS NOT NULL 
            AND m.Data_DL IS NOT NULL
            AND CAST(m.Data_DL AS VARCHAR)::DATE <= CURRENT_DATE
            AND o.PGD = 'Sim'
        GROUP BY m.prontuario, STRFTIME(CAST(m.Data_DL AS VARCHAR)::DATE, '%Y-%m')
    ),
    
    -- Get PGT-A test data by month and prontuario
    -- Count where IdentificacaoPGD LIKE 'PGT%' but exclude 'PGT-M + A'
    pgta_events AS (
        SELECT 
            m.prontuario,
            STRFTIME(CAST(m.Data_DL AS VARCHAR)::DATE, '%Y-%m') as period_month,
            COUNT(DISTINCT o.id) as pgta_test_count
        FROM clinisys_all.silver.view_micromanipulacao m
        INNER JOIN clinisys_all.silver.view_micromanipulacao_oocitos o
            ON m.codigo_ficha = o.id_micromanipulacao
        WHERE m.prontuario IS NOT NULL 
            AND m.Data_DL IS NOT NULL
            AND CAST(m.Data_DL AS VARCHAR)::DATE <= CURRENT_DATE
            AND o.IdentificacaoPGD LIKE 'PGT%'
            AND o.IdentificacaoPGD != 'PGT-M + A'
            AND o.ResultadoPGD IS NOT NULL
            AND o.ResultadoPGD NOT IN ('Não analisado', 'Sem leitura', 'Não detectado')
        GROUP BY m.prontuario, STRFTIME(CAST(m.Data_DL AS VARCHAR)::DATE, '%Y-%m')
    ),
    
    -- Get all unique prontuario-month combinations
    all_periods AS (
        SELECT prontuario, period_month FROM biopsy_events
        UNION
        SELECT prontuario, period_month FROM pgta_events
    ),
    
    -- Combine biopsy and PGT-A data
    monthly_data AS (
        SELECT 
            ap.prontuario,
            ap.period_month,
            COALESCE(be.biopsy_count, 0) as biopsy_count,
            COALESCE(pe.pgta_test_count, 0) as pgta_test_count
        FROM all_periods ap
        LEFT JOIN biopsy_events be ON ap.prontuario = be.prontuario AND ap.period_month = be.period_month
        LEFT JOIN pgta_events pe ON ap.prontuario = pe.prontuario AND ap.period_month = pe.period_month
    ),
    
    -- Calculate cumulative totals per prontuario
    timeline_with_cumulative AS (
        SELECT 
            prontuario,
            period_month,
            biopsy_count,
            pgta_test_count,
            -- Cumulative totals per prontuario
            SUM(biopsy_count) OVER (
                PARTITION BY prontuario
                ORDER BY period_month 
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) as cumulative_biopsy_count,
            SUM(pgta_test_count) OVER (
                PARTITION BY prontuario
                ORDER BY period_month 
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) as cumulative_pgta_test_count
        FROM monthly_data
        ORDER BY prontuario, period_month
    )
    
    SELECT 
        prontuario,
        period_month,
        biopsy_count,
        pgta_test_count,
        cumulative_biopsy_count,
        cumulative_pgta_test_count
    FROM timeline_with_cumulative
    WHERE biopsy_count > 0 OR pgta_test_count > 0
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
            SUM(biopsy_count) as total_biopsies,
            SUM(pgta_test_count) as total_pgta_tests,
            MAX(cumulative_biopsy_count) as max_cumulative_biopsies,
            MAX(cumulative_pgta_test_count) as max_cumulative_pgta_tests
        FROM gold.biopsy_pgta_timeline
    """).fetchdf()
    
    print("Table created successfully.")
    print("Table Statistics:")
    print(f"   - Total records: {table_stats['total_records'].iloc[0]:,}")
    print(f"   - Unique patients: {table_stats['unique_patients'].iloc[0]:,}")
    print(f"   - Period range: {table_stats['earliest_month'].iloc[0]} to {table_stats['latest_month'].iloc[0]}")
    print(f"   - Total biopsies: {table_stats['total_biopsies'].iloc[0]:,}")
    print(f"   - Total PGT-A tests: {table_stats['total_pgta_tests'].iloc[0]:,}")
    print(f"   - Max cumulative biopsies per patient: {table_stats['max_cumulative_biopsies'].iloc[0]:,}")
    print(f"   - Max cumulative PGT-A tests per patient: {table_stats['max_cumulative_pgta_tests'].iloc[0]:,}")
    
    return table_stats

def analyze_biopsy_pgta_timeline_data(conn):
    """Analyze the data in the biopsy_pgta_timeline table"""
    
    print("\nAnalyzing biopsy_pgta_timeline data...")
    
    # Sample data for recent months
    sample_data = conn.execute("""
        SELECT *
        FROM gold.biopsy_pgta_timeline
        ORDER BY period_month DESC, prontuario
        LIMIT 15
    """).fetchdf()
    
    print("\nSample Data (First 15 records, recent months):")
    print(sample_data.to_string(index=False))
    
    # Patients with most biopsies
    top_biopsy_patients = conn.execute("""
        SELECT 
            prontuario,
            MAX(cumulative_biopsy_count) as total_biopsies,
            MAX(cumulative_pgta_test_count) as total_pgta_tests,
            COUNT(*) as months_with_activity
        FROM gold.biopsy_pgta_timeline
        GROUP BY prontuario
        ORDER BY total_biopsies DESC
        LIMIT 10
    """).fetchdf()
    
    print(f"\nTop 10 Patients by Total Biopsies:")
    print(top_biopsy_patients.to_string(index=False))
    
    # Monthly activity summary for last 12 months
    monthly_summary = conn.execute("""
        SELECT 
            period_month,
            SUM(biopsy_count) as total_biopsies,
            SUM(pgta_test_count) as total_pgta_tests,
            COUNT(DISTINCT prontuario) as active_patients
        FROM gold.biopsy_pgta_timeline
        WHERE period_month >= STRFTIME(CURRENT_DATE - INTERVAL 12 MONTH, '%Y-%m')
        GROUP BY period_month
        ORDER BY period_month DESC
        LIMIT 12
    """).fetchdf()
    
    print(f"\nMonthly Activity Summary (Last 12 months):")
    print(monthly_summary.to_string(index=False))
    
    # Patients with both biopsies and PGT-A tests
    combined_activity = conn.execute("""
        SELECT 
            prontuario,
            SUM(biopsy_count) as total_biopsies,
            SUM(pgta_test_count) as total_pgta_tests,
            COUNT(*) as months_with_activity
        FROM gold.biopsy_pgta_timeline
        WHERE biopsy_count > 0 AND pgta_test_count > 0
        GROUP BY prontuario
        ORDER BY (total_biopsies + total_pgta_tests) DESC
        LIMIT 10
    """).fetchdf()
    
    print(f"\nTop 10 Patients with Both Biopsies and PGT-A Tests:")
    print(combined_activity.to_string(index=False))

def create_billing_timeline_table(conn):
    """Create the gold.billing_timeline table"""
    
    print("Creating gold.billing_timeline table...")
    
    # Drop table if it exists
    conn.execute("DROP TABLE IF EXISTS gold.billing_timeline")
    
    # Create the table with monthly billing tracking for biopsy and PGT-A services
    create_table_query = """
    CREATE TABLE gold.billing_timeline AS
    WITH
    -- Get biopsy billing data by month and prontuario
    biopsy_billing AS (
        SELECT 
            prontuario,
            STRFTIME("DT Emissao", '%Y-%m') as period_month,
            COUNT(*) as biopsy_payment_count,
            SUM(Total) as biopsy_payment_amount,
            SUM("Qntd.") as biopsy_payment_qtd
        FROM silver.mesclada_vendas
        WHERE prontuario IS NOT NULL 
            AND prontuario != -1
            AND "DT Emissao" IS NOT NULL
            AND "DT Emissao" <= CURRENT_DATE
            AND "Descrição Gerencial" = 'Biópsia Embrionária'
        GROUP BY prontuario, STRFTIME("DT Emissao", '%Y-%m')
    ),
    
    -- Get PGT-A billing data by month and prontuario (excluding PGT-M + A)
    pgta_billing AS (
        SELECT 
            prontuario,
            STRFTIME("DT Emissao", '%Y-%m') as period_month,
            COUNT(*) as pgta_payment_count,
            SUM(Total) as pgta_payment_amount,
            SUM("Qntd.") as pgta_payment_qtd
        FROM silver.mesclada_vendas
        WHERE prontuario IS NOT NULL 
            AND prontuario != -1
            AND "DT Emissao" IS NOT NULL
            AND "DT Emissao" <= CURRENT_DATE
            AND "Descrição Gerencial" LIKE '%PGT-%'
            AND "Descrição Gerencial" NOT LIKE '%PGT-M + A%'
        GROUP BY prontuario, STRFTIME("DT Emissao", '%Y-%m')
    ),
    
    -- Get all unique prontuario-month combinations
    all_periods AS (
        SELECT prontuario, period_month FROM biopsy_billing
        UNION
        SELECT prontuario, period_month FROM pgta_billing
    ),
    
    -- Combine biopsy and PGT-A billing data
    monthly_billing AS (
        SELECT 
            ap.prontuario,
            ap.period_month,
            COALESCE(bb.biopsy_payment_count, 0) as biopsy_payment_count,
            COALESCE(bb.biopsy_payment_amount, 0) as biopsy_payment_amount,
            COALESCE(bb.biopsy_payment_qtd, 0) as biopsy_payment_qtd,
            COALESCE(pb.pgta_payment_count, 0) as pgta_payment_count,
            COALESCE(pb.pgta_payment_amount, 0) as pgta_payment_amount,
            COALESCE(pb.pgta_payment_qtd, 0) as pgta_payment_qtd
        FROM all_periods ap
        LEFT JOIN biopsy_billing bb ON ap.prontuario = bb.prontuario AND ap.period_month = bb.period_month
        LEFT JOIN pgta_billing pb ON ap.prontuario = pb.prontuario AND ap.period_month = pb.period_month
    ),
    
    -- Calculate cumulative totals per prontuario
    timeline_with_cumulative AS (
        SELECT 
            prontuario,
            period_month,
            biopsy_payment_count,
            biopsy_payment_amount,
            biopsy_payment_qtd,
            pgta_payment_count,
            pgta_payment_amount,
            pgta_payment_qtd,
            -- Cumulative totals per prontuario (using quantities)
            SUM(biopsy_payment_qtd) OVER (
                PARTITION BY prontuario
                ORDER BY period_month 
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) as cumulative_biopsy_payment_qtd,
            SUM(biopsy_payment_amount) OVER (
                PARTITION BY prontuario
                ORDER BY period_month 
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) as cumulative_biopsy_payment_amount,
            SUM(pgta_payment_qtd) OVER (
                PARTITION BY prontuario
                ORDER BY period_month 
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) as cumulative_pgta_payment_qtd,
            SUM(pgta_payment_amount) OVER (
                PARTITION BY prontuario
                ORDER BY period_month 
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) as cumulative_pgta_payment_amount
        FROM monthly_billing
        ORDER BY prontuario, period_month
    )
    
    SELECT 
        prontuario,
        period_month,
        biopsy_payment_count,
        biopsy_payment_amount,
        biopsy_payment_qtd,
        cumulative_biopsy_payment_qtd,
        cumulative_biopsy_payment_amount,
        pgta_payment_count,
        pgta_payment_amount,
        pgta_payment_qtd,
        cumulative_pgta_payment_qtd,
        cumulative_pgta_payment_amount
    FROM timeline_with_cumulative
    WHERE biopsy_payment_count > 0 OR pgta_payment_count > 0
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
            SUM(biopsy_payment_count) as total_biopsy_payments,
            SUM(biopsy_payment_amount) as total_biopsy_payment_amount,
            SUM(biopsy_payment_qtd) as total_biopsy_payment_qtd,
            SUM(pgta_payment_count) as total_pgta_payments,
            SUM(pgta_payment_amount) as total_pgta_payment_amount,
            SUM(pgta_payment_qtd) as total_pgta_payment_qtd,
            MAX(cumulative_biopsy_payment_qtd) as max_cumulative_biopsy_payment_qtd,
            MAX(cumulative_biopsy_payment_amount) as max_cumulative_biopsy_amount,
            MAX(cumulative_pgta_payment_qtd) as max_cumulative_pgta_payment_qtd,
            MAX(cumulative_pgta_payment_amount) as max_cumulative_pgta_amount
        FROM gold.billing_timeline
    """).fetchdf()
    
    print("Table created successfully.")
    print("Table Statistics:")
    print(f"   - Total records: {table_stats['total_records'].iloc[0]:,}")
    print(f"   - Unique patients: {table_stats['unique_patients'].iloc[0]:,}")
    print(f"   - Period range: {table_stats['earliest_month'].iloc[0]} to {table_stats['latest_month'].iloc[0]}")
    print(f"   - Total biopsy payments: {table_stats['total_biopsy_payments'].iloc[0]:,}")
    print(f"   - Total biopsy payment amount: R$ {table_stats['total_biopsy_payment_amount'].iloc[0]:,.2f}")
    print(f"   - Total biopsy quantity: {table_stats['total_biopsy_payment_qtd'].iloc[0]:,}")
    print(f"   - Total PGT-A payments: {table_stats['total_pgta_payments'].iloc[0]:,}")
    print(f"   - Total PGT-A payment amount: R$ {table_stats['total_pgta_payment_amount'].iloc[0]:,.2f}")
    print(f"   - Total PGT-A quantity: {table_stats['total_pgta_payment_qtd'].iloc[0]:,}")
    print(f"   - Max cumulative biopsy quantity per patient: {table_stats['max_cumulative_biopsy_payment_qtd'].iloc[0]:,}")
    print(f"   - Max cumulative biopsy amount per patient: R$ {table_stats['max_cumulative_biopsy_amount'].iloc[0]:,.2f}")
    print(f"   - Max cumulative PGT-A quantity per patient: {table_stats['max_cumulative_pgta_payment_qtd'].iloc[0]:,}")
    print(f"   - Max cumulative PGT-A amount per patient: R$ {table_stats['max_cumulative_pgta_amount'].iloc[0]:,.2f}")
    
    return table_stats

def analyze_billing_timeline_data(conn):
    """Analyze the data in the billing_timeline table"""
    
    print("\nAnalyzing billing_timeline data...")
    
    # Sample data for recent months
    sample_data = conn.execute("""
        SELECT *
        FROM gold.billing_timeline
        ORDER BY period_month DESC, prontuario
        LIMIT 15
    """).fetchdf()
    
    print("\nSample Data (First 15 records, recent months):")
    print(sample_data.to_string(index=False))
    
    # Patients with highest billing amounts
    top_billing_patients = conn.execute("""
        SELECT 
            prontuario,
            MAX(cumulative_biopsy_payment_amount) as total_biopsy_amount,
            MAX(cumulative_pgta_payment_amount) as total_pgta_amount,
            MAX(cumulative_biopsy_payment_amount + cumulative_pgta_payment_amount) as total_amount,
            COUNT(*) as months_with_billing
        FROM gold.billing_timeline
        GROUP BY prontuario
        ORDER BY total_amount DESC
        LIMIT 10
    """).fetchdf()
    
    print(f"\nTop 10 Patients by Total Billing Amount:")
    print(top_billing_patients.to_string(index=False))
    
    # Monthly billing summary for last 12 months
    monthly_summary = conn.execute("""
        SELECT 
            period_month,
            SUM(biopsy_payment_count) as total_biopsy_payments,
            SUM(biopsy_payment_amount) as total_biopsy_amount,
            SUM(pgta_payment_count) as total_pgta_payments,
            SUM(pgta_payment_amount) as total_pgta_amount,
            COUNT(DISTINCT prontuario) as active_patients
        FROM gold.billing_timeline
        WHERE period_month >= STRFTIME(CURRENT_DATE - INTERVAL 12 MONTH, '%Y-%m')
        GROUP BY period_month
        ORDER BY period_month DESC
        LIMIT 12
    """).fetchdf()
    
    print(f"\nMonthly Billing Summary (Last 12 months):")
    print(monthly_summary.to_string(index=False))

def create_comprehensive_timeline_table(conn):
    """Create the gold.comprehensive_biopsy_pgta_timeline table combining both timelines"""
    
    print("Creating gold.comprehensive_biopsy_pgta_timeline table...")
    
    # Drop table if it exists
    conn.execute("DROP TABLE IF EXISTS gold.comprehensive_biopsy_pgta_timeline")
    
    # Create the comprehensive timeline table
    create_table_query = """
    CREATE TABLE gold.comprehensive_biopsy_pgta_timeline AS
    WITH
    -- Get all unique prontuario-month combinations from both timelines (after 2024-01-01)
    all_periods AS (
        SELECT prontuario, period_month FROM gold.biopsy_pgta_timeline
        WHERE period_month >= '2024-01'
        UNION
        SELECT prontuario, period_month FROM gold.billing_timeline
        WHERE period_month >= '2024-01'
    ),
    
    -- Combine biopsy PGT-A and billing data (without cumulative values)
    combined_data AS (
        SELECT 
            ap.prontuario,
            ap.period_month,
            -- Biopsy columns
            COALESCE(bp.biopsy_count, 0) as biopsy_count,
            COALESCE(bp.pgta_test_count, 0) as pgta_test_count,
            -- Billing columns
            COALESCE(bt.biopsy_payment_amount, 0) as biopsy_payment_amount,
            COALESCE(bt.biopsy_payment_count, 0) as biopsy_payment_count,
            COALESCE(bt.biopsy_payment_qtd, 0) as biopsy_payment_qtd,
            COALESCE(bt.pgta_payment_amount, 0) as pgta_payment_amount,
            COALESCE(bt.pgta_payment_count, 0) as pgta_payment_count,
            COALESCE(bt.pgta_payment_qtd, 0) as pgta_payment_qtd
        FROM all_periods ap
        LEFT JOIN gold.biopsy_pgta_timeline bp ON ap.prontuario = bp.prontuario AND ap.period_month = bp.period_month
        LEFT JOIN gold.billing_timeline bt ON ap.prontuario = bt.prontuario AND ap.period_month = bt.period_month
    ),
    
    -- Calculate cumulative values in the comprehensive table
    timeline_with_cumulative AS (
        SELECT 
            prontuario,
            period_month,
            biopsy_count,
            pgta_test_count,
            biopsy_payment_amount,
            biopsy_payment_count,
            biopsy_payment_qtd,
            pgta_payment_amount,
            pgta_payment_count,
            pgta_payment_qtd,
            -- Calculate cumulative values per prontuario
            SUM(biopsy_count) OVER (
                PARTITION BY prontuario
                ORDER BY period_month 
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) as cumulative_biopsy_count,
            SUM(pgta_test_count) OVER (
                PARTITION BY prontuario
                ORDER BY period_month 
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) as cumulative_pgta_test_count,
            SUM(biopsy_payment_qtd) OVER (
                PARTITION BY prontuario
                ORDER BY period_month 
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) as cumulative_biopsy_payment_qtd,
            SUM(pgta_payment_qtd) OVER (
                PARTITION BY prontuario
                ORDER BY period_month 
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) as cumulative_pgta_payment_qtd
        FROM combined_data
        WHERE (biopsy_count > 0 OR pgta_test_count > 0 OR biopsy_payment_count > 0 OR pgta_payment_count > 0)
          AND period_month >= '2024-01'
    )
    
    SELECT 
        prontuario,
        period_month,
        -- Biopsy columns
        biopsy_count,
        cumulative_biopsy_count,
        biopsy_payment_amount,
        biopsy_payment_count,
        biopsy_payment_qtd,
        cumulative_biopsy_payment_qtd,
        -- Missing payment analysis for biopsy (using quantities)
        cumulative_biopsy_count - cumulative_biopsy_payment_qtd as biopsy_missing_payment,
        -- PGT-A columns
        pgta_test_count,
        cumulative_pgta_test_count,
        pgta_payment_amount,
        pgta_payment_count,
        pgta_payment_qtd,
        cumulative_pgta_payment_qtd,
        -- Missing payment analysis for PGT-A (using quantities)
        cumulative_pgta_test_count - cumulative_pgta_payment_qtd as pgta_missing_payment
    FROM timeline_with_cumulative
    WHERE period_month >= '2024-01'
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
            -- Biopsy statistics
            SUM(biopsy_count) as total_biopsies,
            MAX(cumulative_biopsy_count) as max_cumulative_biopsies,
            SUM(biopsy_payment_count) as total_biopsy_payments,
            SUM(biopsy_missing_payment) as total_biopsy_missing_payment,
            -- PGT-A statistics
            SUM(pgta_test_count) as total_pgta_tests,
            MAX(cumulative_pgta_test_count) as max_cumulative_pgta_tests,
            SUM(pgta_payment_count) as total_pgta_payments,
            SUM(pgta_missing_payment) as total_pgta_missing_payment
        FROM gold.comprehensive_biopsy_pgta_timeline
    """).fetchdf()
    
    print("Table created successfully.")
    print("Table Statistics:")
    print(f"   - Total records: {table_stats['total_records'].iloc[0]:,}")
    print(f"   - Unique patients: {table_stats['unique_patients'].iloc[0]:,}")
    print(f"   - Period range: {table_stats['earliest_month'].iloc[0]} to {table_stats['latest_month'].iloc[0]}")
    print(f"   - Total biopsies: {table_stats['total_biopsies'].iloc[0]:,}")
    print(f"   - Max cumulative biopsies per patient: {table_stats['max_cumulative_biopsies'].iloc[0]:,}")
    print(f"   - Total biopsy payments: {table_stats['total_biopsy_payments'].iloc[0]:,}")
    print(f"   - Total biopsy missing payments: {table_stats['total_biopsy_missing_payment'].iloc[0]:,}")
    print(f"   - Total PGT-A tests: {table_stats['total_pgta_tests'].iloc[0]:,}")
    print(f"   - Max cumulative PGT-A tests per patient: {table_stats['max_cumulative_pgta_tests'].iloc[0]:,}")
    print(f"   - Total PGT-A payments: {table_stats['total_pgta_payments'].iloc[0]:,}")
    print(f"   - Total PGT-A missing payments: {table_stats['total_pgta_missing_payment'].iloc[0]:,}")
    
    return table_stats

def analyze_comprehensive_timeline_data(conn):
    """Analyze the data in the comprehensive timeline table"""
    
    print("\nAnalyzing comprehensive_biopsy_pgta_timeline data...")
    
    # Sample data for recent months
    sample_data = conn.execute("""
        SELECT *
        FROM gold.comprehensive_biopsy_pgta_timeline
        ORDER BY period_month DESC, prontuario
        LIMIT 15
    """).fetchdf()
    
    print("\nSample Data (First 15 records, recent months):")
    print(sample_data.to_string(index=False))
    
    # Patients with missing payments
    missing_payment_patients = conn.execute("""
        SELECT 
            prontuario,
            MAX(cumulative_biopsy_count) as total_biopsies,
            MAX(cumulative_biopsy_payment_qtd) as total_biopsy_payment_qtd,
            MAX(biopsy_missing_payment) as biopsy_missing_payment,
            MAX(cumulative_pgta_test_count) as total_pgta_tests,
            MAX(cumulative_pgta_payment_qtd) as total_pgta_payment_qtd,
            MAX(pgta_missing_payment) as pgta_missing_payment,
            COUNT(*) as months_with_activity
        FROM gold.comprehensive_biopsy_pgta_timeline
        WHERE biopsy_missing_payment > 0 OR pgta_missing_payment > 0
        GROUP BY prontuario
        ORDER BY (MAX(biopsy_missing_payment) + MAX(pgta_missing_payment)) DESC
        LIMIT 10
    """).fetchdf()
    
    print(f"\nTop 10 Patients with Missing Payments:")
    print(missing_payment_patients.to_string(index=False))
    
    # Monthly summary for last 12 months
    monthly_summary = conn.execute("""
        SELECT 
            period_month,
            SUM(biopsy_count) as total_biopsies,
            SUM(biopsy_payment_count) as total_biopsy_payments,
            SUM(pgta_test_count) as total_pgta_tests,
            SUM(pgta_payment_count) as total_pgta_payments,
            COUNT(DISTINCT prontuario) as active_patients
        FROM gold.comprehensive_biopsy_pgta_timeline
        WHERE period_month >= STRFTIME(CURRENT_DATE - INTERVAL 12 MONTH, '%Y-%m')
        GROUP BY period_month
        ORDER BY period_month DESC
        LIMIT 12
    """).fetchdf()
    
    print(f"\nMonthly Summary (Last 12 months):")
    print(monthly_summary.to_string(index=False))

def create_resumed_timeline_table(conn):
    """Create a resumed timeline table with only the most recent record per patient"""
    print("Creating gold.resumed_biopsy_pgta_timeline table...")
    
    # Drop table if it exists
    conn.execute("DROP TABLE IF EXISTS gold.resumed_biopsy_pgta_timeline")
    
    # Create the resumed timeline table
    create_table_query = """
    CREATE TABLE gold.resumed_biopsy_pgta_timeline AS
    WITH latest_records AS (
        SELECT 
            prontuario,
            MAX(period_month) as latest_month
        FROM gold.comprehensive_biopsy_pgta_timeline
        GROUP BY prontuario
    )
    SELECT 
        ct.*
    FROM gold.comprehensive_biopsy_pgta_timeline ct
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
            SUM(cumulative_biopsy_count) as total_biopsies,
            SUM(cumulative_pgta_test_count) as total_pgta_tests,
            SUM(cumulative_biopsy_payment_qtd) as total_biopsy_payments,
            SUM(cumulative_pgta_payment_qtd) as total_pgta_payments,
            SUM(biopsy_missing_payment) as total_biopsy_missing_payment,
            SUM(pgta_missing_payment) as total_pgta_missing_payment,
            MAX(cumulative_biopsy_count) as max_biopsies_per_patient,
            MAX(cumulative_pgta_test_count) as max_pgta_per_patient
        FROM gold.resumed_biopsy_pgta_timeline
    """).fetchdf()
    
    print("Table created successfully.")
    print("Table Statistics:")
    print(f"   - Total records: {table_stats['total_records'].iloc[0]:,}")
    print(f"   - Unique patients: {table_stats['unique_patients'].iloc[0]:,}")
    print(f"   - Period range: {table_stats['earliest_month'].iloc[0]} to {table_stats['latest_month'].iloc[0]}")
    print(f"   - Total biopsies: {table_stats['total_biopsies'].iloc[0]:,}")
    print(f"   - Total PGT-A tests: {table_stats['total_pgta_tests'].iloc[0]:,}")
    print(f"   - Total biopsy payments: {table_stats['total_biopsy_payments'].iloc[0]:,}")
    print(f"   - Total PGT-A payments: {table_stats['total_pgta_payments'].iloc[0]:,}")
    print(f"   - Total biopsy missing payments: {table_stats['total_biopsy_missing_payment'].iloc[0]:,}")
    print(f"   - Total PGT-A missing payments: {table_stats['total_pgta_missing_payment'].iloc[0]:,}")
    print(f"   - Max biopsies per patient: {table_stats['max_biopsies_per_patient'].iloc[0]:,}")
    print(f"   - Max PGT-A tests per patient: {table_stats['max_pgta_per_patient'].iloc[0]:,}")
    
    return table_stats

def analyze_resumed_timeline_data(conn):
    """Analyze the data in the resumed timeline table"""
    print("\nAnalyzing resumed timeline data...")
    
    # Sample data
    sample_data = conn.execute("""
        SELECT 
            prontuario,
            period_month,
            cumulative_biopsy_count,
            cumulative_pgta_test_count,
            cumulative_biopsy_payment_qtd,
            cumulative_pgta_payment_qtd,
            biopsy_missing_payment,
            pgta_missing_payment
        FROM gold.resumed_biopsy_pgta_timeline
        ORDER BY (biopsy_missing_payment + pgta_missing_payment) DESC
        LIMIT 15
    """).fetchdf()
    
    print("\nSample Data (Top 15 patients by missing payments):")
    print(sample_data.to_string(index=False))
    
    # Patients with highest missing payments
    high_missing = conn.execute("""
        SELECT 
            prontuario,
            cumulative_biopsy_count as total_biopsies,
            cumulative_pgta_test_count as total_pgta_tests,
            cumulative_biopsy_payment_qtd as total_biopsy_payments,
            cumulative_pgta_payment_qtd as total_pgta_payments,
            biopsy_missing_payment,
            pgta_missing_payment,
            period_month as latest_month
        FROM gold.resumed_biopsy_pgta_timeline
        WHERE biopsy_missing_payment > 0 OR pgta_missing_payment > 0
        ORDER BY (biopsy_missing_payment + pgta_missing_payment) DESC
        LIMIT 20
    """).fetchdf()
    
    print(f"\nTop 20 Patients with Missing Payments (Resumed View):")
    print(high_missing.to_string(index=False))
    
    # Monthly distribution of latest records
    monthly_dist = conn.execute("""
        SELECT 
            period_month,
            COUNT(*) as patients_with_latest_record,
            SUM(cumulative_biopsy_count) as total_biopsies,
            SUM(cumulative_pgta_test_count) as total_pgta_tests,
            SUM(biopsy_missing_payment) as total_biopsy_missing,
            SUM(pgta_missing_payment) as total_pgta_missing
        FROM gold.resumed_biopsy_pgta_timeline
        GROUP BY period_month
        ORDER BY period_month DESC
        LIMIT 12
    """).fetchdf()
    
    print(f"\nMonthly Distribution of Latest Records (Last 12 months):")
    print(monthly_dist.to_string(index=False))

def create_patient_info_table(conn):
    """Create a patient info table with medico and unidade information"""
    print("Creating gold.patient_info table...")
    
    # Drop table if it exists
    conn.execute("DROP TABLE IF EXISTS gold.patient_info")
    
    # Create the patient info table
    create_table_query = """
    CREATE TABLE gold.patient_info AS
    WITH
    -- Get all unique patients from timeline tables
    all_patients AS (
        SELECT DISTINCT prontuario FROM gold.biopsy_pgta_timeline
        UNION
        SELECT DISTINCT prontuario FROM gold.billing_timeline
    ),
    
    -- Get medico information for each patient
    medico_info AS (
        SELECT DISTINCT
            ap.prontuario,
            COALESCE(m.nome, 'Não informado') as medico_nome,
            ROW_NUMBER() OVER (PARTITION BY ap.prontuario ORDER BY tr.data_tratamento DESC) as rn
        FROM all_patients ap
        LEFT JOIN clinisys_all.silver.view_tratamentos tr ON ap.prontuario = tr.prontuario
        LEFT JOIN clinisys_all.silver.view_medicos m ON tr.responsavel_informacoes = m.id
        WHERE tr.responsavel_informacoes IS NOT NULL
    ),
    
    -- Get unidade information for each patient
    unidade_info AS (
        SELECT DISTINCT
            ap.prontuario,
            COALESCE(u.nome, 'Não informado') as unidade_nome
        FROM all_patients ap
        LEFT JOIN clinisys_all.silver.view_pacientes p ON ap.prontuario = p.prontuario
        LEFT JOIN clinisys_all.silver.view_unidades u ON p.unidade_origem = u.id
    )
    
    SELECT 
        ap.prontuario,
        COALESCE(med.medico_nome, 'Não informado') as medico_nome,
        COALESCE(uni.unidade_nome, 'Não informado') as unidade_nome
    FROM all_patients ap
    LEFT JOIN (
        SELECT prontuario, medico_nome 
        FROM medico_info 
        WHERE rn = 1
    ) med ON ap.prontuario = med.prontuario
    LEFT JOIN unidade_info uni ON ap.prontuario = uni.prontuario
    ORDER BY ap.prontuario
    """
    
    conn.execute(create_table_query)
    
    # Get statistics
    table_stats = conn.execute("""
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT prontuario) as unique_patients,
            COUNT(DISTINCT medico_nome) as unique_medicos,
            COUNT(DISTINCT unidade_nome) as unique_unidades
        FROM gold.patient_info
    """).fetchdf()
    
    print("Table created successfully.")
    print("Table Statistics:")
    print(f"   - Total records: {table_stats['total_records'].iloc[0]:,}")
    print(f"   - Unique patients: {table_stats['unique_patients'].iloc[0]:,}")
    print(f"   - Unique medicos: {table_stats['unique_medicos'].iloc[0]:,}")
    print(f"   - Unique unidades: {table_stats['unique_unidades'].iloc[0]:,}")
    
    # Show sample data
    sample_data = conn.execute("""
        SELECT * FROM gold.patient_info 
        ORDER BY prontuario 
        LIMIT 10
    """).fetchdf()
    
    print("\nSample Data (First 10 patients):")
    print(sample_data.to_string(index=False))
    
    return table_stats

def main():
    """Main function to create all timeline tables"""
    
    print("=== CREATING COMPREHENSIVE BIOPSY PGT-A AND BILLING TIMELINE TABLES ===")
    print(f"Timestamp: {datetime.now()}")
    print()
    
    try:
        # Connect to database
        print("Connecting to database...")
        conn = get_database_connection()
        
        # Create the biopsy PGT-A timeline table
        print("Creating biopsy PGT-A timeline table...")
        biopsy_stats = create_biopsy_pgta_timeline_table(conn)
        
        # Analyze the biopsy data
        analyze_biopsy_pgta_timeline_data(conn)
        
        print(f"\nSuccessfully created gold.biopsy_pgta_timeline table")
        print(f"Table contains {biopsy_stats['unique_patients'].iloc[0]:,} unique patients")
        print(f"Total timeline records: {biopsy_stats['total_records'].iloc[0]:,}")
        print(f"Period: {biopsy_stats['earliest_month'].iloc[0]} to {biopsy_stats['latest_month'].iloc[0]}")
        print(f"Total biopsies: {biopsy_stats['total_biopsies'].iloc[0]:,}")
        print(f"Total PGT-A tests: {biopsy_stats['total_pgta_tests'].iloc[0]:,}")
        
        print("\n" + "="*80)
        
        # Create the billing timeline table
        print("Creating billing timeline table...")
        billing_stats = create_billing_timeline_table(conn)
        
        # Analyze the billing data
        analyze_billing_timeline_data(conn)
        
        print(f"\nSuccessfully created gold.billing_timeline table")
        print(f"Table contains {billing_stats['unique_patients'].iloc[0]:,} unique patients")
        print(f"Total timeline records: {billing_stats['total_records'].iloc[0]:,}")
        print(f"Period: {billing_stats['earliest_month'].iloc[0]} to {billing_stats['latest_month'].iloc[0]}")
        print(f"Total biopsy payments: {billing_stats['total_biopsy_payments'].iloc[0]:,}")
        print(f"Total biopsy amount: R$ {billing_stats['total_biopsy_payment_amount'].iloc[0]:,.2f}")
        print(f"Total PGT-A payments: {billing_stats['total_pgta_payments'].iloc[0]:,}")
        print(f"Total PGT-A amount: R$ {billing_stats['total_pgta_payment_amount'].iloc[0]:,.2f}")
        
        print("\n" + "="*80)
        
        # Create the comprehensive timeline table
        print("Creating comprehensive timeline table...")
        comprehensive_stats = create_comprehensive_timeline_table(conn)
        
        # Analyze the comprehensive data
        analyze_comprehensive_timeline_data(conn)
        
        print(f"\nSuccessfully created gold.comprehensive_biopsy_pgta_timeline table")
        print(f"Table contains {comprehensive_stats['unique_patients'].iloc[0]:,} unique patients")
        print(f"Total timeline records: {comprehensive_stats['total_records'].iloc[0]:,}")
        print(f"Period: {comprehensive_stats['earliest_month'].iloc[0]} to {comprehensive_stats['latest_month'].iloc[0]}")
        print(f"Total biopsy missing payments: {comprehensive_stats['total_biopsy_missing_payment'].iloc[0]:,}")
        print(f"Total PGT-A missing payments: {comprehensive_stats['total_pgta_missing_payment'].iloc[0]:,}")
        
        # Create resumed timeline (most recent record per patient)
        print("\n" + "="*80)
        print("Creating resumed timeline table...")
        resumed_stats = create_resumed_timeline_table(conn)
        
        # Analyze the resumed data
        analyze_resumed_timeline_data(conn)
        
        print(f"\nSuccessfully created gold.resumed_biopsy_pgta_timeline table")
        print(f"Table contains {resumed_stats['unique_patients'].iloc[0]:,} unique patients")
        print(f"Total records: {resumed_stats['total_records'].iloc[0]:,}")
        print(f"Total biopsy missing payments: {resumed_stats['total_biopsy_missing_payment'].iloc[0]:,}")
        print(f"Total PGT-A missing payments: {resumed_stats['total_pgta_missing_payment'].iloc[0]:,}")
        
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

