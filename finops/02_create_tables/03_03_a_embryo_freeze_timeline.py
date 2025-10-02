#!/usr/bin/env python3
"""
Create gold.embryo_freeze_timeline table with monthly embryo freeze/unfreeze tracking per prontuario
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

def create_embryo_freeze_timeline_table(conn):
    """Create the gold.embryo_freeze_timeline table"""
    
    print("Creating gold.embryo_freeze_timeline table...")
    
    # Attach clinisys_all database
    repo_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    clinisys_db_path = os.path.join(repo_root, 'database', 'clinisys_all.duckdb')
    conn.execute(f"ATTACH '{clinisys_db_path}' AS clinisys_all")
    print(f"Attached clinisys_all database: {clinisys_db_path}")
    
    # Drop table if it exists
    conn.execute("DROP TABLE IF EXISTS gold.embryo_freeze_timeline")
    
    # Create the table with monthly embryo freeze/unfreeze tracking
    create_table_query = """
    CREATE TABLE gold.embryo_freeze_timeline AS
    WITH
    -- Identify patients with data quality issues to exclude
    problem_patients AS (
        -- Patients with congelamento problems
        SELECT DISTINCT prontuario
        FROM clinisys_all.silver.view_congelamentos_embrioes
        WHERE prontuario IS NOT NULL 
            AND (Data IS NULL 
                 OR Data > CURRENT_DATE 
                 OR Data < '2000-01-01'
                 OR NEmbrioes IS NULL 
                 OR NEmbrioes = 0)
        
        UNION
        
        -- Patients with descongelamento problems
        SELECT DISTINCT prontuario
        FROM clinisys_all.silver.view_descongelamentos_embrioes
        WHERE prontuario IS NOT NULL 
            AND (DataDescongelamento IS NULL 
                 OR DataCongelamento IS NULL 
                 OR DataDescongelamento > CURRENT_DATE 
                 OR DataCongelamento > CURRENT_DATE
                 OR DataDescongelamento < '2000-01-01'
                 OR DataCongelamento < '2000-01-01'
                 OR (DataCongelamento IS NOT NULL AND DataDescongelamento IS NOT NULL 
                     AND DataCongelamento > DataDescongelamento))
    ),
    -- Get freeze events by month (excluding problem patients)
    freeze_events AS (
        -- Regular freeze events from congelamentos_embrioes (excluding problem patients)
        SELECT 
            prontuario,
            STRFTIME(Data, '%Y-%m') as period_month,
            SUM(NEmbrioes) as embryos_frozen_count,
            0 as embryos_discarded_count
        FROM clinisys_all.silver.view_congelamentos_embrioes
        WHERE prontuario IS NOT NULL 
            AND Data IS NOT NULL
            AND NEmbrioes IS NOT NULL
            AND NEmbrioes > 0
            AND prontuario NOT IN (SELECT prontuario FROM problem_patients)
        GROUP BY prontuario, STRFTIME(Data, '%Y-%m')
        
        UNION ALL
        
        -- Historical freeze events inferred from descongelamentos_embrioes
        -- where no freeze record exists for that date (excluding problem patients)
        SELECT 
            d.prontuario,
            STRFTIME(d.DataCongelamento, '%Y-%m') as period_month,
            COUNT(*) as embryos_frozen_count,
            0 as embryos_discarded_count  -- No embryo tracking for inferred events
        FROM clinisys_all.silver.view_descongelamentos_embrioes d
        LEFT JOIN clinisys_all.silver.view_congelamentos_embrioes c
            ON d.prontuario = c.prontuario 
            AND STRFTIME(c.Data, '%Y-%m') = STRFTIME(d.DataCongelamento, '%Y-%m')
        WHERE d.prontuario IS NOT NULL 
            AND d.DataCongelamento IS NOT NULL
            AND c.prontuario IS NULL  -- No freeze record exists for this date
            AND d.prontuario NOT IN (SELECT prontuario FROM problem_patients)
        GROUP BY d.prontuario, STRFTIME(d.DataCongelamento, '%Y-%m')
    ),
    -- Get discarded embryo counts by month
    discarded_events AS (
        SELECT 
            c.prontuario,
            STRFTIME(c.Data, '%Y-%m') as period_month,
            0 as embryos_frozen_count,
            COUNT(CASE WHEN e.transferidos IN ('Descartado', 'Transferido para outra Clínica', 'Doado para Pesquisa') 
                          AND (e.id_descongelamento IS NULL OR e.id_descongelamento = 0) THEN 1 END) as embryos_discarded_count
        FROM clinisys_all.silver.view_congelamentos_embrioes c
        LEFT JOIN clinisys_all.silver.view_embrioes_congelados e ON c.id = e.id_congelamento
        WHERE c.prontuario IS NOT NULL 
            AND c.Data IS NOT NULL
            AND c.NEmbrioes IS NOT NULL
            AND c.NEmbrioes > 0
            AND c.prontuario NOT IN (SELECT prontuario FROM problem_patients)
        GROUP BY c.prontuario, STRFTIME(c.Data, '%Y-%m')
        HAVING COUNT(CASE WHEN e.transferidos IN ('Descartado', 'Transferido para outra Clínica', 'Doado para Pesquisa') 
                          AND (e.id_descongelamento IS NULL OR e.id_descongelamento = 0) THEN 1 END) > 0
    ),
    -- Get all unfreeze events by month (each line = 1 embryo unfrozen, excluding problem patients)
    -- Only count embryos that were actually transferred (transferidos_transferencia = 1)
    unfreeze_events AS (
        SELECT 
            prontuario,
            STRFTIME(DataDescongelamento, '%Y-%m') as period_month,
            0 as embryos_frozen_count,
            COUNT(*) as embryos_unfrozen_count
        FROM clinisys_all.silver.view_descongelamentos_embrioes
        WHERE prontuario IS NOT NULL 
            AND DataDescongelamento IS NOT NULL
            AND prontuario NOT IN (SELECT prontuario FROM problem_patients)
        GROUP BY prontuario, STRFTIME(DataDescongelamento, '%Y-%m')
    ),
    -- Aggregate freeze events (in case we have both regular and inferred events for same month)
    aggregated_freeze_events AS (
        SELECT 
            prontuario,
            period_month,
            SUM(embryos_frozen_count) as embryos_frozen_count,
            SUM(embryos_discarded_count) as embryos_discarded_count
        FROM freeze_events
        GROUP BY prontuario, period_month
    ),
    -- Get all unique prontuario-month combinations
    all_periods AS (
        SELECT prontuario, period_month FROM aggregated_freeze_events
        UNION
        SELECT prontuario, period_month FROM unfreeze_events
        UNION
        SELECT prontuario, period_month FROM discarded_events
    ),
    -- Calculate monthly changes (without net change)
    monthly_changes AS (
        SELECT 
            ap.prontuario,
            ap.period_month,
            COALESCE(f.embryos_frozen_count, 0) as embryos_frozen_count,
            COALESCE(u.embryos_unfrozen_count, 0) as embryos_unfrozen_count,
            COALESCE(d.embryos_discarded_count, 0) as embryos_discarded_count
        FROM all_periods ap
        LEFT JOIN aggregated_freeze_events f ON ap.prontuario = f.prontuario AND ap.period_month = f.period_month
        LEFT JOIN unfreeze_events u ON ap.prontuario = u.prontuario AND ap.period_month = u.period_month
        LEFT JOIN discarded_events d ON ap.prontuario = d.prontuario AND ap.period_month = d.period_month
    ),
    -- Calculate cumulative running total (ordered by period_month DESC for proper calculation)
    timeline_with_balance AS (
        SELECT 
            prontuario,
            period_month,
            embryos_frozen_count,
            embryos_unfrozen_count,
            embryos_discarded_count,
            SUM(embryos_frozen_count - embryos_unfrozen_count - embryos_discarded_count) OVER (
                PARTITION BY prontuario 
                ORDER BY period_month 
                ROWS UNBOUNDED PRECEDING
            ) as embryos_storage_balance
        FROM monthly_changes
        ORDER BY prontuario, period_month
    )
    SELECT 
        prontuario,
        period_month,
        embryos_frozen_count,
        embryos_unfrozen_count,
        embryos_discarded_count,
        embryos_storage_balance
    FROM timeline_with_balance
    ORDER BY prontuario, period_month DESC
    """
    
    conn.execute(create_table_query)
    
    # Verify the table was created and check exclusion impact
    table_stats = conn.execute("""
        SELECT 
            COUNT(DISTINCT prontuario) as unique_patients,
            COUNT(*) as total_records,
            MIN(period_month) as earliest_month,
            MAX(period_month) as latest_month,
            SUM(embryos_frozen_count) as total_embryos_frozen,
            SUM(embryos_unfrozen_count) as total_embryos_unfrozen,
            SUM(embryos_discarded_count) as total_embryos_discarded,
            AVG(embryos_storage_balance) as avg_storage_balance,
            MAX(embryos_storage_balance) as max_storage_balance,
            MIN(embryos_storage_balance) as min_storage_balance,
            COUNT(CASE WHEN embryos_storage_balance < 0 THEN 1 END) as negative_balance_records
        FROM gold.embryo_freeze_timeline
    """).fetchdf()
    
    # Get exclusion statistics
    exclusion_stats = conn.execute("""
        WITH problem_patients AS (
            SELECT DISTINCT prontuario
            FROM clinisys_all.silver.view_congelamentos_embrioes
            WHERE prontuario IS NOT NULL 
                AND (Data IS NULL 
                     OR Data > CURRENT_DATE 
                     OR Data < '2000-01-01'
                     OR NEmbrioes IS NULL 
                     OR NEmbrioes = 0)
            
            UNION
            
            SELECT DISTINCT prontuario
            FROM clinisys_all.silver.view_descongelamentos_embrioes
            WHERE prontuario IS NOT NULL 
                AND (DataDescongelamento IS NULL 
                     OR DataCongelamento IS NULL 
                     OR DataDescongelamento > CURRENT_DATE 
                     OR DataCongelamento > CURRENT_DATE
                     OR DataDescongelamento < '2000-01-01'
                     OR DataCongelamento < '2000-01-01'
                     OR (DataCongelamento IS NOT NULL AND DataDescongelamento IS NOT NULL 
                         AND DataCongelamento > DataDescongelamento))
        )
        SELECT 
            COUNT(*) as excluded_patients
        FROM problem_patients
    """).fetchdf()
    
    print("Table created successfully.")
    print("Table Statistics:")
    print(f"   - Unique patients: {table_stats['unique_patients'].iloc[0]:,}")
    print(f"   - Total timeline records: {table_stats['total_records'].iloc[0]:,}")
    print(f"   - Period range: {table_stats['earliest_month'].iloc[0]} to {table_stats['latest_month'].iloc[0]}")
    print(f"   - Total embryos frozen: {table_stats['total_embryos_frozen'].iloc[0]:,}")
    print(f"   - Total embryos unfrozen: {table_stats['total_embryos_unfrozen'].iloc[0]:,}")
    print(f"   - Total embryos discarded: {table_stats['total_embryos_discarded'].iloc[0]:,}")
    print(f"   - Average storage balance: {table_stats['avg_storage_balance'].iloc[0]:.1f}")
    print(f"   - Max storage balance: {table_stats['max_storage_balance'].iloc[0]:.0f}")
    print(f"   - Min storage balance: {table_stats['min_storage_balance'].iloc[0]:.0f}")
    print(f"   - Negative balance records: {table_stats['negative_balance_records'].iloc[0]:,}")
    print(f"   - Excluded patients (data quality issues): {exclusion_stats['excluded_patients'].iloc[0]:,}")
    
    return table_stats

def analyze_embryo_timeline_data(conn):
    """Analyze the data in the embryo_freeze_timeline table"""
    
    print("\nAnalyzing embryo_freeze_timeline data...")
    
    # Sample data for patients with activity
    sample_data = conn.execute("""
        SELECT *
        FROM gold.embryo_freeze_timeline
        WHERE embryos_frozen_count > 0 OR embryos_unfrozen_count > 0 OR embryos_discarded_count > 0
        ORDER BY prontuario, period_month
        LIMIT 15
    """).fetchdf()
    
    print("\nSample Data (First 15 records with freeze/unfreeze activity):")
    print(sample_data.to_string(index=False))
    
    # Patients with negative balance (more unfrozen than frozen)
    negative_balance = conn.execute("""
        SELECT 
            prontuario,
            COUNT(*) as months_with_negative_balance,
            MIN(embryos_storage_balance) as worst_balance
        FROM gold.embryo_freeze_timeline
        WHERE embryos_storage_balance < 0
        GROUP BY prontuario
        ORDER BY worst_balance
        LIMIT 10
    """).fetchdf()
    
    print(f"\nPatients with Negative Storage Balance (Top 10):")
    print(negative_balance.to_string(index=False))
    
    # Monthly activity summary
    monthly_summary = conn.execute("""
        SELECT 
            period_month,
            COUNT(DISTINCT prontuario) as active_patients,
            SUM(embryos_frozen_count) as total_frozen,
            SUM(embryos_unfrozen_count) as total_unfrozen,
            SUM(embryos_frozen_count - embryos_unfrozen_count) as net_change
        FROM gold.embryo_freeze_timeline
        GROUP BY period_month
        ORDER BY period_month DESC
        LIMIT 12
    """).fetchdf()
    
    print(f"\nMonthly Activity Summary (Last 12 months):")
    print(monthly_summary.to_string(index=False))

def create_embryo_billing_timeline_table(conn):
    """Create the gold.embryo_billing_timeline table"""
    
    print("Creating gold.embryo_billing_timeline table...")
    
    # Drop table if it exists
    conn.execute("DROP TABLE IF EXISTS gold.embryo_billing_timeline")
    
    # Create the table with monthly embryo billing tracking
    create_table_query = """
    CREATE TABLE gold.embryo_billing_timeline AS
    WITH
    -- Get embryo cryopreservation billing data (monthly fees and maintenance)
    embryo_billing AS (
        SELECT 
            prontuario,
            STRFTIME("DT Emissao", '%Y-%m') as period_month,
            Descricao,
            Total as billing_amount,
            "Qntd." as quantity
        FROM silver.mesclada_vendas
        WHERE "Descrição Gerencial" = 'Criopreservação'
            AND prontuario IS NOT NULL
            AND prontuario != -1
            AND "DT Emissao" IS NOT NULL
            AND Total IS NOT NULL
            AND (
                Descricao = 'MENSALIDADE CRIOPRESERVACAO DE EMBRIAO -'
                OR Descricao = 'MANUTENCAO DE CONGELAMENTO DE EMBRIOES'
            )
    ),
    
    -- Aggregate billing by month and patient
    monthly_billing AS (
        SELECT 
            prontuario,
            period_month,
            COUNT(*) as billing_entries_count,
            SUM(billing_amount) as total_billing_amount,
            SUM(quantity) as total_quantity
        FROM embryo_billing
        GROUP BY prontuario, period_month
    )
    
    SELECT 
        prontuario,
        period_month,
        billing_entries_count,
        total_billing_amount,
        total_quantity
    FROM monthly_billing
    ORDER BY prontuario, period_month DESC
    """
    
    conn.execute(create_table_query)
    print("Table created successfully.")
    
    # Get table statistics
    stats = conn.execute("""
        SELECT 
            COUNT(DISTINCT prontuario) as unique_patients,
            COUNT(*) as total_records,
            MIN(period_month) as earliest_month,
            MAX(period_month) as latest_month,
            SUM(total_billing_amount) as total_billing_amount,
            AVG(total_billing_amount) as avg_monthly_billing,
            SUM(billing_entries_count) as total_billing_entries
        FROM gold.embryo_billing_timeline
    """).fetchdf()
    
    print("\\nTable Statistics:")
    for col in stats.columns:
        print(f"   - {col}: {stats.iloc[0][col]}")
    
    return True

def create_comprehensive_embryo_timeline_table(conn):
    """Create the gold.comprehensive_embryo_timeline table with complete monthly data"""
    
    print("Creating gold.comprehensive_embryo_timeline table...")
    
    # Drop table if it exists
    conn.execute("DROP TABLE IF EXISTS gold.comprehensive_embryo_timeline")
    
    # Create the comprehensive timeline table
    create_table_query = """
    CREATE TABLE gold.comprehensive_embryo_timeline AS
    WITH
    -- Get date range for all patients
    date_range AS (
        SELECT 
            MIN(period_date) as start_date,
            MAX(period_date) as end_date
        FROM (
            SELECT (period_month || '-01')::DATE as period_date
            FROM gold.embryo_freeze_timeline
            
            UNION ALL
            
            SELECT (period_month || '-01')::DATE as period_date
            FROM gold.embryo_billing_timeline
        )
    ),
    
    -- Generate all months from start to current (as proper dates)
    all_months AS (
        SELECT DISTINCT (period_month || '-01')::DATE as period_date
        FROM (
            SELECT period_month FROM gold.embryo_freeze_timeline
            UNION
            SELECT period_month FROM gold.embryo_billing_timeline
        )
    ),
    
    -- Get all unique patients from both tables
    all_patients AS (
        SELECT DISTINCT prontuario
        FROM gold.embryo_freeze_timeline
        
        UNION
        
        SELECT DISTINCT prontuario
        FROM gold.embryo_billing_timeline
    ),
    
    -- Get first and last event dates for each patient, plus current embryo balance
    patient_date_ranges AS (
        SELECT 
            p.prontuario,
            MIN(event_month) as first_month,
            MAX(event_month) as last_month,
            -- Get the most recent embryo storage balance for each patient
            COALESCE(MAX(CASE WHEN ef.embryos_storage_balance IS NOT NULL THEN ef.embryos_storage_balance END), 0) as current_embryo_balance
        FROM all_patients p
        LEFT JOIN (
            SELECT prontuario, period_month as event_month FROM gold.embryo_freeze_timeline
            UNION ALL
            SELECT prontuario, period_month as event_month FROM gold.embryo_billing_timeline
        ) events ON p.prontuario = events.prontuario
        LEFT JOIN gold.embryo_freeze_timeline ef ON p.prontuario = ef.prontuario
        GROUP BY p.prontuario
    ),
    
    -- Create patient-month matrix: extend to current month only if patient has stored embryos
    patient_month_matrix AS (
        SELECT 
            pdr.prontuario,
            am.period_date
        FROM patient_date_ranges pdr
        CROSS JOIN all_months am
        WHERE am.period_date >= (pdr.first_month || '-01')::DATE
            AND am.period_date <= CASE 
                WHEN pdr.current_embryo_balance > 0 THEN CURRENT_DATE
                ELSE (pdr.last_month || '-01')::DATE
            END
    ),
    
    -- Add embryo freeze data (with forward-fill for missing months)
    embryo_data_with_fill AS (
        SELECT 
            pmm.prontuario,
            pmm.period_date,
            COALESCE(ef.embryos_frozen_count, 0) as embryos_frozen_count,
            COALESCE(ef.embryos_unfrozen_count, 0) as embryos_unfrozen_count,
            COALESCE(ef.embryos_discarded_count, 0) as embryos_discarded_count,
            -- Forward-fill the storage balance using window functions
            LAST_VALUE(ef.embryos_storage_balance IGNORE NULLS) 
                OVER (PARTITION BY pmm.prontuario 
                      ORDER BY pmm.period_date 
                      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as embryos_storage_balance
        FROM patient_month_matrix pmm
        LEFT JOIN gold.embryo_freeze_timeline ef 
            ON pmm.prontuario = ef.prontuario 
            AND STRFTIME(pmm.period_date, '%Y-%m') = ef.period_month
    ),
    
    -- Add billing data (no forward-fill, 0 for missing months)
    billing_data AS (
        SELECT 
            pmm.prontuario,
            pmm.period_date,
            COALESCE(bt.billing_entries_count, 0) as billing_entries_count,
            COALESCE(bt.total_billing_amount, 0) as total_billing_amount,
            COALESCE(bt.total_quantity, 0) as total_quantity
        FROM patient_month_matrix pmm
        LEFT JOIN gold.embryo_billing_timeline bt 
            ON pmm.prontuario = bt.prontuario 
            AND STRFTIME(pmm.period_date, '%Y-%m') = bt.period_month
    )
    
    -- Final comprehensive timeline
    SELECT 
        ed.prontuario,
        DATE(ed.period_date) as period_month,
        ed.embryos_frozen_count,
        ed.embryos_unfrozen_count,
        ed.embryos_discarded_count,
        ed.embryos_storage_balance,
        bd.billing_entries_count,
        bd.total_billing_amount,
        bd.total_quantity,
        -- Additional calculated fields
        CASE 
            WHEN ed.embryos_storage_balance > 0 AND bd.total_billing_amount > 0 THEN 'Active with Payment'
            WHEN ed.embryos_storage_balance > 0 AND bd.total_billing_amount = 0 THEN 'Active without Payment'
            WHEN ed.embryos_storage_balance = 0 AND bd.total_billing_amount > 0 THEN 'Payment without Storage'
            ELSE 'Inactive'
        END as patient_status
    FROM embryo_data_with_fill ed
    JOIN billing_data bd 
        ON ed.prontuario = bd.prontuario 
        AND ed.period_date = bd.period_date
    ORDER BY ed.prontuario, ed.period_date DESC
    """
    
    conn.execute(create_table_query)
    print("Table created successfully.")
    
    # Get table statistics
    stats = conn.execute("""
        SELECT 
            COUNT(DISTINCT prontuario) as unique_patients,
            COUNT(*) as total_records,
            MIN(period_month) as earliest_month,
            MAX(period_month) as latest_month,
            SUM(embryos_storage_balance) as total_storage_balance,
            SUM(total_billing_amount) as total_billing_amount,
            COUNT(DISTINCT CASE WHEN embryos_storage_balance > 0 THEN prontuario END) as patients_with_storage,
            COUNT(DISTINCT CASE WHEN total_billing_amount > 0 THEN prontuario END) as patients_with_billing
        FROM gold.comprehensive_embryo_timeline
    """).fetchdf()
    
    print("\\nTable Statistics:")
    for col in stats.columns:
        value = stats.iloc[0][col]
        if 'amount' in col.lower():
            print(f"   - {col}: R$ {value:,.2f}")
        elif isinstance(value, (int, float)):
            print(f"   - {col}: {value:,.0f}")
        else:
            print(f"   - {col}: {value}")
    
    return True

def main():
    """Main function to create the embryo_freeze_timeline, embryo_billing_timeline, and comprehensive_embryo_timeline tables"""
    
    print("=== CREATING EMBRYO TIMELINE TABLES ===")
    print(f"Timestamp: {datetime.now()}")
    print()
    
    try:
        # Connect to database
        print("Connecting to database...")
        conn = get_database_connection()
        
        # Create the embryo freeze timeline table
        print("Creating embryo freeze timeline table...")
        table_stats = create_embryo_freeze_timeline_table(conn)
        
        # Analyze the data
        analyze_embryo_timeline_data(conn)
        
        print(f"\nSuccessfully created gold.embryo_freeze_timeline table")
        print(f"Table contains {table_stats['unique_patients'].iloc[0]:,} unique patients")
        print(f"Total timeline records: {table_stats['total_records'].iloc[0]:,}")
        print(f"Period: {table_stats['earliest_month'].iloc[0]} to {table_stats['latest_month'].iloc[0]}")
        print(f"Total embryos frozen: {table_stats['total_embryos_frozen'].iloc[0]:,}")
        print(f"Total embryos unfrozen: {table_stats['total_embryos_unfrozen'].iloc[0]:,}")
        
        print("\n" + "="*60)
        
        # Create the embryo billing timeline table
        print("Creating embryo billing timeline table...")
        create_embryo_billing_timeline_table(conn)
        
        print(f"\nSuccessfully created gold.embryo_billing_timeline table")
        
        print("\n" + "="*60)
        
        # Create the comprehensive embryo timeline table
        print("Creating comprehensive embryo timeline table...")
        create_comprehensive_embryo_timeline_table(conn)
        
        print(f"\nSuccessfully created gold.comprehensive_embryo_timeline table")
        
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
