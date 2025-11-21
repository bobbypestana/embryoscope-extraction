#!/usr/bin/env python3
"""
Create All Patient Timeline
Creates unified timelines for ALL patients by combining data from multiple tables using DuckDB SQL.
"""

import duckdb as db
import os
import pandas as pd
from datetime import datetime
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_database_connection():
    """Create and return a connection to the clinisys_all database"""
    # Resolve DB path relative to repository root
    repo_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    path_to_db = os.path.join(repo_root, 'database', 'clinisys_all.duckdb')
    conn = db.connect(path_to_db, read_only=True)
    
    logger.info(f"Connected to database: {path_to_db}")
    logger.info(f"Database file exists: {os.path.exists(path_to_db)}")
    
    return conn

def get_all_patients(conn):
    """Get all unique patients from all relevant tables"""
    logger.info("Extracting all unique patients from all tables...")
    
    patients_query = """
    SELECT DISTINCT prontuario 
    FROM (
        SELECT prontuario FROM silver.view_tratamentos WHERE prontuario IS NOT NULL
        UNION
        SELECT prontuario FROM silver.view_extrato_atendimentos_central WHERE prontuario IS NOT NULL
        UNION
        SELECT prontuario FROM silver.view_congelamentos_embrioes WHERE prontuario IS NOT NULL
        UNION
        SELECT prontuario FROM silver.view_congelamentos_ovulos WHERE prontuario IS NOT NULL
        UNION
        SELECT prontuario FROM silver.view_descongelamentos_embrioes WHERE prontuario IS NOT NULL
        UNION
        SELECT prontuario FROM silver.view_descongelamentos_ovulos WHERE prontuario IS NOT NULL
    ) all_patients
    WHERE prontuario IS NOT NULL
    ORDER BY prontuario
    """
    
    patients_df = conn.execute(patients_query).fetchdf()
    logger.info(f"Found {len(patients_df)} unique patients")
    return patients_df['prontuario'].tolist()

def create_all_patient_timeline_sql(conn):
    """
    Create unified timeline for ALL patients using SQL
    """
    logger.info("Creating unified timeline for ALL patients using SQL...")
    
    # Main SQL query that processes all patients at once
    timeline_sql = """
    WITH all_timeline_events AS (
        -- 1. Treatments (tratamentos)
        SELECT 
            t.prontuario,
            'tratamentos' as reference,
            t.id as event_id,
            CASE 
                WHEN t.data_procedimento IS NOT NULL THEN t.data_procedimento
                WHEN t.data_inicio_inducao IS NOT NULL THEN 
                    -- Add 14 days to data_inicio_inducao as fallback
                    t.data_inicio_inducao + INTERVAL 14 DAY
                ELSE NULL
            END as event_date,
            CASE 
                WHEN t.data_procedimento IS NOT NULL THEN FALSE
                WHEN t.data_inicio_inducao IS NOT NULL THEN TRUE
                ELSE FALSE
            END as flag_date_estimated,
            t.tipo_procedimento as reference_value,
            t.tentativa,
            COALESCE(u.nome, CAST(t.unidade AS VARCHAR)) as unidade,
            CAST(t.resultado_tratamento AS VARCHAR) as resultado_tratamento,
            '{}' as additional_info
        FROM silver.view_tratamentos t
        LEFT JOIN silver.view_unidades u ON t.unidade = u.id
        WHERE t.prontuario IS NOT NULL
        AND (
            t.data_procedimento IS NOT NULL 
            OR t.data_inicio_inducao IS NOT NULL
        )
        
        UNION ALL
        
        -- 2. Appointments (extrato_atendimentos) - only confirmed appointments with valid procedures
        SELECT 
            prontuario,
            'extrato_atendimentos' as reference,
            agendamento_id as event_id,
            data as event_date,
            FALSE as flag_date_estimated,
            procedimento_nome as reference_value,
            NULL as tentativa,
            centro_custos_nome as unidade,
            CAST(NULL AS VARCHAR) as resultado_tratamento,
            '{}' as additional_info
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY prontuario, data, procedimento_nome, confirmado 
                       ORDER BY agendamento_id
                   ) as rn
            FROM silver.view_extrato_atendimentos_central 
            WHERE prontuario IS NOT NULL
            AND data IS NOT NULL
            AND procedimento_nome IS NOT NULL
            AND confirmado = 1
        ) deduplicated_appointments
        WHERE rn = 1
        
        UNION ALL
        
        -- 3. Embryo Freezing (congelamentos_embrioes)
        SELECT 
            prontuario,
            'congelamentos_embrioes' as reference,
            id as event_id,
            Data as event_date,
            FALSE as flag_date_estimated,
            Ciclo as reference_value,
            NULL as tentativa,
            NULL as unidade,
            CAST(NULL AS VARCHAR) as resultado_tratamento,
            CASE 
                WHEN NEmbrioes IS NOT NULL OR Unidade IS NOT NULL 
                THEN '{"NEmbrioes": "' || COALESCE(CAST(NEmbrioes AS VARCHAR), '') || '", "Unidade": "' || COALESCE(CAST(Unidade AS VARCHAR), '') || '"}'
                ELSE '{}'
            END as additional_info
        FROM silver.view_congelamentos_embrioes 
        WHERE prontuario IS NOT NULL
        AND Data IS NOT NULL
        
        UNION ALL
        
        -- 4. Oocyte Freezing (congelamentos_ovulos)
        SELECT 
            prontuario,
            'congelamentos_ovulos' as reference,
            id as event_id,
            Data as event_date,
            FALSE as flag_date_estimated,
            Ciclo as reference_value,
            NULL as tentativa,
            NULL as unidade,
            CAST(NULL AS VARCHAR) as resultado_tratamento,
            CASE 
                WHEN NOvulos IS NOT NULL OR Unidade IS NOT NULL 
                THEN '{"NOvulos": "' || COALESCE(CAST(NOvulos AS VARCHAR), '') || '", "Unidade": "' || COALESCE(CAST(Unidade AS VARCHAR), '') || '"}'
                ELSE '{}'
            END as additional_info
        FROM silver.view_congelamentos_ovulos 
        WHERE prontuario IS NOT NULL
        AND Data IS NOT NULL
        
        UNION ALL
        
        -- 5. Embryo Thawing (descongelamentos_embrioes)
        SELECT 
            prontuario,
            'descongelamentos_embrioes' as reference,
            id as event_id,
            DataDescongelamento as event_date,
            FALSE as flag_date_estimated,
            Ciclo as reference_value,
            NULL as tentativa,
            NULL as unidade,
            CAST(NULL AS VARCHAR) as resultado_tratamento,
            CASE 
                WHEN CodDescongelamento IS NOT NULL OR Unidade IS NOT NULL 
                THEN '{"CodDescongelamento": "' || COALESCE(CodDescongelamento, '') || '", "Unidade": "' || COALESCE(CAST(Unidade AS VARCHAR), '') || '"}'
                ELSE '{}'
            END as additional_info
        FROM silver.view_descongelamentos_embrioes 
        WHERE prontuario IS NOT NULL
        AND DataDescongelamento IS NOT NULL
        
        UNION ALL
        
        -- 6. Oocyte Thawing (descongelamentos_ovulos)
        SELECT 
            prontuario,
            'descongelamentos_ovulos' as reference,
            id as event_id,
            DataDescongelamento as event_date,
            FALSE as flag_date_estimated,
            Ciclo as reference_value,
            NULL as tentativa,
            NULL as unidade,
            CAST(NULL AS VARCHAR) as resultado_tratamento,
            CASE 
                WHEN CodDescongelamento IS NOT NULL OR Unidade IS NOT NULL 
                THEN '{"CodDescongelamento": "' || COALESCE(CodDescongelamento, '') || '", "Unidade": "' || COALESCE(CAST(Unidade AS VARCHAR), '') || '"}'
                ELSE '{}'
            END as additional_info
        FROM silver.view_descongelamentos_ovulos 
        WHERE prontuario IS NOT NULL
        AND DataDescongelamento IS NOT NULL
        
    ),
    
    timeline_with_order AS (
        SELECT 
            *,
            -- Define table hierarchy for sorting (same as Python logic)
            CASE reference
                WHEN 'extrato_atendimentos' THEN 1
                WHEN 'congelamentos_ovulos' THEN 2
                WHEN 'descongelamentos_ovulos' THEN 3
                WHEN 'congelamentos_embrioes' THEN 4
                WHEN 'descongelamentos_embrioes' THEN 5
                WHEN 'tratamentos' THEN 6
                ELSE 7
            END as table_order
        FROM all_timeline_events
        WHERE event_date IS NOT NULL
    )
    
    SELECT 
        prontuario,
        event_id,
        event_date,
        reference,
        reference_value,
        tentativa,
        unidade,
        resultado_tratamento,
        flag_date_estimated,
        additional_info
    FROM timeline_with_order
    ORDER BY 
        prontuario,
        event_date DESC,
        table_order DESC,
        event_id DESC
    """
    
    logger.info("Executing timeline creation SQL...")
    timeline_df = conn.execute(timeline_sql).fetchdf()
    
    logger.info(f"Created timeline with {len(timeline_df)} total events")
    logger.info(f"Date range: {timeline_df['event_date'].min()} to {timeline_df['event_date'].max()}")
    
    # Get patient count
    unique_patients = timeline_df['prontuario'].nunique()
    logger.info(f"Timeline covers {unique_patients} unique patients")
    
    return timeline_df

def save_timeline_to_database(timeline_df):
    """Save timeline data to huntington_data_lake database in gold schema"""
    
    if timeline_df.empty:
        logger.warning("No timeline data to save")
        return
    
    logger.info("Saving timeline to huntington_data_lake database...")
    
    try:
        # Connect to huntington_data_lake database
        repo_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
        path_to_db = os.path.join(repo_root, 'database', 'huntington_data_lake.duckdb')
        conn = db.connect(path_to_db)
        
        logger.info(f"Connected to database: {path_to_db}")
        
        # Create gold schema if it doesn't exist
        conn.execute("CREATE SCHEMA IF NOT EXISTS gold")
        
        # Drop existing table and create new one
        conn.execute("DROP TABLE IF EXISTS gold.all_patients_timeline")
        logger.info("Dropped existing table (if any)")
        
        # Create new table with current data
        # Explicitly cast resultado_tratamento to VARCHAR to preserve type
        conn.execute("""
            CREATE TABLE gold.all_patients_timeline AS 
            SELECT 
                prontuario,
                event_id,
                event_date,
                reference,
                reference_value,
                tentativa,
                unidade,
                CAST(resultado_tratamento AS VARCHAR) as resultado_tratamento,
                flag_date_estimated,
                additional_info
            FROM timeline_df
        """)
        
        # Get row count
        count_result = conn.execute("SELECT COUNT(*) as row_count FROM gold.all_patients_timeline").fetchdf()
        row_count = count_result['row_count'].iloc[0]
        
        # Get patient count
        patient_count_result = conn.execute("SELECT COUNT(DISTINCT prontuario) as patient_count FROM gold.all_patients_timeline").fetchdf()
        patient_count = patient_count_result['patient_count'].iloc[0]
        
        logger.info(f"Successfully saved {len(timeline_df)} timeline events to gold.all_patients_timeline")
        logger.info(f"Total events in table: {row_count}")
        logger.info(f"Total patients in table: {patient_count}")
        
        # Show sample of saved data
        sample_data = conn.execute("SELECT * FROM gold.all_patients_timeline LIMIT 5").fetchdf()
        logger.info(f"Sample of saved data:")
        logger.info(sample_data.to_string(index=False))
        
        conn.close()
        
    except Exception as e:
        logger.error(f"Error saving timeline to database: {str(e)}")
        if 'conn' in locals():
            conn.close()

def display_timeline_summary(timeline_df):
    """Display summary statistics of the timeline"""
    
    if timeline_df.empty:
        logger.warning("No timeline data available")
        return
    
    logger.info("="*80)
    logger.info("TIMELINE SUMMARY")
    logger.info("="*80)
    
    # Basic statistics
    total_events = len(timeline_df)
    unique_patients = timeline_df['prontuario'].nunique()
    date_range_start = timeline_df['event_date'].min()
    date_range_end = timeline_df['event_date'].max()
    
    logger.info(f"Total events: {total_events:,}")
    logger.info(f"Unique patients: {unique_patients:,}")
    logger.info(f"Date range: {date_range_start} to {date_range_end}")
    
    # Events per table
    logger.info("\nEvents per table:")
    table_counts = timeline_df['reference'].value_counts()
    for table, count in table_counts.items():
        logger.info(f"  {table}: {count:,}")
    
    # Events per patient (summary)
    events_per_patient = timeline_df.groupby('prontuario').size()
    logger.info(f"\nEvents per patient:")
    logger.info(f"  Average: {events_per_patient.mean():.1f}")
    logger.info(f"  Median: {events_per_patient.median():.1f}")
    logger.info(f"  Min: {events_per_patient.min()}")
    logger.info(f"  Max: {events_per_patient.max()}")
    
    # Estimated dates count
    estimated_count = timeline_df['flag_date_estimated'].sum()
    logger.info(f"\nEstimated dates: {estimated_count:,} ({estimated_count/total_events*100:.1f}%)")
    
    logger.info("="*80)

def main():
    """Main function to create timeline for all patients"""
    
    logger.info("Starting all patient timeline creation...")
    
    # Get database connection
    conn = get_database_connection()
    
    try:
        # Get all patients
        all_patients = get_all_patients(conn)
        logger.info(f"Processing timeline for {len(all_patients)} patients")
        
        # Create unified timeline for all patients
        timeline_df = create_all_patient_timeline_sql(conn)
        
        # Display summary
        display_timeline_summary(timeline_df)
        
        # Save timeline to database
        save_timeline_to_database(timeline_df)
        
        logger.info("All patient timeline creation completed successfully!")
        
        return timeline_df
        
    finally:
        conn.close()

if __name__ == "__main__":
    timeline_df = main()
