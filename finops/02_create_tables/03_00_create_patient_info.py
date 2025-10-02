#!/usr/bin/env python3
"""
Create gold.patient_info table with medico and unidade information
"""

import duckdb as db
import pandas as pd
from datetime import datetime

def get_database_connection():
    """Create and return a connection to the huntington_data_lake database"""
    import os
    repo_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    path_to_db = os.path.join(repo_root, 'database', 'huntington_data_lake.duckdb')
    conn = db.connect(path_to_db)
    
    print(f"Connected to database: {path_to_db}")
    return conn

def create_patient_info_table(conn):
    """Create the gold.patient_info table with medico and unidade information"""
    print("Creating gold.patient_info table...")
    
    # Attach clinisys_all database
    import os
    repo_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    clinisys_db_path = os.path.join(repo_root, 'database', 'clinisys_all.duckdb')
    conn.execute(f"ATTACH '{clinisys_db_path}' AS clinisys_all")
    print(f"Attached clinisys_all database: {clinisys_db_path}")
    
    # Drop table if it exists
    conn.execute("DROP TABLE IF EXISTS gold.patient_info")
    
    # Create the patient info table following 03_01 logic
    create_table_query = """
    CREATE TABLE gold.patient_info AS
    WITH
    -- Get all unique patients from the comprehensive timeline (same as 03_01)
    all_patients AS (
        SELECT DISTINCT prontuario FROM gold.all_patients_timeline
    ),
    
    -- Get medico names by joining responsavel_informacoes with view_medicos
    -- Use the most recent doctor for each patient to avoid duplicates (same logic as 03_01)
    medico_mapping AS (
        SELECT 
            prontuario,
            COALESCE(m.nome, 'Não informado') as medico_nome
        FROM (
            SELECT 
                prontuario,
                responsavel_informacoes,
                ROW_NUMBER() OVER (PARTITION BY prontuario ORDER BY id DESC) as rn
            FROM clinisys_all.silver.view_tratamentos
            WHERE responsavel_informacoes IS NOT NULL
        ) latest_treatment
        LEFT JOIN clinisys_all.silver.view_medicos m ON latest_treatment.responsavel_informacoes = m.id
        WHERE latest_treatment.rn = 1
    ),
    
    -- Get unidade information from view_pacientes and view_unidades
    unidade_mapping AS (
        SELECT 
            p.codigo as prontuario,
            COALESCE(u.nome, 'Não informado') as unidade_nome
        FROM clinisys_all.silver.view_pacientes p
        LEFT JOIN clinisys_all.silver.view_unidades u ON p.unidade_origem = u.id
    )
    
    SELECT 
        ap.prontuario,
        COALESCE(med.medico_nome, 'Não informado') as medico_nome,
        COALESCE(uni.unidade_nome, 'Não informado') as unidade_nome
    FROM all_patients ap
    LEFT JOIN medico_mapping med ON ap.prontuario = med.prontuario
    LEFT JOIN unidade_mapping uni ON ap.prontuario = uni.prontuario
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
    
    return table_stats

def analyze_patient_info_data(conn):
    """Analyze the data in the patient_info table"""
    print("\nAnalyzing patient_info data...")
    
    # Sample data
    sample_data = conn.execute("""
        SELECT * FROM gold.patient_info 
        ORDER BY prontuario 
        LIMIT 15
    """).fetchdf()
    
    print("\nSample Data (First 15 patients):")
    print(sample_data.to_string(index=False))
    
    # Medico distribution
    medico_dist = conn.execute("""
        SELECT 
            medico_nome,
            COUNT(*) as patient_count
        FROM gold.patient_info
        GROUP BY medico_nome
        ORDER BY patient_count DESC
        LIMIT 10
    """).fetchdf()
    
    print(f"\nTop 10 Medicos by Patient Count:")
    print(medico_dist.to_string(index=False))
    
    # Unidade distribution
    unidade_dist = conn.execute("""
        SELECT 
            unidade_nome,
            COUNT(*) as patient_count
        FROM gold.patient_info
        GROUP BY unidade_nome
        ORDER BY patient_count DESC
        LIMIT 10
    """).fetchdf()
    
    print(f"\nTop 10 Unidades by Patient Count:")
    print(unidade_dist.to_string(index=False))
    
    # Medico-Unidade combinations
    medico_unidade_dist = conn.execute("""
        SELECT 
            medico_nome,
            unidade_nome,
            COUNT(*) as patient_count
        FROM gold.patient_info
        GROUP BY medico_nome, unidade_nome
        ORDER BY patient_count DESC
        LIMIT 15
    """).fetchdf()
    
    print(f"\nTop 15 Medico-Unidade Combinations:")
    print(medico_unidade_dist.to_string(index=False))

def main():
    """Main function to create patient info table"""
    print("=" * 80)
    print("CREATING PATIENT INFO TABLE")
    print("=" * 80)
    print(f"Timestamp: {datetime.now()}")
    print()
    
    try:
        # Connect to database
        print("Connecting to database...")
        conn = get_database_connection()
        
        # Create patient info table
        print("Creating patient info table...")
        patient_info_stats = create_patient_info_table(conn)
        
        # Analyze the data
        analyze_patient_info_data(conn)
        
        print(f"\nSuccessfully created gold.patient_info table")
        print(f"Table contains {patient_info_stats['unique_patients'].iloc[0]:,} unique patients")
        print(f"Total records: {patient_info_stats['total_records'].iloc[0]:,}")
        print(f"Unique medicos: {patient_info_stats['unique_medicos'].iloc[0]:,}")
        print(f"Unique unidades: {patient_info_stats['unique_unidades'].iloc[0]:,}")
        
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
