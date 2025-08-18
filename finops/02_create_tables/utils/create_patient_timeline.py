#!/usr/bin/env python3
"""
Create Patient Timeline
Creates a unified timeline for a patient by combining data from multiple tables.
"""

import pandas as pd
import duckdb as db
import os
from datetime import datetime
from typing import Dict, List, Tuple, Optional

def get_database_connection():
    """Create and return a connection to the clinisys_all database"""
    # Resolve DB path relative to repository root
    repo_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
    path_to_db = os.path.join(repo_root, 'database', 'clinisys_all.duckdb')
    conn = db.connect(path_to_db, read_only=True)
    
    print(f"Connected to database: {path_to_db}")
    print(f"Database file exists: {os.path.exists(path_to_db)}")
    
    return conn

def extract_timeline_data_for_patient(conn, prontuario: int) -> Dict[str, pd.DataFrame]:
    """
    Extract timeline data for a specific patient from all relevant tables
    """
    
    print(f"\n{'='*80}")
    print(f"EXTRACTING TIMELINE DATA FOR PRONTUARIO: {prontuario}")
    print(f"{'='*80}")
    
    timeline_data = {}
    
    # 1. view_tratamentos: id, data_procedimento, tentativa
    print(f"\n1. EXTRACTING view_tratamentos")
    try:
        tratamentos_df = conn.execute(f"""
            SELECT id, prontuario, data_procedimento, tentativa, 
                   tipo_procedimento, usuario_responsavel, unidade, resultado_tratamento,
                   data_inicio_inducao, dia_transferencia
            FROM silver.view_tratamentos 
            WHERE prontuario = {prontuario}
            ORDER BY data_procedimento
        """).fetchdf()
        
        print(f"Found {len(tratamentos_df)} treatment records")
        if len(tratamentos_df) > 0:
            print("Sample data:")
            print(tratamentos_df.head())
        
        timeline_data['tratamentos'] = tratamentos_df
        
    except Exception as e:
        print(f"Error extracting tratamentos: {str(e)}")
        timeline_data['tratamentos'] = pd.DataFrame()
    
    # 2. view_extrato_atendimentos_central: agendamento_id, data, agenda, agenda_nome
    print(f"\n2. EXTRACTING view_extrato_atendimentos_central")
    try:
        extrato_df = conn.execute(f"""
            SELECT agendamento_id, prontuario, data, agenda, agenda_nome,
                   inicio, medico, centro_custos, chegou, confirmado, procedimento_nome
            FROM silver.view_extrato_atendimentos_central 
            WHERE prontuario = {prontuario}
            ORDER BY data, inicio
        """).fetchdf()
        
        print(f"Found {len(extrato_df)} appointment records")
        if len(extrato_df) > 0:
            print("Sample data:")
            print(extrato_df.head())
        
        # Remove duplicates based on relevant columns (excluding agendamento_id)
        extrato_df = extrato_df.drop_duplicates(
            subset=['prontuario', 'data', 'procedimento_nome', 'confirmado'], 
            keep='first'
        )
        
        print(f"After removing duplicates: {len(extrato_df)} appointment records")
        timeline_data['extrato_atendimentos'] = extrato_df
        
    except Exception as e:
        print(f"Error extracting extrato_atendimentos: {str(e)}")
        timeline_data['extrato_atendimentos'] = pd.DataFrame()
    
    # 3. view_congelamentos_embrioes: CodCongelamento, data, ciclo, NEmbrioes
    print(f"\n3. EXTRACTING view_congelamentos_embrioes")
    try:
        cong_emb_df = conn.execute(f"""
            SELECT id, CodCongelamento, prontuario, Data, Ciclo, NEmbrioes,
                   Unidade, responsavel_recebimento, responsavel_armazenamento
            FROM silver.view_congelamentos_embrioes 
            WHERE prontuario = {prontuario}
            ORDER BY Data
        """).fetchdf()
        
        print(f"Found {len(cong_emb_df)} embryo freezing records")
        if len(cong_emb_df) > 0:
            print("Sample data:")
            print(cong_emb_df.head())
        
        timeline_data['congelamentos_embrioes'] = cong_emb_df
        
    except Exception as e:
        print(f"Error extracting congelamentos_embrioes: {str(e)}")
        timeline_data['congelamentos_embrioes'] = pd.DataFrame()
    
    # 4. view_congelamentos_ovulos: CodCongelamento, data, ciclo, NOvulos
    print(f"\n4. EXTRACTING view_congelamentos_ovulos")
    try:
        cong_ov_df = conn.execute(f"""
            SELECT id, CodCongelamento, prontuario, Data, Ciclo, NOvulos,
                   Unidade, responsavel_recebimento, responsavel_armazenamento
            FROM silver.view_congelamentos_ovulos 
            WHERE prontuario = {prontuario}
            ORDER BY Data
        """).fetchdf()
        
        print(f"Found {len(cong_ov_df)} oocyte freezing records")
        if len(cong_ov_df) > 0:
            print("Sample data:")
            print(cong_ov_df.head())
        
        timeline_data['congelamentos_ovulos'] = cong_ov_df
        
    except Exception as e:
        print(f"Error extracting congelamentos_ovulos: {str(e)}")
        timeline_data['congelamentos_ovulos'] = pd.DataFrame()
    
    # 5. view_descongelamentos_embrioes: CodDescongelamento, DataDescongelamento, ciclo
    print(f"\n5. EXTRACTING view_descongelamentos_embrioes")
    try:
        descong_emb_df = conn.execute(f"""
            SELECT id, CodDescongelamento, prontuario, DataDescongelamento, Ciclo,
                   Unidade, DataCongelamento, Transferencia, DataTransferencia
            FROM silver.view_descongelamentos_embrioes 
            WHERE prontuario = {prontuario}
            ORDER BY DataDescongelamento
        """).fetchdf()
        
        print(f"Found {len(descong_emb_df)} embryo thawing records")
        if len(descong_emb_df) > 0:
            print("Sample data:")
            print(descong_emb_df.head())
        
        timeline_data['descongelamentos_embrioes'] = descong_emb_df
        
    except Exception as e:
        print(f"Error extracting descongelamentos_embrioes: {str(e)}")
        timeline_data['descongelamentos_embrioes'] = pd.DataFrame()
    
    # 6. view_descongelamentos_ovulos: CodDescongelamento, DataDescongelamento, ciclo
    print(f"\n6. EXTRACTING view_descongelamentos_ovulos")
    try:
        descong_ov_df = conn.execute(f"""
            SELECT id, CodDescongelamento, prontuario, DataDescongelamento, Ciclo,
                   Unidade, DataCongelamento
            FROM silver.view_descongelamentos_ovulos 
            WHERE prontuario = {prontuario}
            ORDER BY DataDescongelamento
        """).fetchdf()
        
        print(f"Found {len(descong_ov_df)} oocyte thawing records")
        if len(descong_ov_df) > 0:
            print("Sample data:")
            print(descong_ov_df.head())
        
        timeline_data['descongelamentos_ovulos'] = descong_ov_df
        
    except Exception as e:
        print(f"Error extracting descongelamentos_ovulos: {str(e)}")
        timeline_data['descongelamentos_ovulos'] = pd.DataFrame()
    
    return timeline_data

def create_unified_timeline(timeline_data: Dict[str, pd.DataFrame], prontuario: int) -> pd.DataFrame:
    """
    Create a unified timeline from all extracted data
    """
    
    print(f"\n{'='*80}")
    print(f"CREATING UNIFIED TIMELINE FOR PRONTUARIO: {prontuario}")
    print(f"{'='*80}")
    
    timeline_events = []
    
    # Process each table and create timeline events
    for table_name, df in timeline_data.items():
        if df.empty:
            continue
            
        print(f"\nProcessing {table_name}: {len(df)} records")
        
        # Remove 'view_' prefix for display
        display_name = table_name.replace('view_', '')
        
        for _, row in df.iterrows():
            event = {
                'prontuario': prontuario,
                'table_name': display_name,
                'event_id': row.get('id', row.get('agendamento_id', row.get('CodCongelamento', row.get('CodDescongelamento', 'N/A')))),
                'event_date': None,
                'reference_column': None,
                'reference_value': None,
                'flag_date_estimated': False,  # Default to False for all events
                'additional_info': {}
            }
            
            # Extract date and reference information based on table
            if table_name == 'tratamentos':
                 # Handle ALL treatments - use data_procedimento if available, otherwise use data_inicio_inducao + 14 days
                 if pd.notna(row.get('data_procedimento')):
                     # Use original data_procedimento
                     event['event_date'] = row.get('data_procedimento')
                     event['flag_date_estimated'] = False
                 elif pd.notna(row.get('data_inicio_inducao')):
                     # Use data_inicio_inducao + 14 days as fallback
                     try:
                         # Convert data_inicio_inducao from string to date if needed
                         if isinstance(row.get('data_inicio_inducao'), str):
                             # Handle different date formats
                             for fmt in ['%d/%m/%Y', '%Y-%m-%d', '%d-%m-%Y', '%Y/%m/%d']:
                                 try:
                                     inducao_date = pd.to_datetime(row.get('data_inicio_inducao'), format=fmt)
                                     break
                                 except:
                                     continue
                             else:
                                 # If no specific format works, try pandas automatic parsing
                                 inducao_date = pd.to_datetime(row.get('data_inicio_inducao'))
                         else:
                             inducao_date = row.get('data_inicio_inducao')
                         
                         # Add 14 days to get estimated procedure date
                         estimated_date = inducao_date + pd.Timedelta(days=14)
                         event['event_date'] = estimated_date
                         event['flag_date_estimated'] = True
                         print(f"Using estimated date for tentativa {row.get('tentativa')}: {inducao_date} + 14 days = {estimated_date}")
                     except Exception as e:
                         print(f"Error estimating date for tentativa {row.get('tentativa')}: {e}")
                         continue  # Skip this record if we can't estimate the date
                 else:
                     # No date available - skip this treatment
                     print(f"Skipping treatment tentativa {row.get('tentativa')} - no date available")
                     continue
                 
                 event['reference_column'] = 'tipo_procedimento'
                 event['reference_value'] = f"{row.get('tipo_procedimento')} | {row.get('tentativa')}"
                 event['additional_info'] = {
                     'unidade': row.get('unidade'),
                     'resultado_tratamento': row.get('resultado_tratamento'),
                     'dia_transferencia': row.get('dia_transferencia')
                 }
                
            elif table_name == 'extrato_atendimentos':
                # Skip entries where procedimento_nome is null or confirmado is not 1
                if pd.isna(row.get('procedimento_nome')) or row.get('procedimento_nome') is None:
                    continue
                if row.get('confirmado') != 1:
                    continue
                    
                event['event_date'] = row.get('data')
                event['reference_column'] = 'procedimento_nome'
                event['reference_value'] = row.get('procedimento_nome')
                event['additional_info'] = {}
                
            elif table_name == 'congelamentos_embrioes':
                event['event_date'] = row.get('Data')
                event['reference_column'] = 'ciclo'
                event['reference_value'] = row.get('Ciclo')
                event['additional_info'] = {
                    'NEmbrioes': row.get('NEmbrioes'),
                    'Unidade': row.get('Unidade'),
                    'responsavel_recebimento': row.get('responsavel_recebimento'),
                    'responsavel_armazenamento': row.get('responsavel_armazenamento')
                }
                
            elif table_name == 'congelamentos_ovulos':
                event['event_date'] = row.get('Data')
                event['reference_column'] = 'ciclo'
                event['reference_value'] = row.get('Ciclo')
                event['additional_info'] = {
                    'NOvulos': row.get('NOvulos'),
                    'Unidade': row.get('Unidade'),
                    'responsavel_recebimento': row.get('responsavel_recebimento'),
                    'responsavel_armazenamento': row.get('responsavel_armazenamento')
                }
                
            elif table_name == 'descongelamentos_embrioes':
                event['event_date'] = row.get('DataDescongelamento')
                event['reference_column'] = 'ciclo'
                event['reference_value'] = row.get('Ciclo')
                event['additional_info'] = {
                    'CodDescongelamento': row.get('CodDescongelamento'),
                    'Unidade': row.get('Unidade'),
                    'DataCongelamento': row.get('DataCongelamento'),
                    'Transferencia': row.get('Transferencia'),
                    'DataTransferencia': row.get('DataTransferencia')
                }
                
            elif table_name == 'descongelamentos_ovulos':
                event['event_date'] = row.get('DataDescongelamento')
                event['reference_column'] = 'ciclo'
                event['reference_value'] = row.get('Ciclo')
                event['additional_info'] = {
                    'CodDescongelamento': row.get('CodDescongelamento'),
                    'Unidade': row.get('Unidade'),
                    'DataCongelamento': row.get('DataCongelamento')
                }
            
            # Only add events with valid dates
            if pd.notna(event['event_date']) and event['event_date'] is not None:
                timeline_events.append(event)
    
    # Create DataFrame and sort by date, table hierarchy, and event_id
    if timeline_events:
        timeline_df = pd.DataFrame(timeline_events)
        
        # Define table hierarchy for sorting
        table_hierarchy = {
            'extrato_atendimentos': 1,
            'congelamentos_ovulos': 2,
            'descongelamentos_ovulos': 3,
            'congelamentos_embrioes': 4,
            'descongelamentos_embrioes': 5,
            'tratamentos': 6,
        }
        
        # Add table_order column for sorting
        timeline_df['table_order'] = timeline_df['table_name'].map(table_hierarchy)
        
        # Sort by date DESC, table hierarchy DESC, event_id DESC
        timeline_df = timeline_df.sort_values(
            ['event_date', 'table_order', 'event_id'], 
            ascending=[False, False, False]
        )
        
        # Remove the temporary table_order column
        timeline_df = timeline_df.drop('table_order', axis=1)
        
        print(f"\nCreated timeline with {len(timeline_df)} events")
        print(f"Date range: {timeline_df['event_date'].min()} to {timeline_df['event_date'].max()}")
        
        return timeline_df
    else:
        print("\nNo timeline events found")
        return pd.DataFrame()

# REMOVED: Complex insert_missing_treatments function
# Now all treatments are handled in create_unified_timeline function

def display_timeline(timeline_df: pd.DataFrame, prontuario: int):
    """Display the timeline in a readable format"""
    
    if timeline_df.empty:
        print(f"\nNo timeline data available for prontuario {prontuario}")
        return
    
    print(f"\n{'='*100}")
    print(f"PATIENT TIMELINE - PRONTUARIO: {prontuario}")
    print(f"{'='*100}")
    
    for _, event in timeline_df.iterrows():
        date_str = event['event_date'].strftime('%Y-%m-%d') if pd.notna(event['event_date']) else 'Unknown'
        
        # Show estimated flag if applicable
        estimated_flag = " (ESTIMATED)" if event.get('flag_date_estimated', False) else ""
        print(f"\n📅 {date_str}{estimated_flag} | {event['table_name'].upper()}")
        print(f"   ID: {event['event_id']}")
        
        if pd.notna(event['reference_column']) and pd.notna(event['reference_value']):
            print(f"   {event['reference_column'].title()}: {event['reference_value']}")
        
        # Display additional info
        for key, value in event['additional_info'].items():
            if pd.notna(value) and value is not None:
                print(f"   {key}: {value}")
    
    print(f"\n{'='*100}")
    print(f"Total events: {len(timeline_df)}")
    print(f"Tables represented: {timeline_df['table_name'].unique()}")

def save_timeline_to_database(timeline_df: pd.DataFrame, prontuario: int):
    """Save timeline data to huntington_data_lake database in gold schema"""
    
    if timeline_df.empty:
        print(f"\nNo timeline data to save for prontuario {prontuario}")
        return
    
    print(f"\n{'='*80}")
    print(f"SAVING TIMELINE TO DATABASE FOR PRONTUARIO: {prontuario}")
    print(f"{'='*80}")
    
    try:
        # Connect to huntington_data_lake database
        # Resolve DB path relative to repository root
        repo_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
        path_to_db = os.path.join(repo_root, 'database', 'huntington_data_lake.duckdb')
        conn = db.connect(path_to_db)
        
        print(f"Connected to database: {path_to_db}")
        
        # Create gold schema if it doesn't exist
        conn.execute("CREATE SCHEMA IF NOT EXISTS gold")
        
        # Convert additional_info dict to JSON string for storage
        timeline_df_copy = timeline_df.copy()
        
        # Handle additional_info conversion more robustly
        def safe_convert_additional_info(info_dict):
            if not info_dict:
                return "{}"
            try:
                # Convert any problematic values to strings
                cleaned_dict = {}
                for key, value in info_dict.items():
                    if pd.isna(value):
                        cleaned_dict[key] = None
                    else:
                        cleaned_dict[key] = str(value)
                return str(cleaned_dict)
            except Exception:
                return "{}"
        
        timeline_df_copy['additional_info'] = timeline_df_copy['additional_info'].apply(safe_convert_additional_info)
        
        # Always drop and recreate the table to ensure correct schema
        conn.execute("DROP TABLE IF EXISTS gold.patients_timeline")
        print("Dropped existing table (if any)")
        
        # Create new table with current data
        conn.execute("""
            CREATE TABLE gold.patients_timeline AS 
            SELECT * FROM timeline_df_copy
        """)
        print(f"Created new table with data for patient {prontuario}")
        
        # Get row count
        count_result = conn.execute("SELECT COUNT(*) as row_count FROM gold.patients_timeline").fetchdf()
        row_count = count_result['row_count'].iloc[0]
        
        print(f"Successfully saved {len(timeline_df)} timeline events to gold.patients_timeline")
        print(f"Total events in table: {row_count}")
        
        # Show sample of saved data
        sample_data = conn.execute("SELECT * FROM gold.patients_timeline LIMIT 5").fetchdf()
        print(f"\nSample of saved data:")
        print(sample_data.to_string(index=False))
        
        conn.close()
        
    except Exception as e:
        print(f"Error saving timeline to database: {str(e)}")
        if 'conn' in locals():
            conn.close()

def main(test_prontuario):
    """Main function to create patient timeline"""
    
    # Test with a single patient first
    # test_prontuario = 175583
    
    print(f"Creating timeline for patient: {test_prontuario}")
    
    # Get database connection
    conn = get_database_connection()
    
    try:
        # Extract timeline data
        timeline_data = extract_timeline_data_for_patient(conn, test_prontuario)
        
        # Create unified timeline (now handles all treatments including those with estimated dates)
        timeline_df = create_unified_timeline(timeline_data, test_prontuario)
        
        # Display timeline
        display_timeline(timeline_df, test_prontuario)
        
        # Save timeline to database
        save_timeline_to_database(timeline_df, test_prontuario)
        
        return timeline_df
        
    finally:
        conn.close()

if __name__ == "__main__":
    # Example usage - create timeline for a single patient
    patient_id = 220783  # Change this to test different patients
    timeline_df = main(patient_id)