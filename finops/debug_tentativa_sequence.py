#!/usr/bin/env python3
"""
Debug script to examine tentativa sequence for patient 220783
"""

import pandas as pd
import duckdb as db

def get_database_connection():
    """Create and return a connection to the clinisys_all database"""
    path_to_db = 'database/clinisys_all.duckdb'
    conn = db.connect(path_to_db, read_only=True)
    return conn

def examine_tentativa_sequence(prontuario: int):
    """Examine the tentativa sequence for a specific patient"""
    print(f"\n{'='*80}")
    print(f"EXAMINING TENTATIVA SEQUENCE FOR PATIENT: {prontuario}")
    print(f"{'='*80}")
    
    conn = get_database_connection()
    
    try:
        # Get all treatments for the patient
        tratamentos_df = conn.execute(f"""
            SELECT id, prontuario, data_procedimento, tentativa, 
                   tipo_procedimento, usuario_responsavel, unidade, resultado_tratamento
            FROM silver.view_tratamentos 
            WHERE prontuario = {prontuario}
            ORDER BY id
        """).fetchdf()
        
        print(f"\nTotal treatments found: {len(tratamentos_df)}")
        
        # Show all treatments with their tentativas
        print(f"\nAll treatments with tentativas:")
        for _, row in tratamentos_df.iterrows():
            tentativa_str = str(row['tentativa']) if pd.notna(row['tentativa']) else 'None'
            date_str = str(row['data_procedimento']) if pd.notna(row['data_procedimento']) else 'No Date'
            print(f"  ID: {row['id']:5d} | Tentativa: {tentativa_str:>4} | Date: {date_str} | Type: {row['tipo_procedimento']}")
        
        # Analyze tentativa distribution
        print(f"\nTentativa analysis:")
        
        # Get existing tentativas
        existing_tentativas = []
        for _, row in tratamentos_df.iterrows():
            if pd.notna(row['tentativa']) and row['tentativa'] is not None:
                existing_tentativas.append(int(row['tentativa']))
        
        existing_tentativas.sort()
        print(f"  Existing tentativas: {existing_tentativas}")
        
        # Find gaps
        if existing_tentativas:
            expected_range = list(range(1, max(existing_tentativas) + 1))
            gaps = [x for x in expected_range if x not in existing_tentativas]
            print(f"  Expected range: {expected_range}")
            print(f"  Gaps in sequence: {gaps}")
        
        # Count treatments without dates
        no_date_treatments = tratamentos_df[
            pd.isna(tratamentos_df['data_procedimento']) | 
            (tratamentos_df['data_procedimento'] == None)
        ]
        print(f"\nTreatments without dates: {len(no_date_treatments)}")
        
        if not no_date_treatments.empty:
            print(f"  These treatments have no dates:")
            for _, row in no_date_treatments.iterrows():
                tentativa_str = str(row['tentativa']) if pd.notna(row['tentativa']) else 'None'
                print(f"    ID: {row['id']:5d} | Tentativa: {tentativa_str:>4} | Type: {row['tipo_procedimento']}")
        
        # Count treatments without tentativas
        no_tentativa_treatments = tratamentos_df[
            pd.isna(tratamentos_df['tentativa']) | 
            (tratamentos_df['tentativa'] == None)
        ]
        print(f"\nTreatments without tentativas: {len(no_tentativa_treatments)}")
        
        if not no_tentativa_treatments.empty:
            print(f"  These treatments have no tentativas:")
            for _, row in no_tentativa_treatments.iterrows():
                date_str = str(row['data_procedimento']) if pd.notna(row['data_procedimento']) else 'No Date'
                print(f"    ID: {row['id']:5d} | Date: {date_str} | Type: {row['tipo_procedimento']}")
        
        # Check US cycle entries
        print(f"\nChecking US cycle entries for patient {prontuario}:")
        conn = get_database_connection()
        
        try:
            # Get all appointments for the patient
            appointments_df = conn.execute(f"""
                SELECT agendamento_id, prontuario, data, agenda, agenda_nome, 
                       centro_custos, chegou, confirmado, procedimento_nome
                FROM silver.view_extrato_atendimentos_central
                WHERE prontuario = {prontuario}
                AND confirmado = 1
                AND procedimento_nome IS NOT NULL
            """).df()
            
            # Get all US cycle entries
            us_cycle_patterns = [
                '1° US Ciclo',
                'US - Ciclo',
                '1º US de Ciclo',
                'US Ciclo',
                '1° US de Ciclo'
            ]
            
            all_us_cycle_entries = appointments_df[
                appointments_df['procedimento_nome'].str.contains('|'.join(us_cycle_patterns), case=False, na=False)
            ]
            
            # Filter to only first US cycle entries (1st, 1º, etc.)
            first_us_patterns = [
                '1° US Ciclo',
                '1º US de Ciclo', 
                '1° US de Ciclo',
                '1º US Ciclo'
            ]
            
            first_us_entries = all_us_cycle_entries[
                all_us_cycle_entries['procedimento_nome'].str.contains('|'.join(first_us_patterns), case=False, na=False)
            ]
            
            print(f"Total appointments: {len(appointments_df)}")
            print(f"All US cycle entries: {len(all_us_cycle_entries)}")
            print(f"First US cycle entries: {len(first_us_entries)}")
            
            if not first_us_entries.empty:
                print("First US cycle entries found:")
                for _, entry in first_us_entries.iterrows():
                    print(f"  {entry['data']}: {entry['procedimento_nome']}")
            
        except Exception as e:
            print(f"Error checking US cycle entries: {e}")
        finally:
            conn.close()
        
        return tratamentos_df
        
    finally:
        conn.close()

if __name__ == "__main__":
    examine_tentativa_sequence(825890)
