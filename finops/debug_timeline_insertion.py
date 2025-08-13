#!/usr/bin/env python3
"""
Debug script to see what tentativas are being inserted into the timeline for patient 220783
"""

import pandas as pd
import duckdb as db
from create_patient_timeline import (
    get_database_connection,
    extract_timeline_data_for_patient,
    create_unified_timeline,
    insert_missing_treatments
)

def debug_timeline_insertion(prontuario: int):
    """Debug the timeline insertion process"""
    print(f"\n{'='*80}")
    print(f"DEBUGGING TIMELINE INSERTION FOR PATIENT: {prontuario}")
    print(f"{'='*80}")
    
    conn = get_database_connection()
    
    try:
        # Extract timeline data
        timeline_data = extract_timeline_data_for_patient(conn, prontuario)
        
        # Create unified timeline
        timeline_df = create_unified_timeline(timeline_data, prontuario)
        
        # Get treatments without dates
        tratamentos_df = timeline_data.get('tratamentos', pd.DataFrame())
        no_date_treatments = tratamentos_df[
            pd.isna(tratamentos_df['data_procedimento']) | 
            (tratamentos_df['data_procedimento'] == None)
        ].copy()
        
        print(f"\nTreatments without dates that will be inserted:")
        for _, row in no_date_treatments.iterrows():
            print(f"  ID: {row['id']:5d} | Tentativa: {row['tentativa']:>2} | Type: {row['tipo_procedimento']}")
        
        # Insert missing treatments
        timeline_df_after_insertion = insert_missing_treatments(timeline_df, timeline_data, prontuario)
        
        # Find inserted treatments
        inserted_treatments = timeline_df_after_insertion[
            timeline_df_after_insertion['additional_info'].apply(
                lambda x: isinstance(x, dict) and x.get('inserted', False)
            )
        ]
        
        print(f"\nActually inserted treatments:")
        for _, row in inserted_treatments.iterrows():
            print(f"  ID: {row['event_id']:5d} | Tentativa: {row['reference_value']:>2} | Date: {row['event_date']}")
        
        # Show all tratamentos in final timeline
        all_tratamentos = timeline_df_after_insertion[
            timeline_df_after_insertion['table_name'] == 'tratamentos'
        ].sort_values('event_date', ascending=False)
        
        print(f"\nAll tratamentos in final timeline (sorted by date DESC):")
        for _, row in all_tratamentos.iterrows():
            inserted_flag = " [INSERTED]" if (isinstance(row['additional_info'], dict) and row['additional_info'].get('inserted', False)) else ""
            print(f"  Date: {row['event_date']} | Tentativa: {row['reference_value']:>2} | ID: {row['event_id']:5d}{inserted_flag}")
        
        return timeline_df_after_insertion
        
    finally:
        conn.close()

if __name__ == "__main__":
    debug_timeline_insertion(220783)
