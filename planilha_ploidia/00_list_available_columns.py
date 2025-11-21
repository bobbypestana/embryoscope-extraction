#!/usr/bin/env python3
"""
Helper script to list available columns in gold.planilha_embryoscope_combined
This helps verify the column mapping configuration.
"""

import duckdb as db
import os
import sys

def get_database_connection(read_only=True):
    """Create and return a connection to the huntington_data_lake database"""
    repo_root = os.path.dirname(os.path.dirname(__file__))
    path_to_db = os.path.join(repo_root, 'database', 'huntington_data_lake.duckdb')
    conn = db.connect(path_to_db, read_only=read_only)
    return conn

def list_columns():
    """List all columns grouped by prefix"""
    conn = get_database_connection(read_only=True)
    
    try:
        # Get all column names
        col_info = conn.execute("DESCRIBE gold.planilha_embryoscope_combined").df()
        
        print("=" * 80)
        print("ALL COLUMNS IN gold.planilha_embryoscope_combined")
        print("=" * 80)
        print(f"\nTotal columns: {len(col_info)}\n")
        
        # Group by prefix
        embryoscope_cols = []
        planilha_cols = []
        other_cols = []
        
        for _, row in col_info.iterrows():
            col_name = row['column_name']
            if col_name.startswith('embryoscope_'):
                embryoscope_cols.append(col_name)
            elif col_name.startswith('planilha_'):
                planilha_cols.append(col_name)
            else:
                other_cols.append(col_name)
        
        print(f"Embryoscope columns ({len(embryoscope_cols)}):")
        print("-" * 80)
        for col in sorted(embryoscope_cols):
            print(f"  {col}")
        
        print(f"\n\nPlanilha columns ({len(planilha_cols)}):")
        print("-" * 80)
        for col in sorted(planilha_cols):
            print(f"  {col}")
        
        if other_cols:
            print(f"\n\nOther columns ({len(other_cols)}):")
            print("-" * 80)
            for col in sorted(other_cols):
                print(f"  {col}")
        
        # Search for columns matching target keywords
        print("\n\n" + "=" * 80)
        print("SEARCHING FOR COLUMNS MATCHING TARGET KEYWORDS")
        print("=" * 80)
        
        target_keywords = [
            'unidade', 'unit', 'video', 'age', 'bmi', 'birth', 'year',
            'diagnosis', 'comment', 'patient', 'pin', 'prontuario',
            'previus', 'et', 'oocyte', 'source', 'aspirated', 'slide',
            'well', 'embryo', 'id', 'description',
            't2', 't3', 't4', 't5', 't8', 'tb', 'teb', 'thb', 'tm',
            'tpna', 'tpnf', 'tsb', 'tsc',
            'frag', 'fragmentation', 'icm', 'mn', 'nuclei', 'pn',
            'pulsing', 'reexpansion', 're-exp', 'te'
        ]
        
        print("\nColumns matching target keywords:")
        print("-" * 80)
        
        all_cols = embryoscope_cols + planilha_cols + other_cols
        for keyword in target_keywords:
            matches = [col for col in all_cols if keyword.lower() in col.lower()]
            if matches:
                print(f"\n  '{keyword}':")
                for match in matches:
                    print(f"    - {match}")
        
    finally:
        conn.close()

if __name__ == "__main__":
    list_columns()


