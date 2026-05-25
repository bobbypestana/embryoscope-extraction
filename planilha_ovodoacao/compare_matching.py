import duckdb
import pandas as pd
import os
import sys

BASE_DIR = r'g:\My Drive\projetos_individuais\Huntington'
sys.path.insert(0, os.path.join(BASE_DIR, 'planilha_ovodoacao'))

from utils.patient_matching_generic import match_prontuario

def main():
    db_path = os.path.join(BASE_DIR, 'database', 'embryoscope_vila_mariana.db')
    
    print(f"Reading from {db_path}...")
    con = duckdb.connect(db_path)
    df_orig = con.execute('SELECT "PatientID", "FirstName", "LastName", prontuario as orig_prontuario FROM silver.patients').df()
    con.close()
    
    total = len(df_orig)
    orig_matched = (df_orig['orig_prontuario'] != -1).sum()
    print(f"Original matched counts: {orig_matched} / {total} ({(orig_matched/total)*100:.1f}%)")
    
    # We create a full name column mimicking the fallback in case it's needed
    # (Though in our util, if it's "FIRST MIDDLE", it extracts just "FIRST")
    
    print("\nRunning generic matching...")
    df_test = df_orig.copy()
    
    # Run generic matching
    df_result = match_prontuario(df_test, pin_col='PatientID', name_col='FirstName')
    
    generic_matched = (df_result['prontuario'] != -1).sum()
    print(f"\nGeneric matched counts: {generic_matched} / {total} ({(generic_matched/total)*100:.1f}%)")
    
    print("\nComparing...")
    df_compare = df_orig.merge(df_result[['PatientID', 'prontuario']], on='PatientID', how='inner')
    
    diff_mask = df_compare['orig_prontuario'] != df_compare['prontuario']
    df_diff = df_compare[diff_mask]
    
    print(f"Total differences: {len(df_diff)}")
    if len(df_diff) > 0:
        print("\nSamples of differences (Original vs Generic):")
        print(df_diff[['PatientID', 'FirstName', 'LastName', 'orig_prontuario', 'prontuario']].head(20))
    else:
        print("Perfect match! The generic utility reproduced the exact same results.")

if __name__ == "__main__":
    main()
