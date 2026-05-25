"""
transform_ovodoacao.py

Reads the Ovodoacao Excel sheet and enriches it with prontuario + patient_name
by delegating all matching to the generic utility.
"""
import os
import sys
import logging
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BASE_DIR    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT_FILE  = os.path.join(BASE_DIR, 'planilha_ovodoacao', 'data_input', 'Planilha Nomes e PIN ASRM.xlsx')
OUTPUT_DIR  = os.path.join(BASE_DIR, 'planilha_ovodoacao', 'data_exports')
OUTPUT_FILE = os.path.join(OUTPUT_DIR, 'Planilha Nomes e PIN ASRM_processed.xlsx')

sys.path.insert(0, os.path.join(BASE_DIR, 'planilha_ovodoacao'))
from utils.patient_matching_generic import match_prontuario


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    df = pd.read_excel(INPUT_FILE, dtype=str)
    df = match_prontuario(df, pin_col='PIN', name_col='NOME DA RECEPTORA')
    df.to_excel(OUTPUT_FILE, index=False)

    print(f"Saved → {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
