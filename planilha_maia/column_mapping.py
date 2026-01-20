
"""
Mapping configuration for creating the planilha_maia table.
Separated into 'straight' (rename/copy) and 'derived' (calculated) columns.

For derived columns, you can use either:
1. String expressions: "col1 - col2", "col1 + col2", "col1 * col2", "col1 / col2"
2. Python functions: def my_func(df): return df['col1'] + df['col2']
3. Lambda functions with column names (recommended for simple operations)
"""

import pandas as pd
import numpy as np

# ============================================================================
# DERIVED COLUMN HELPER FUNCTIONS
# ============================================================================

def subtract_cols(col1, col2):
    """Factory function to create a subtraction operation between two columns"""
    def _subtract(df):
        if col1 in df.columns and col2 in df.columns:
            return df[col1] - df[col2]
        return None
    return _subtract

def add_cols(col1, col2):
    """Factory function to create an addition operation between two columns"""
    def _add(df):
        if col1 in df.columns and col2 in df.columns:
            return df[col1] + df[col2]
        return None
    return _add

def multiply_cols(col1, col2):
    """Factory function to create a multiplication operation between two columns"""
    def _multiply(df):
        if col1 in df.columns and col2 in df.columns:
            return df[col1] * df[col2]
        return None
    return _multiply

def divide_cols(col1, col2):
    """Factory function to create a division operation between two columns"""
    def _divide(df):
        if col1 in df.columns and col2 in df.columns:
            return df[col1] / df[col2]
        return None
    return _divide

# Example of a more complex derived column function
def example_complex_calculation(df):
    """
    Example of a complex derived column with conditional logic
    You can add any Python logic here
    """
    result = pd.Series(index=df.index, dtype=float)
    
    # Example: Calculate something based on multiple conditions
    # mask = df['some_col'] > 10
    # result[mask] = df.loc[mask, 'col1'] * 2
    # result[~mask] = df.loc[~mask, 'col1'] + df.loc[~mask, 'col2']
    
    return result


# ============================================================================
# COLUMN MAPPING CONFIGURATION
# ============================================================================

COLUMN_MAPPING = {
    # Straight mappings: Target Column Name : Source Column Name
    "straight": {
        "Data coleta": "micro_Data_DL",
        "Data ET": 'descong_em_DataTransferencia',
        "PLANILHA": None,
        "patient ID": 'micro_prontuario',
        "ID": 'planilha_PIN',
        "video ID": "embryo_EmbryoID",
        "KID + (Fetal Heartbeat)": None,
        "Gestation": "trat_resultado_tratamento",
        "KID+": None,
        "KID-": None,
        "Morfologia Correta": None,
        "Cinética (MK)": None,
        "Pacientes (PAC)": None,
        "MP+MK": None, # derived
        "MP+PAC": None, # derived
        "MP+MK+PAC": None, # derived
        "Local": 'planilha_UNIDADE',
        "technique": 'oocito_InseminacaoOocito',
        "technique var.": None,
        "oocyte origin": "trat_origem_ovulo",
        "ploidy": "embryo_Description",
        "T2": "embryo_Time_t2",
        "T3": "embryo_Time_t3",
        "T4": "embryo_Time_t4",
        "T5": "embryo_Time_t5",
        "T6": "embryo_Time_t6",
        "T7": "embryo_Time_t7",
        "T8": "embryo_Time_t8",
        "TsC": "embryo_Time_tSC",
        "TM": "embryo_Time_tM",
        "Tsb": "embryo_Time_tSB",
        "Tb": "embryo_Time_tB",
        "TexpB": None,
        "Thb": "embryo_Value_tHB",
        "CC2": None,
        "CC3": None,
        "RelCC2": None,
        "t4-t3": None, # derived
        "t5-t2": None, # derived
        "Tsb-T8": None, # derived
        "tB-Tsb": None, # derived
        "Morfologia": None,
        "ICM": "embryo_Value_ICM",
        "Tropho": None,
        "selection": None,
        "Saco gestacional": None,
        "BCF": None,
        "Age": "trat_idade_esposa",
        "Score_Age": None, # derived
        "Body Mass Index": None, # derived
        "Score_BMI": None, # derived
        "ET próprias": None, # derived
        "ET donor": None, # derived
        "NUMBER ET": None, # derived
        "Score_PreviousAttempts": None, # derived
        "Retrieved Oocytes": "micro_oocitos", 
        "Score_Oocyte": None,
        "AMH": None,
        "Score_AMH": None,
        "FSH": None,
    },
    
    # Derived mappings are now calculated directly in 01_create_planilha_maia.py
    # This keeps the mapping file simple and the logic in one place
    "derived": {}
}
