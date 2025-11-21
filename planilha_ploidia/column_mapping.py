"""
Column mapping configuration for data_ploidia table.

This file defines:
1. The target column names in the exact order specified
2. The mapping from target columns to source columns in planilha_embryoscope_combined
"""

# Target columns in the exact order specified
TARGET_COLUMNS = [
    "Unidade",
    "Video ID",
    "Age",
    "BMI",
    "Birth Year",
    "Diagnosis",
    "Patient Comments",
    "Patient ID",
    "Previus ET",
    "Previus OD ET",
    "Oocyte History",
    "Oocyte Source",
    "Oocytes Aspirated",
    "Slide ID",
    "Well",
    "Embryo ID",
    "t2",
    "t3",
    "t4",
    "t5",
    "t8",
    "tB",
    "tEB",
    "tHB",
    "tM",
    "tPNa",
    "tPNf",
    "tSB",
    "tSC",
    "Frag-2 Cat. - Value",
    "Fragmentation - Value",
    "ICM - Value",
    "MN-2 Type - Value",
    "MN-2 Cells - Value",
    "PN - Value",
    "Pulsing - Value",
    "Re-exp Count - Value",
    "TE - Value",
    "Embryo Description",
]

# Mapping from target column name to source column name in planilha_embryoscope_combined
# Columns not in this mapping or set to None will be left as NULL
# Based on actual column names found in the database (verified)
# Ordered to match TARGET_COLUMNS list
COLUMN_MAPPING = {
    "Unidade": "patient_unit_huntington",
    "Video ID": None,
    "Age": None,  # Calculated dynamically as difference between embryo_FertilizationTime and patient_DateOfBirth
    "BMI": None,  
    "Birth Year": "patient_DateOfBirth",  # Will extract year in SQL
    "Diagnosis": None,  
    "Patient Comments": None,
    "Patient ID": "micro_prontuario",
    "Previus ET": None,  
    "Previus OD ET": None,  
    "Oocyte History": "oocito_OrigemOocito",  
    "Oocyte Source": None,
    "Oocytes Aspirated": "micro_oocitos",
    "Slide ID": "embryo_EmbryoID",  
    "Well": "embryo_WellNumber",
    "Embryo ID": "embryo_EmbryoDescriptionID",
    "t2": "embryo_Time_t2",
    "t3": "embryo_Time_t3",
    "t4": "embryo_Time_t4",
    "t5": "embryo_Time_t5",
    "t8": "embryo_Time_t8",
    "tB": "embryo_Time_tB",
    "tEB": "embryo_Time_tEB",
    "tHB": "embryo_Time_tHB",
    "tM": "embryo_Time_tM",
    "tPNa": "embryo_Time_tPNa",
    "tPNf": "embryo_Time_tPNf",
    "tSB": "embryo_Time_tSB",
    "tSC": "embryo_Time_tSC",
    "Frag-2 Cat. - Value": "embryo_Time_FRAG2CAT",
    "Fragmentation - Value": "embryo_Time_Fragmentation",
    "ICM - Value": "embryo_Value_ICM",
    "MN-2 Type - Value": "embryo_Value_MN2Type",
    "MN-2 Cells - Value": "embryo_Value_Nuclei2",
    "PN - Value": "embryo_Value_PN",
    "Pulsing - Value": "embryo_Value_Pulsing",
    "Re-exp Count - Value": "embryo_Value_ReexpansionCount",
    "TE - Value": "embryo_Value_TE",
    "Embryo Description": "embryo_Description",
}

# Filter configuration for table creation
# Set to None to include all rows, or specify filters
FILTER_PATIENT_ID = 823589  # Filter by Patient ID (micro_prontuario) - set to None to disable
FILTER_EMBRYO_IDS = None  # Filter by Embryo ID (embryo_EmbryoID)

