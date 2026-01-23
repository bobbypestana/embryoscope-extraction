"""
Column mapping configuration for data_ploidia table.

This file defines the target column names and their source mappings in order.
Using OrderedDict to maintain column order while providing the mapping.
"""

from collections import OrderedDict

# Column mapping: target column name -> source column name
# Order is preserved (Python 3.7+), columns will appear in this exact order in the output
# Set source to None for columns that need custom logic or should be NULL
COLUMN_MAPPING = OrderedDict([
    ("Unidade", "patient_unit_huntington"),
    ("Video ID", None),  # Calculated dynamically as prontuario || '_' || EmbryoID
    ("Age", "AgeAtFertilization"),  # Already calculated in gold layer
    ("BMI", "trat1_bmi"),  # Already calculated in gold layer
    ("Birth Year", "patient_YearOfBirth"),  # Already calculated in gold layer
    ("Diagnosis", "trat1_fator_infertilidade1"),
    ("Patient Comments", None),
    ("Patient ID", "micro_prontuario"),
    ("Previus ET", "trat1_previous_et"),  # Already in gold layer (cycle number)
    ("Previus OD ET", "trat1_previous_et_od"),  # Fixed: was trat1_previous_od_et
    ("Oocyte History", "oocito_OrigemOocito"),
    ("Oocyte Source", "trat1_origem_ovulo"),
    ("Oocytes Aspirated", "micro_oocitos"),
    ("Slide ID", "embryo_EmbryoID"),
    ("Well", "embryo_WellNumber"),
    ("Embryo ID", "embryo_EmbryoDescriptionID"),
    ("t2", "embryo_Time_t2"),
    ("t3", "embryo_Time_t3"),
    ("t4", "embryo_Time_t4"),
    ("t5", "embryo_Time_t5"),
    ("t8", "embryo_Time_t8"),
    ("tB", "embryo_Time_tB"),
    ("tEB", "embryo_Time_tEB"),
    ("tHB", "embryo_Time_tHB"),
    ("tM", "embryo_Time_tM"),
    ("tPNa", "embryo_Time_tPNa"),
    ("tPNf", "embryo_Time_tPNf"),
    ("tSB", "embryo_Time_tSB"),
    ("tSC", "embryo_Time_tSC"),
    ("Frag-2 Cat. - Value", "embryo_Value_FRAG2CAT"),
    ("Fragmentation - Value", "embryo_Time_Fragmentation"),
    ("ICM - Value", "embryo_Value_ICM"),
    ("MN-2 Type - Value", "embryo_Value_MN2Type"),
    ("MN-2 Cells - Value", "embryo_Value_Nuclei2"),
    ("PN - Value", "embryo_Value_PN"),
    ("Pulsing - Value", "embryo_Value_Pulsing"),
    ("Re-exp Count - Value", "embryo_Value_ReexpansionCount"),
    ("TE - Value", "embryo_Value_TE"),
    ("Embryo Description", "embryo_Description"),
])

# Derived: Target columns list (for backward compatibility)
TARGET_COLUMNS = list(COLUMN_MAPPING.keys())
