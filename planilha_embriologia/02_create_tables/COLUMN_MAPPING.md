# Column Mapping for planilha_embryoscope_combined Export

This document maps the target column names (from your picture) to the actual database column names.

## Target Columns → Database Column Mapping

Based on the structure of `planilha_embryoscope_combined`, columns are prefixed with:
- `embryoscope_` for columns from embryoscope_clinisys_combined
- `planilha_` for columns from planilha_embriologia

Column names in the database have spaces and special characters replaced with underscores.

### Mapping Dictionary

```python
COLUMN_MAPPING = {
    # Target Column Name → Source Database Column Name
    
    # From Planilha (likely)
    "Unidade": "planilha_Unidade",  # or similar - need to verify exact name
    "Age": "planilha_Age",  # or "planilha_IDADE" - need to verify
    "BMI": "planilha_BMI",  # need to verify
    "Birth Year": "planilha_Birth_Year",  # or "planilha_ANO_NASCIMENTO" - need to verify
    "Diagnosis": "planilha_Diagnosis",  # or "planilha_DIAGNOSTICO" - need to verify
    "Patient Comments": "planilha_Patient_Comments",  # need to verify
    "Patient ID": "planilha_PIN",  # or "embryoscope_micro_prontuario" - PIN is the patient ID
    "Previus ET": "planilha_Previus_ET",  # need to verify exact name
    "Previus OD ET": "planilha_Previus_OD_ET",  # need to verify
    "Oocyte History": "planilha_Oocyte_History",  # need to verify
    "Oocyte Source": "planilha_Oocyte_Source",  # need to verify
    "Oocytes Aspirated": "planilha_Oocytes_Aspirated",  # need to verify
    "Slide ID": "planilha_Slide_ID",  # need to verify
    
    # From Embryoscope (likely)
    "Video ID": "embryoscope_embryo_EmbryoID",  # Embryo ID from embryoscope
    "Well": "embryoscope_embryo_WellNumber",  # or "embryoscope_oocito_WellNumber"
    "Embryo ID": "embryoscope_embryo_EmbryoID",  # or "embryoscope_oocito_id"
    
    # Time annotations from Embryoscope
    "t2": "embryoscope_embryo_Value_t2",  # or "embryoscope_embryo_Time_t2"
    "t3": "embryoscope_embryo_Value_t3",
    "t4": "embryoscope_embryo_Value_t4",
    "t5": "embryoscope_embryo_Value_t5",
    "t8": "embryoscope_embryo_Value_t8",
    "tB": "embryoscope_embryo_Value_tB",
    "tEB": "embryoscope_embryo_Value_tEB",
    "tHB": "embryoscope_embryo_Value_tHB",
    "tM": "embryoscope_embryo_Value_tM",
    "tPNa": "embryoscope_embryo_Value_tPNa",
    "tPNf": "embryoscope_embryo_Value_tPNf",
    "tSB": "embryoscope_embryo_Value_tSB",
    "tSC": "embryoscope_embryo_Value_tSC",
    
    # Annotation values from Embryoscope
    "Frag-2 Cat. - Value": "embryoscope_embryo_Value_FRAG2CAT",
    "Fragmentation - Value": "embryoscope_embryo_Value_Fragmentation",
    "ICM - Value": "embryoscope_embryo_Value_ICM",
    "MN-2 Type - Value": "embryoscope_embryo_Value_MN2Type",
    "MN-2 Cells - Value": "embryoscope_embryo_Value_Nuclei2",  # Nuclei2 might be cells count
    "PN - Value": "embryoscope_embryo_Value_PN",
    "Pulsing - Value": "embryoscope_embryo_Value_Pulsing",
    "Re-exp Count - Value": "embryoscope_embryo_Value_ReexpansionCount",
    "TE - Value": "embryoscope_embryo_Value_TE",
    "Embryo Description": "embryoscope_embryo_Description",
}
```

## Notes

1. **Column Name Cleaning**: Database column names have spaces and special characters replaced with underscores. For example:
   - "DATA DA FET" → "planilha_DATA_DA_FET"
   - "Patient ID" → likely "planilha_PIN" or "embryoscope_micro_prontuario"

2. **Time Values**: The time annotations (t2, t3, etc.) might be:
   - `Value_t2` (the value/score)
   - `Time_t2` (the time in hours)
   - `Timestamp_t2` (the timestamp)
   - You'll need to decide which one you want

3. **Verification Needed**: To get the exact column names, you can:
   - Query the database: `DESCRIBE gold.planilha_embryoscope_combined`
   - Or run a script to list all columns

4. **Column Selection**: You'll need to specify which columns to include in the export.

## Next Steps

1. Verify the exact column names in the database
2. Update the mapping dictionary with correct source column names
3. Configure the export script to use this mapping


