# Planilha Ploidia

This directory contains scripts to create the `gold.data_ploidia` table from `gold.planilha_embryoscope_combined`.

## Files

- `column_mapping.py` - Column mapping configuration defining target columns and their mappings to source columns
- `00_list_available_columns.py` - Helper script to list all available columns in the source table
- `01_create_data_ploidia_table.py` - Main script to create the `gold.data_ploidia` table

## Usage

### 1. List Available Columns (Optional)

To verify the column mapping, first list all available columns:

```bash
python planilha_ploidia/00_list_available_columns.py
```

This will show all columns in `gold.planilha_embryoscope_combined` grouped by prefix (`embryoscope_` vs `planilha_`).

### 2. Update Column Mapping (if needed)

Edit `column_mapping.py` to update the `COLUMN_MAPPING` dictionary with the correct source column names based on the output from step 1.

### 3. Create the Table

Run the main script to create the `gold.data_ploidia` table:

```bash
python planilha_ploidia/01_create_data_ploidia_table.py
```

This script will:
- Read the column mapping from `column_mapping.py`
- Create a SELECT query that maps source columns to target columns
- Leave NULL for unmapped columns
- Create the table `gold.data_ploidia` with columns in the exact order specified
- Log detailed information about mapped/unmapped columns

## Column Order

The target columns are defined in the exact order specified:

1. Unidade
2. Video ID
3. Age
4. BMI
5. Birth Year
6. Diagnosis
7. Patient Comments
8. Patient ID
9. Previus ET
10. Previus OD ET
11. Oocyte History
12. Oocyte Source
13. Oocytes Aspirated
14. Slide ID
15. Well
16. Embryo ID
17. t2
18. t3
19. t4
20. t5
21. t8
22. tB
23. tEB
24. tHB
25. tM
26. tPNa
27. tPNf
28. tSB
29. tSC
30. Frag-2 Cat. - Value
31. Fragmentation - Value
32. ICM - Value
33. MN-2 Type - Value
34. MN-2 Cells - Value
35. PN - Value
36. Pulsing - Value
37. Re-exp Count - Value
38. TE - Value
39. Embryo Description

## Notes

- Columns not in the `COLUMN_MAPPING` dictionary will be set to NULL
- If a source column doesn't exist in the source table, the target column will be set to NULL
- The script logs warnings for unmapped or missing columns
- Column names with special characters (spaces, hyphens) are properly quoted in SQL queries


