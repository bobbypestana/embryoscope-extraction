import duckdb

conn = duckdb.connect('../../database/huntington_data_lake.duckdb')

print("=" * 70)
print("EMBRYO COUNTS BY CATEGORY")
print("Source: gold.data_ploidia (extraction script source)")
print("=" * 70)

# Check data_ploidia totals
dp_total_rows = conn.execute('SELECT COUNT(*) FROM gold.data_ploidia').fetchone()[0]
dp_unique_embryos = conn.execute('SELECT COUNT(DISTINCT "Slide ID") FROM gold.data_ploidia WHERE "Slide ID" IS NOT NULL').fetchone()[0]

print(f"\nTable stats:")
print(f"  Total rows: {dp_total_rows:,}")
print(f"  Unique Slide IDs (embryos): {dp_unique_embryos:,}")
print()

# Query from data_ploidia (the actual source)
total = dp_unique_embryos

with_biopsy = conn.execute("""
    SELECT COUNT(DISTINCT "Slide ID") 
    FROM gold.data_ploidia
    WHERE "Slide ID" IS NOT NULL 
    AND ("Embryo Description" IS NOT NULL 
         OR "Embryo Description Clinisys" IS NOT NULL 
         OR "Embryo Description Clinisys Detalhes" IS NOT NULL)
""").fetchone()[0]

without_biopsy_with_outcome = conn.execute("""
    SELECT COUNT(DISTINCT "Slide ID") 
    FROM gold.data_ploidia
    WHERE "Slide ID" IS NOT NULL 
    AND ("Embryo Description" IS NULL 
         AND "Embryo Description Clinisys" IS NULL 
         AND "Embryo Description Clinisys Detalhes" IS NULL)
    AND (outcome_type IS NOT NULL 
         OR merged_numero_de_nascidos IS NOT NULL 
         OR fet_gravidez_clinica IS NOT NULL 
         OR fet_tipo_resultado IS NOT NULL 
         OR trat1_resultado_tratamento IS NOT NULL 
         OR trat2_resultado_tratamento IS NOT NULL)
""").fetchone()[0]

embryo_desc = conn.execute("""
    SELECT COUNT(DISTINCT "Slide ID") 
    FROM gold.data_ploidia
    WHERE "Slide ID" IS NOT NULL 
    AND "Embryo Description" IS NOT NULL
""").fetchone()[0]

clinisys_desc = conn.execute("""
    SELECT COUNT(DISTINCT "Slide ID") 
    FROM gold.data_ploidia
    WHERE "Slide ID" IS NOT NULL 
    AND "Embryo Description Clinisys" IS NOT NULL
""").fetchone()[0]

clinisys_detalhes = conn.execute("""
    SELECT COUNT(DISTINCT "Slide ID") 
    FROM gold.data_ploidia
    WHERE "Slide ID" IS NOT NULL 
    AND "Embryo Description Clinisys Detalhes" IS NOT NULL
""").fetchone()[0]

other = total - with_biopsy - without_biopsy_with_outcome

print(f"\nTOTAL UNIQUE EMBRYOS: {total:,}")
print()
print(f"WITH_BIOPSY: {with_biopsy:,} embryos ({with_biopsy/total*100:.1f}%)")
print(f"  Breakdown by source (may overlap):")
print(f"    - Embryo Description:                  {embryo_desc:,}")
print(f"    - Embryo Description Clinisys:         {clinisys_desc:,}")
print(f"    - Embryo Description Clinisys Detalhes: {clinisys_detalhes:,}")
print()
print(f"WITHOUT_BIOPSY (with outcome): {without_biopsy_with_outcome:,} embryos ({without_biopsy_with_outcome/total*100:.1f}%)")
print(f"  (No biopsy data but has outcome information)")
print()
print(f"OTHER (no biopsy, no outcome): {other:,} embryos ({other/total*100:.1f}%)")
print(f"  (Neither biopsy nor outcome data)")
print("=" * 70)

conn.close()

