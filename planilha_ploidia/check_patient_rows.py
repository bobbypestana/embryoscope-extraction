#!/usr/bin/env python3
"""Quick script to check row counts for Patient ID 823589"""

import duckdb as db
import os

repo_root = os.path.dirname(os.path.dirname(__file__))
path_to_db = os.path.join(repo_root, 'database', 'huntington_data_lake.duckdb')
conn = db.connect(path_to_db, read_only=True)

# Check with embryoscope_micro_prontuario
result1 = conn.execute('SELECT COUNT(*) FROM gold.planilha_embryoscope_combined WHERE "embryoscope_micro_prontuario" = 823589').fetchone()[0]
print(f'Rows with embryoscope_micro_prontuario = 823589: {result1}')

# Check with planilha_PIN
result2 = conn.execute('SELECT COUNT(*) FROM gold.planilha_embryoscope_combined WHERE "planilha_PIN" = 823589').fetchone()[0]
print(f'Rows with planilha_PIN = 823589: {result2}')

# Show sample of distinct values for embryoscope_micro_prontuario when planilha_PIN = 823589
result3 = conn.execute('SELECT DISTINCT "embryoscope_micro_prontuario" FROM gold.planilha_embryoscope_combined WHERE "planilha_PIN" = 823589 LIMIT 10').fetchall()
print(f'\nDistinct embryoscope_micro_prontuario values when planilha_PIN = 823589:')
for row in result3:
    print(f'  {row[0]}')

# Show sample of distinct values for planilha_PIN when embryoscope_micro_prontuario = 823589
result4 = conn.execute('SELECT DISTINCT "planilha_PIN" FROM gold.planilha_embryoscope_combined WHERE "embryoscope_micro_prontuario" = 823589 LIMIT 10').fetchall()
print(f'\nDistinct planilha_PIN values when embryoscope_micro_prontuario = 823589:')
for row in result4:
    print(f'  {row[0]}')

# Show distinct Embryo IDs for this patient
result5 = conn.execute('SELECT "embryoscope_embryo_EmbryoID", COUNT(*) as cnt FROM gold.planilha_embryoscope_combined WHERE "embryoscope_micro_prontuario" = 823589 GROUP BY "embryoscope_embryo_EmbryoID" ORDER BY "embryoscope_embryo_EmbryoID"').fetchall()
print(f'\nDistinct Embryo IDs for Patient 823589 (embryoscope_micro_prontuario):')
for row in result5:
    print(f'  {row[0]}: {row[1]} row(s)')
print(f'Total distinct embryos: {len(result5)}')
print(f'Total rows: {sum([r[1] for r in result5])}')

conn.close()

