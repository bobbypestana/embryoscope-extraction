import duckdb as db
import os
import pandas as pd

repo_root = r'g:\My Drive\projetos_individuais\Huntington'
path_to_db = os.path.join(repo_root, 'database', 'huntington_data_lake.duckdb')
conn = db.connect(path_to_db, read_only=True)

output_file = r'g:\My Drive\projetos_individuais\Huntington\planilha_ploidia\data_export\check_output.txt'

with open(output_file, 'w', encoding='utf-8') as f:
    # Get sample from Excel
    excel_file = r'g:\My Drive\projetos_individuais\Huntington\planilha_ploidia\planilhas_exemplo\Planilha IA ploidia - 344 embrioes (1).xlsx'
    df_excel = pd.read_excel(excel_file, sheet_name='Export')
    f.write('Sample Video IDs from Excel:\n')
    f.write(str(df_excel['Video ID'].head(10)))
    f.write('\n\n')

    # Try to find matching pattern in database
    f.write('Sample data from database (Patient ID + Embryo ID):\n')
    sample = conn.execute('''
        SELECT 
            "Patient ID",
            "Embryo ID",
            "Slide ID",
            CAST("Patient ID" AS VARCHAR) || ' - ' || "Embryo ID" as constructed_video_id
        FROM gold.data_ploidia 
        WHERE "Patient ID" IS NOT NULL AND "Embryo ID" IS NOT NULL
        LIMIT 10
    ''').df()
    f.write(str(sample))
    f.write('\n\n')

    # Check if any constructed IDs match Excel IDs
    excel_ids = set(df_excel['Video ID'].dropna().unique())
    f.write(f'Total unique Video IDs in Excel: {len(excel_ids)}\n')
    f.write(f'Sample Excel IDs: {list(excel_ids)[:5]}\n\n')

    # Try to find matches
    all_db_ids = conn.execute('''
        SELECT DISTINCT
            CAST("Patient ID" AS VARCHAR) || ' - ' || "Embryo ID" as video_id
        FROM gold.data_ploidia 
        WHERE "Patient ID" IS NOT NULL AND "Embryo ID" IS NOT NULL
    ''').df()

    db_ids_set = set(all_db_ids['video_id'].dropna().unique())
    f.write(f'Total unique constructed Video IDs in DB: {len(db_ids_set)}\n')
    f.write(f'Sample DB IDs: {list(db_ids_set)[:5]}\n\n')

    matches = excel_ids & db_ids_set
    f.write(f'Matching IDs: {len(matches)}\n')
    if matches:
        f.write(f'Sample matches: {list(matches)[:10]}\n')

conn.close()

print(f'Output written to: {output_file}')
