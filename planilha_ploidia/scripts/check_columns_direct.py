
import duckdb
import os

DB_PATH = r"g:\My Drive\projetos_individuais\Huntington\database\huntington_data_lake.duckdb"

def check():
    conn = duckdb.connect(DB_PATH, read_only=True)
    conn.execute("ATTACH 'g:/My Drive/projetos_individuais/Huntington/database/clinisys_all.duckdb' AS clinisys")
    
    # List columns in gold.embryoscope_clinisys_combined
    try:
        cols = [r[0] for r in conn.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'embryoscope_clinisys_combined' AND table_schema = 'gold'").fetchall()]
    except:
        # Maybe it's not in gold schema or different name?
        cols = [r[0] for r in conn.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'embryoscope_clinisys_combined'").fetchall()]

    print(f"Total columns: {len(cols)}")
    
    # List all columns to find candidates
    try:
        conn.execute("DESCRIBE gold.data_ploidia")
    except duckdb.CatalogException:
        print("Table gold.data_ploidia not found!")
        return

    all_cols_desc = conn.fetchall()
    all_col_names = [r[0] for r in all_cols_desc]
    
    print(f"Total columns via DESCRIBE: {len(all_col_names)}")

    # Columns from 00_02_column_mapping.py
    existing_mapping = {
        "Unidade": "patient_unit_huntington",
        "Age": "AgeAtFertilization",
        "BMI": "trat1_bmi",
        "Birth Year": "patient_YearOfBirth",
        "Diagnosis": "trat1_fator_infertilidade1",
        "Patient ID": "micro_prontuario",
        "Previus ET": "trat1_previous_et",
        "Previus OD ET": "trat1_previous_et_od",
        "Oocyte History": "oocito_OrigemOocito",
        "Oocyte Source": "trat1_origem_ovulo",
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
        "Frag-2 Cat. - Value": "embryo_Value_FRAG2CAT",
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
    
    # New columns
    new_columns = [
        "outcome_type", 
        "merged_numero_de_nascidos", 
        "fet_gravidez_clinica", 
        "trat2_resultado_tratamento", 
        "trat1_resultado_tratamento", 
        "fet_tipo_resultado"
    ]
    
    print("\n--- Verifying Existing Mappings ---")
    missing_existing = []
    for target, source in existing_mapping.items():
        if source and source not in all_col_names:
            print(f"MISSING: {source} (for {target})")
            missing_existing.append(source)
    
    if not missing_existing:
        print("All existing mapped columns FOUND.")
        
    print("\n--- Verifying New Columns ---")
    missing_new = []
    for col in new_columns:
        if col not in all_col_names:
            print(f"MISSING: {col}")
            missing_new.append(col)
        else:
            print(f"FOUND: {col}")

    if missing_new:
        print(f"\nMissing {len(missing_new)} new columns.")
    else:
        print("All new columns FOUND.")

if __name__ == "__main__":
    check()
