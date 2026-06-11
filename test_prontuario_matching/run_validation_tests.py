import os
import sys
import logging
import time
from datetime import datetime
import duckdb
import pandas as pd

# ── Logging Setup ────────────────────────────────────────────────────────────
_HERE = os.path.dirname(os.path.abspath(__file__))
_LOG_DIR = os.path.join(_HERE, "logs")
os.makedirs(_LOG_DIR, exist_ok=True)

log_file = os.path.join(_LOG_DIR, f"run_validation_tests_{datetime.now():%Y%m%d_%H%M%S}.log")
formatter = logging.Formatter("%(asctime)s %(levelname)-8s %(message)s")

fh = logging.FileHandler(log_file, encoding="utf-8")
fh.setFormatter(formatter)
sh = logging.StreamHandler(sys.stdout)
sh.setFormatter(formatter)
logging.basicConfig(level=logging.INFO, handlers=[sh, fh])

# ── Paths ────────────────────────────────────────────────────────────────────
DB_DIR = os.path.abspath(os.path.join(_HERE, "..", "database"))
CLINISYS_DB = os.path.join(DB_DIR, "clinisys_all.duckdb")
SOURCE_DB   = os.path.join(DB_DIR, "test_mapped_patients.duckdb")
ARTIFACT_DIR = r"C:\Users\FilipeFurlanBellotti\.gemini\antigravity\brain\74c92848-ac2a-4f95-b400-c7f6a54f4ad4"
REPORT_PATH = os.path.join(ARTIFACT_DIR, "false_positives_report.md")

logging.info("Source DB   : %s", SOURCE_DB)
logging.info("Clinisys DB : %s", CLINISYS_DB)
logging.info("Artifact Dir: %s", ARTIFACT_DIR)
logging.info("Report Path : %s", REPORT_PATH)

def main():
    if not os.path.exists(CLINISYS_DB):
        logging.error("Clinisys DB not found at %s. Aborting.", CLINISYS_DB)
        sys.exit(1)
    if not os.path.exists(SOURCE_DB):
        logging.error("Source DB not found at %s. Aborting.", SOURCE_DB)
        sys.exit(1)

    t_start = time.perf_counter()
    con = duckdb.connect(SOURCE_DB)

    try:
        # ── 1. Attach Clinisys DB ──────────────────────────────────────────────
        logging.info("Attaching Clinisys DB...")
        con.execute(f"ATTACH '{CLINISYS_DB}' AS clinisys_all (READ_ONLY)")

        # ── 2. Add validation columns if they don't exist ──────────────────────
        logging.info("Checking schema of mapped_patients...")
        cols_df = con.execute("PRAGMA table_info('main.mapped_patients')").df()
        existing_cols = {c.lower() for c in cols_df["name"]}

        if "is_false_positive" not in existing_cols:
            logging.info("Adding column is_false_positive...")
            con.execute("ALTER TABLE main.mapped_patients ADD COLUMN is_false_positive INTEGER")
        if "validation_flags" not in existing_cols:
            logging.info("Adding column validation_flags...")
            con.execute("ALTER TABLE main.mapped_patients ADD COLUMN validation_flags VARCHAR")

        # ── 3. Reset validation columns ────────────────────────────────────────
        logging.info("Resetting validation columns...")
        # For matched rows: set to 0 (valid) and NULL (no flags)
        con.execute("UPDATE main.mapped_patients SET is_false_positive = 0, validation_flags = NULL WHERE prontuario != -1")
        # For unmatched rows: set to NULL (untested)
        con.execute("UPDATE main.mapped_patients SET is_false_positive = NULL, validation_flags = NULL WHERE prontuario = -1")

        # ── 4. Create cleaned temporary tables ─────────────────────────────────
        logging.info("Creating cleaned temporary tables...")
        con.execute("""
            CREATE OR REPLACE TEMP TABLE __cleaned_patients AS
            SELECT 
                id,
                source,
                name,
                birthdate,
                cpf,
                prontuario,
                -- Clean/normalize name (same logic as matching engine)
                regexp_replace(
                    strip_accents(
                        lower(
                            trim(
                                CASE
                                    WHEN name IS NOT NULL THEN
                                        CASE
                                            WHEN POSITION(',' IN name) > 0 THEN
                                                SPLIT_PART(name, ',', 2) || ' ' || SPLIT_PART(name, ',', 1)
                                            ELSE name
                                        END
                                    ELSE NULL
                                END
                            )
                        )
                    ),
                    '[^a-z]', ' ', 'g'
                ) AS norm_name,
                -- Clean CPF
                CASE
                    WHEN length(regexp_replace(cpf, '[^0-9]', '', 'g')) < 9 THEN NULL
                    WHEN lpad(regexp_replace(cpf, '[^0-9]', '', 'g'), 11, '0') IN (
                        '00000000000', '11111111111', '22222222222', '33333333333', '44444444444',
                        '55555555555', '66666666666', '77777777777', '88888888888', '99999999999'
                    ) THEN NULL
                    ELSE lpad(regexp_replace(cpf, '[^0-9]', '', 'g'), 11, '0')
                END AS clean_cpf,
                -- Parse DOB
                TRY_CAST(birthdate AS DATE) AS parsed_dob
            FROM main.mapped_patients
            WHERE prontuario != -1;
        """)

        con.execute("""
            CREATE OR REPLACE TEMP TABLE __cleaned_clinisys AS
            SELECT
                codigo,
                -- Esposa
                esposa_nome,
                esposa_nascimento,
                esposa_cpf,
                prontuario_esposa,
                regexp_replace(strip_accents(lower(trim(esposa_nome))), '[^a-z]', ' ', 'g') AS norm_esposa_nome,
                regexp_replace(strip_accents(lower(trim(esposa_nome_social))), '[^a-z]', ' ', 'g') AS norm_esposa_social,
                TRY_CAST(try_strptime(esposa_nascimento, '%d/%m/%Y') AS DATE) AS esposa_dob,
                lpad(regexp_replace(esposa_cpf, '[^0-9]', '', 'g'), 11, '0') AS esposa_cpf_clean,
                
                -- Marido
                marido_nome,
                marido_nascimento,
                marido_cpf,
                prontuario_marido,
                regexp_replace(strip_accents(lower(trim(marido_nome))), '[^a-z]', ' ', 'g') AS norm_marido_nome,
                regexp_replace(strip_accents(lower(trim(marido_nome_social))), '[^a-z]', ' ', 'g') AS norm_marido_social,
                TRY_CAST(try_strptime(marido_nascimento, '%d/%m/%Y') AS DATE) AS marido_dob,
                lpad(regexp_replace(marido_cpf, '[^0-9]', '', 'g'), 11, '0') AS marido_cpf_clean,

                -- Responsavel
                responsavel_nome,
                responsavel_nascimento,
                responsavel_cpf,
                prontuario_responsavel1 AS responsavel_id,
                regexp_replace(strip_accents(lower(trim(responsavel_nome))), '[^a-z]', ' ', 'g') AS norm_responsavel_nome,
                TRY_CAST(try_strptime(responsavel_nascimento, '%d/%m/%Y') AS DATE) AS responsavel_dob,
                lpad(regexp_replace(responsavel_cpf, '[^0-9]', '', 'g'), 11, '0') AS responsavel_cpf_clean
            FROM clinisys_all.silver.view_pacientes
            WHERE codigo IN (SELECT DISTINCT prontuario FROM __cleaned_patients);
        """)

        # ── 5. Run Validation Checks ───────────────────────────────────────────
        
        # Rule 1: CPF Mismatch
        logging.info("Checking Rule 1: CPF_MISMATCH...")
        con.execute("""
            UPDATE main.mapped_patients
            SET validation_flags = CASE WHEN validation_flags IS NULL THEN 'CPF_MISMATCH' ELSE validation_flags || ',CPF_MISMATCH' END,
                is_false_positive = 1
            FROM __cleaned_patients p
            JOIN __cleaned_clinisys c ON p.prontuario = c.codigo
            WHERE main.mapped_patients.id = p.id 
              AND main.mapped_patients.source = p.source
              AND main.mapped_patients.name IS NOT DISTINCT FROM p.name
              AND main.mapped_patients.cpf IS NOT DISTINCT FROM p.cpf
              AND main.mapped_patients.birthdate IS NOT DISTINCT FROM p.birthdate
              AND p.clean_cpf IS NOT NULL
              AND (c.esposa_cpf_clean IS NOT NULL OR c.marido_cpf_clean IS NOT NULL OR c.responsavel_cpf_clean IS NOT NULL)
              AND (c.esposa_cpf_clean IS NULL OR p.clean_cpf != c.esposa_cpf_clean)
              AND (c.marido_cpf_clean IS NULL OR p.clean_cpf != c.marido_cpf_clean)
              AND (c.responsavel_cpf_clean IS NULL OR p.clean_cpf != c.responsavel_cpf_clean);
        """)

        # Rule 2: DOB Mismatch (with no matching CPF)
        logging.info("Checking Rule 2: DOB_MISMATCH / DOB_WARNING...")
        # DOB_MISMATCH: Different DOB, low name similarity (<0.85) -> False Positive
        con.execute("""
            UPDATE main.mapped_patients
            SET validation_flags = CASE WHEN validation_flags IS NULL THEN 'DOB_MISMATCH' ELSE validation_flags || ',DOB_MISMATCH' END,
                is_false_positive = 1
            FROM __cleaned_patients p
            JOIN __cleaned_clinisys c ON p.prontuario = c.codigo
            WHERE main.mapped_patients.id = p.id 
              AND main.mapped_patients.source = p.source
              AND main.mapped_patients.name IS NOT DISTINCT FROM p.name
              AND main.mapped_patients.cpf IS NOT DISTINCT FROM p.cpf
              AND main.mapped_patients.birthdate IS NOT DISTINCT FROM p.birthdate
              AND p.parsed_dob IS NOT NULL
              AND (c.esposa_dob IS NOT NULL OR c.marido_dob IS NOT NULL OR c.responsavel_dob IS NOT NULL)
              AND (c.esposa_dob IS NULL OR (CASE WHEN DAY(p.parsed_dob) = 1 THEN DATE_TRUNC('month', p.parsed_dob) != DATE_TRUNC('month', c.esposa_dob) ELSE p.parsed_dob != c.esposa_dob END))
              AND (c.marido_dob IS NULL OR (CASE WHEN DAY(p.parsed_dob) = 1 THEN DATE_TRUNC('month', p.parsed_dob) != DATE_TRUNC('month', c.marido_dob) ELSE p.parsed_dob != c.marido_dob END))
              AND (c.responsavel_dob IS NULL OR (CASE WHEN DAY(p.parsed_dob) = 1 THEN DATE_TRUNC('month', p.parsed_dob) != DATE_TRUNC('month', c.responsavel_dob) ELSE p.parsed_dob != c.responsavel_dob END))
              -- No matching CPF
              AND NOT (p.clean_cpf IS NOT NULL AND (p.clean_cpf = c.esposa_cpf_clean OR p.clean_cpf = c.marido_cpf_clean OR p.clean_cpf = c.responsavel_cpf_clean))
              AND GREATEST(
                  COALESCE(jaro_winkler_similarity(p.norm_name, c.norm_esposa_nome), 0),
                  COALESCE(jaro_winkler_similarity(p.norm_name, c.norm_esposa_social), 0),
                  COALESCE(jaro_winkler_similarity(p.norm_name, c.norm_marido_nome), 0),
                  COALESCE(jaro_winkler_similarity(p.norm_name, c.norm_marido_social), 0),
                  COALESCE(jaro_winkler_similarity(p.norm_name, c.norm_responsavel_nome), 0)
              ) < 0.85;
        """)
        # DOB_WARNING: Different DOB, high name similarity (>=0.85) -> Typo Warning (not a False Positive)
        con.execute("""
            UPDATE main.mapped_patients
            SET validation_flags = CASE WHEN validation_flags IS NULL THEN 'DOB_WARNING' ELSE validation_flags || ',DOB_WARNING' END
            FROM __cleaned_patients p
            JOIN __cleaned_clinisys c ON p.prontuario = c.codigo
            WHERE main.mapped_patients.id = p.id 
              AND main.mapped_patients.source = p.source
              AND main.mapped_patients.name IS NOT DISTINCT FROM p.name
              AND main.mapped_patients.cpf IS NOT DISTINCT FROM p.cpf
              AND main.mapped_patients.birthdate IS NOT DISTINCT FROM p.birthdate
              AND p.parsed_dob IS NOT NULL
              AND (c.esposa_dob IS NOT NULL OR c.marido_dob IS NOT NULL OR c.responsavel_dob IS NOT NULL)
              AND (c.esposa_dob IS NULL OR (CASE WHEN DAY(p.parsed_dob) = 1 THEN DATE_TRUNC('month', p.parsed_dob) != DATE_TRUNC('month', c.esposa_dob) ELSE p.parsed_dob != c.esposa_dob END))
              AND (c.marido_dob IS NULL OR (CASE WHEN DAY(p.parsed_dob) = 1 THEN DATE_TRUNC('month', p.parsed_dob) != DATE_TRUNC('month', c.marido_dob) ELSE p.parsed_dob != c.marido_dob END))
              AND (c.responsavel_dob IS NULL OR (CASE WHEN DAY(p.parsed_dob) = 1 THEN DATE_TRUNC('month', p.parsed_dob) != DATE_TRUNC('month', c.responsavel_dob) ELSE p.parsed_dob != c.responsavel_dob END))
              -- No matching CPF
              AND NOT (p.clean_cpf IS NOT NULL AND (p.clean_cpf = c.esposa_cpf_clean OR p.clean_cpf = c.marido_cpf_clean OR p.clean_cpf = c.responsavel_cpf_clean))
              AND GREATEST(
                  COALESCE(jaro_winkler_similarity(p.norm_name, c.norm_esposa_nome), 0),
                  COALESCE(jaro_winkler_similarity(p.norm_name, c.norm_esposa_social), 0),
                  COALESCE(jaro_winkler_similarity(p.norm_name, c.norm_marido_nome), 0),
                  COALESCE(jaro_winkler_similarity(p.norm_name, c.norm_marido_social), 0),
                  COALESCE(jaro_winkler_similarity(p.norm_name, c.norm_responsavel_nome), 0)
              ) >= 0.85;
        """)

        # Rule 3: Name Mismatch (Jaro-Winkler < 0.75 for all roles)
        logging.info("Checking Rule 3: NAME_MISMATCH...")
        con.execute("""
            UPDATE main.mapped_patients
            SET validation_flags = CASE WHEN validation_flags IS NULL THEN 'NAME_MISMATCH' ELSE validation_flags || ',NAME_MISMATCH' END,
                is_false_positive = 1
            FROM __cleaned_patients p
            JOIN __cleaned_clinisys c ON p.prontuario = c.codigo
            WHERE main.mapped_patients.id = p.id 
              AND main.mapped_patients.source = p.source
              AND main.mapped_patients.name IS NOT DISTINCT FROM p.name
              AND main.mapped_patients.cpf IS NOT DISTINCT FROM p.cpf
              AND main.mapped_patients.birthdate IS NOT DISTINCT FROM p.birthdate
              AND p.norm_name IS NOT NULL AND p.norm_name != ''
              AND GREATEST(
                  COALESCE(jaro_winkler_similarity(p.norm_name, c.norm_esposa_nome), 0),
                  COALESCE(jaro_winkler_similarity(p.norm_name, c.norm_esposa_social), 0),
                  COALESCE(jaro_winkler_similarity(p.norm_name, c.norm_marido_nome), 0),
                  COALESCE(jaro_winkler_similarity(p.norm_name, c.norm_marido_social), 0),
                  COALESCE(jaro_winkler_similarity(p.norm_name, c.norm_responsavel_nome), 0)
              ) < 0.75;
        """)

        # Rule 4: Multiple Patients (role conflicts or multiple patients mapping to the same role)
        logging.info("Checking Rule 4: MULTIPLE_PATIENTS...")
        # Step 4a: Determine best matched role for each patient
        con.execute("""
            CREATE OR REPLACE TEMP TABLE __patient_roles AS
            SELECT 
                p.id,
                p.source,
                p.norm_name,
                p.prontuario,
                COALESCE(jaro_winkler_similarity(p.norm_name, c.norm_esposa_nome), 0) AS sim_esposa,
                COALESCE(jaro_winkler_similarity(p.norm_name, c.norm_marido_nome), 0) AS sim_marido,
                COALESCE(jaro_winkler_similarity(p.norm_name, c.norm_responsavel_nome), 0) AS sim_responsavel,
                CASE 
                    WHEN sim_esposa >= sim_marido AND sim_esposa >= sim_responsavel THEN 'esposa'
                    WHEN sim_marido >= sim_esposa AND sim_marido >= sim_responsavel THEN 'marido'
                    ELSE 'responsavel'
                END AS best_role
            FROM __cleaned_patients p
            JOIN __cleaned_clinisys c ON p.prontuario = c.codigo;
        """)
        # Step 4b: Find conflicts where different physical names map to the same prontuario + role
        con.execute("""
            CREATE OR REPLACE TEMP TABLE __multiple_patients_conflicts AS
            SELECT DISTINCT p1.id, p1.source, p1.norm_name
            FROM __patient_roles p1
            JOIN __patient_roles p2 
              ON p1.prontuario = p2.prontuario 
             AND p1.best_role = p2.best_role
             AND (p1.id != p2.id OR p1.source != p2.source)
             AND p1.norm_name != '' AND p2.norm_name != ''
             AND jaro_winkler_similarity(p1.norm_name, p2.norm_name) < 0.80;
        """)
        con.execute("""
            UPDATE main.mapped_patients
            SET validation_flags = CASE WHEN validation_flags IS NULL THEN 'MULTIPLE_PATIENTS' ELSE validation_flags || ',MULTIPLE_PATIENTS' END,
                is_false_positive = 1
            FROM __cleaned_patients cp
            WHERE main.mapped_patients.id = cp.id
              AND main.mapped_patients.source = cp.source
              AND main.mapped_patients.name IS NOT DISTINCT FROM cp.name
              AND main.mapped_patients.cpf IS NOT DISTINCT FROM cp.cpf
              AND main.mapped_patients.birthdate IS NOT DISTINCT FROM cp.birthdate
              AND EXISTS (
                  SELECT 1 FROM __multiple_patients_conflicts m
                  WHERE cp.id = m.id AND cp.source = m.source AND cp.norm_name = m.norm_name
              );
        """)

        # Rule 5: Gender Mismatch (Removed)
        logging.info("Rule 5 (GENDER_MISMATCH) has been removed.")

        t_duration = time.perf_counter() - t_start
        logging.info("Validation completed in %.2f seconds.", t_duration)

        # ── 6. Generate Summary Statistics ─────────────────────────────────────
        logging.info("Gathering statistics...")
        
        total_rows = con.execute("SELECT COUNT(*) FROM main.mapped_patients").fetchone()[0]
        matched_rows = con.execute("SELECT COUNT(*) FROM main.mapped_patients WHERE prontuario != -1").fetchone()[0]
        unmatched_rows = total_rows - matched_rows
        
        false_positives = con.execute("SELECT COUNT(*) FROM main.mapped_patients WHERE is_false_positive = 1").fetchone()[0]
        true_positives = matched_rows - false_positives
        
        # Breakdown by Source
        df_source = con.execute("""
            SELECT 
                source,
                COUNT(*) as total,
                COUNT(CASE WHEN prontuario != -1 THEN 1 END) as matched,
                COUNT(CASE WHEN is_false_positive = 1 THEN 1 END) as false_positives,
                COUNT(CASE WHEN is_false_positive = 0 THEN 1 END) as true_positives
            FROM main.mapped_patients
            GROUP BY source
            ORDER BY source
        """).df()
        
        # Breakdown by Validation Flag
        df_flags = con.execute("""
            SELECT 
                flag,
                COUNT(*) as count
            FROM (
                SELECT unnest(string_split(validation_flags, ',')) as flag
                FROM main.mapped_patients
                WHERE validation_flags IS NOT NULL
            )
            GROUP BY flag
            ORDER BY count DESC
        """).df()

        # Query all false positives with all compared fields and similarity scores
        df_all_fp = con.execute("""
            SELECT 
                p.source,
                p.id as source_id,
                p.name as source_name,
                p.birthdate as source_dob,
                p.cpf as source_cpf,
                p.clean_cpf as source_clean_cpf,
                p.prontuario,
                c.esposa_nome,
                c.esposa_nascimento,
                c.esposa_cpf,
                c.esposa_cpf_clean,
                c.prontuario_esposa as esposa_id_origem,
                c.marido_nome,
                c.marido_nascimento,
                c.marido_cpf,
                c.marido_cpf_clean,
                c.prontuario_marido as marido_id_origem,
                c.responsavel_nome,
                c.responsavel_nascimento,
                c.responsavel_cpf,
                c.responsavel_cpf_clean,
                c.responsavel_id as responsavel_id_origem,
                jaro_winkler_similarity(p.norm_name, c.norm_esposa_nome) as sim_esposa,
                jaro_winkler_similarity(p.norm_name, c.norm_marido_nome) as sim_marido,
                jaro_winkler_similarity(p.norm_name, c.norm_responsavel_nome) as sim_responsavel,
                mp.validation_flags
            FROM main.mapped_patients mp
            JOIN __cleaned_patients p 
              ON mp.id = p.id 
             AND mp.source = p.source 
             AND mp.name IS NOT DISTINCT FROM p.name 
             AND mp.cpf IS NOT DISTINCT FROM p.cpf 
             AND mp.birthdate IS NOT DISTINCT FROM p.birthdate
            JOIN __cleaned_clinisys c ON p.prontuario = c.codigo
            WHERE mp.is_false_positive = 1
        """).df()

        # Clean |@||@| from raw Clinisys fields
        for col in ['esposa_nome', 'esposa_cpf', 'esposa_nascimento', 'marido_nome', 'marido_cpf', 'marido_nascimento', 'responsavel_nome', 'responsavel_cpf', 'responsavel_nascimento']:
            if col in df_all_fp.columns:
                df_all_fp[col] = df_all_fp[col].astype(str).str.replace('|@||@|', '', regex=False).str.replace('|@|', '', regex=False).str.strip()
                df_all_fp[col] = df_all_fp[col].replace({'nan': None, '': None, 'None': None})

        # Save all false positives to CSV
        CSV_PATH = os.path.join(ARTIFACT_DIR, "false_positives_all.csv")
        df_all_fp.to_csv(CSV_PATH, index=False, encoding="utf-8")
        logging.info("Detailed false positives saved to %s", CSV_PATH)

        # Get a sample of 25 for the markdown report table
        df_samples = df_all_fp.sort_values(by=['validation_flags', 'source', 'source_id']).head(25)

        # ── 7. Write Markdown Report ───────────────────────────────────────────
        logging.info("Writing Markdown report...")
        os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
        
        with open(REPORT_PATH, "w", encoding="utf-8") as f:
            f.write("# Patient Matching Validation & False Positives Report\n\n")
            f.write(f"Report generated on: **{datetime.now():%Y-%m-%d %H:%M:%S}**\n")
            f.write(f"Validation completed in: **{t_duration:.2f} seconds**\n\n")
            
            f.write("## 1. High-Level Summary\n\n")
            f.write("| Metric | Value | Percentage of Matched | Percentage of Total |\n")
            f.write("| :--- | :---: | :---: | :---: |\n")
            f.write(f"| **Total Rows** | {total_rows:,} | - | 100.00% |\n")
            f.write(f"| **Unmatched Rows (prontuario = -1)** | {unmatched_rows:,} | - | {(unmatched_rows/total_rows*100):.2f}% |\n")
            f.write(f"| **Matched Rows** | {matched_rows:,} | 100.00% | {(matched_rows/total_rows*100):.2f}% |\n")
            f.write(f"| **True Positives (Verified Matches)** | {true_positives:,} | **{(true_positives/matched_rows*100):.2f}%** | {(true_positives/total_rows*100):.2f}% |\n")
            f.write(f"| **False Positives (Flagged Mismatches)** | {false_positives:,} | **{(false_positives/matched_rows*100):.2f}%** | {(false_positives/total_rows*100):.2f}% |\n\n")
            
            f.write("## 2. Breakdown by Data Source\n\n")
            f.write("| Source | Total Rows | Matched Rows | True Positives | False Positives | False Positive Rate |\n")
            f.write("| :--- | :---: | :---: | :---: | :---: | :---: |\n")
            for _, r in df_source.iterrows():
                fp_rate = (r['false_positives'] / r['matched'] * 100) if r['matched'] > 0 else 0.0
                f.write(f"| {r['source']} | {r['total']:,} | {r['matched']:,} | {r['true_positives']:,} | {r['false_positives']:,} | **{fp_rate:.2f}%** |\n")
            f.write("\n")
            
            f.write("## 3. Breakdown by Failure Type\n\n")
            f.write("A single match can be flagged with multiple failure types (e.g. both CPF and DOB mismatches).\n\n")
            f.write("| Failure Flag | Description | Flagged Count | % of False Positives |\n")
            f.write("| :--- | :--- | :---: | :---: |\n")
            flag_desc = {
                "CPF_MISMATCH": "Both patient and Clinisys CPFs are present but differ.",
                "DOB_MISMATCH": "Both DOBs are present and differ, and name similarity is low (Jaro-Winkler < 0.85).",
                "DOB_WARNING": "Both DOBs differ but name similarity is high (Jaro-Winkler >= 0.85) (not flagged as false positive).",
                "NAME_MISMATCH": "Patient name has low similarity (Jaro-Winkler < 0.75) to all Clinisys names.",
                "MULTIPLE_PATIENTS": "Multiple different patients mapped to the same prontuario role."
            }
            for _, r in df_flags.iterrows():
                desc = flag_desc.get(r['flag'], "Other verification check failure.")
                pct = (r['count'] / false_positives * 100) if false_positives > 0 else 0.0
                f.write(f"| **{r['flag']}** | {desc} | {r['count']:,} | {pct:.2f}% |\n")
            f.write("\n")
            
            f.write("## 4. Detailed Export\n\n")
            f.write(f"The complete list of all **{false_positives:,}** flagged false positive matches with all compared fields, cleaned values, and similarity scores is available here:\n")
            f.write(f"- [false_positives_all.csv](file:///C:/Users/FilipeFurlanBellotti/.gemini/antigravity/brain/74c92848-ac2a-4f95-b400-c7f6a54f4ad4/false_positives_all.csv) (Contains all compared fields side-by-side)\n\n")
            
            f.write("## 5. Sample Flagged False Positives\n\n")
            f.write("The table below shows a sample of 25 matches flagged as potential false positives, displaying all compared fields:\n\n")
            f.write("| Source | Source Patient | Clinisys Wife (Matched Prontuario) | Clinisys Husband (Matched Prontuario) | Clinisys Responsible (Matched Prontuario) | Flags |\n")
            f.write("| :--- | :--- | :--- | :--- | :--- | :--- |\n")
            for _, r in df_samples.iterrows():
                dob_val = r['source_dob'] if pd.notna(r['source_dob']) else "-"
                cpf_val = r['source_cpf'] if pd.notna(r['source_cpf']) else "-"
                src_patient = f"**ID**: {r['source_id']}<br>**Name**: {r['source_name']}<br>**DOB**: {dob_val}<br>**CPF**: {cpf_val}"
                
                w_nome = r['esposa_nome'] if pd.notna(r['esposa_nome']) else "-"
                w_dob = r['esposa_nascimento'] if pd.notna(r['esposa_nascimento']) else "-"
                w_cpf = r['esposa_cpf'] if pd.notna(r['esposa_cpf']) else "-"
                w_id = f"{int(float(r['esposa_id_origem']))}" if pd.notna(r['esposa_id_origem']) and str(r['esposa_id_origem']).strip() != '' and str(r['esposa_id_origem']).strip().lower() != 'none' else "-"
                w_sim = f"{r['sim_esposa']*100:.1f}%" if pd.notna(r['sim_esposa']) else "-"
                wife_info = f"**ID**: {w_id}<br>**Name**: {w_nome}<br>**DOB**: {w_dob}<br>**CPF**: {w_cpf}<br>**Name Sim**: {w_sim}"
                
                h_nome = r['marido_nome'] if pd.notna(r['marido_nome']) else "-"
                h_dob = r['marido_nascimento'] if pd.notna(r['marido_nascimento']) else "-"
                h_cpf = r['marido_cpf'] if pd.notna(r['marido_cpf']) else "-"
                h_id = f"{int(float(r['marido_id_origem']))}" if pd.notna(r['marido_id_origem']) and str(r['marido_id_origem']).strip() != '' and str(r['marido_id_origem']).strip().lower() != 'none' else "-"
                h_sim = f"{r['sim_marido']*100:.1f}%" if pd.notna(r['sim_marido']) else "-"
                husband_info = f"**ID**: {h_id}<br>**Name**: {h_nome}<br>**DOB**: {h_dob}<br>**CPF**: {h_cpf}<br>**Name Sim**: {h_sim}"

                r_nome = r['responsavel_nome'] if pd.notna(r['responsavel_nome']) else "-"
                r_dob = r['responsavel_nascimento'] if pd.notna(r['responsavel_nascimento']) else "-"
                r_cpf = r['responsavel_cpf'] if pd.notna(r['responsavel_cpf']) else "-"
                r_id = f"{int(float(r['responsavel_id_origem']))}" if pd.notna(r['responsavel_id_origem']) and str(r['responsavel_id_origem']).strip() != '' and str(r['responsavel_id_origem']).strip().lower() != 'none' else "-"
                r_sim = f"{r['sim_responsavel']*100:.1f}%" if pd.notna(r['sim_responsavel']) else "-"
                resp_info = f"**ID**: {r_id}<br>**Name**: {r_nome}<br>**DOB**: {r_dob}<br>**CPF**: {r_cpf}<br>**Name Sim**: {r_sim}"
                
                f.write(f"| {r['source']} | {src_patient} | {wife_info} | {husband_info} | {resp_info} | `{r['validation_flags']}` |\n")
            f.write("\n")
            f.write("> [!NOTE]\n")
            f.write("> The complete list of flagged matches has been updated in `mapped_patients` with `is_false_positive = 1`. You can query them using:\n")
            f.write("> `SELECT * FROM mapped_patients WHERE is_false_positive = 1;`\n")

        logging.info("Markdown report written successfully!")

    except Exception as e:
        logging.exception("Error during validation script execution: %s", e)
        sys.exit(1)
    finally:
        con.close()
        logging.info("DuckDB connection closed.")

if __name__ == "__main__":
    main()
