import os
import sys
import time
import logging
from datetime import datetime
import duckdb
import pandas as pd

# --- Logging Setup ---
_HERE = os.path.dirname(os.path.abspath(__file__))
_LOG_DIR = os.path.join(_HERE, "logs")
os.makedirs(_LOG_DIR, exist_ok=True)

log_file = os.path.join(_LOG_DIR, f"run_strategy_d_{datetime.now():%Y%m%d_%H%M%S}.log")
formatter = logging.Formatter("%(asctime)s %(levelname)-8s %(message)s")

fh = logging.FileHandler(log_file, encoding="utf-8")
fh.setFormatter(formatter)
sh = logging.StreamHandler(sys.stdout)
sh.setFormatter(formatter)
logging.basicConfig(level=logging.INFO, handlers=[sh, fh])

# --- Paths ---
DB_DIR = os.path.abspath(os.path.join(_HERE, "..", "database"))
CLINISYS_DB = os.path.join(DB_DIR, "clinisys_all.duckdb")
SOURCE_DB = os.path.join(DB_DIR, "test_mapped_patients.duckdb")

logging.info("Source DB   : %s", SOURCE_DB)
logging.info("Clinisys DB : %s", CLINISYS_DB)

# Import matching engine D
from matching_engine_d import run_strategy_d

def main():
    if not os.path.exists(CLINISYS_DB):
        logging.error("Clinisys DB not found at %s. Make sure it exists.", CLINISYS_DB)
        sys.exit(1)
    if not os.path.exists(SOURCE_DB):
        logging.error("Source DB not found at %s. Make sure it exists.", SOURCE_DB)
        sys.exit(1)

    con = duckdb.connect(SOURCE_DB)

    try:
        # 1. Ensure target columns exist
        logging.info("Ensuring target columns exist in mapped_patients...")
        columns_to_add = [
            ("prontuario_D", "BIGINT"),
            ("clinisys_name_D", "VARCHAR"),
        ]
        
        info = con.execute("PRAGMA table_info('main.mapped_patients')").df()
        existing_cols = {c.lower() for c in info["name"]}
        
        for col_name, col_type in columns_to_add:
            if col_name.lower() not in existing_cols:
                logging.info("Adding column %s (%s)...", col_name, col_type)
                con.execute(f"ALTER TABLE main.mapped_patients ADD COLUMN {col_name} {col_type}")
                if "prontuario" in col_name.lower():
                    con.execute(f"UPDATE main.mapped_patients SET {col_name} = -1")
            else:
                if "prontuario" in col_name.lower():
                    logging.info("Resetting column %s to -1...", col_name)
                    con.execute(f"UPDATE main.mapped_patients SET {col_name} = -1")
                else:
                    logging.info("Resetting column %s to NULL...", col_name)
                    con.execute(f"UPDATE main.mapped_patients SET {col_name} = NULL")

        # 2. Run Strategy D
        logging.info("--- RUNNING STRATEGY D ---")
        t0 = time.perf_counter()
        df_d = run_strategy_d(
            source_con=con,
            clinisys_db_path=CLINISYS_DB,
            source_schema="main",
            source_table="mapped_patients",
            id_col="id",
            name_col="name",
            label="mapped_patients_d",
        )
        elapsed = time.perf_counter() - t0
        logging.info("Strategy D engine run time: %.2f seconds", elapsed)
        
        logging.info("Resolving matched names from Clinisys view...")
        con.register("df_d_temp", df_d)
        
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE matched_d_resolved AS
            SELECT
                d.source_id,
                d.patient_name,
                d.prontuario,
                CASE
                    WHEN d.prontuario = -1 THEN NULL
                    WHEN d.match_type = 'esposa_match' THEN c.esposa_nome
                    WHEN d.match_type = 'marido_match' THEN c.marido_nome
                    WHEN d.match_type = 'responsavel_match' THEN c.responsavel_nome
                    ELSE COALESCE(c.esposa_nome, c.marido_nome, c.responsavel_nome)
                END AS matched_name
            FROM df_d_temp d
            LEFT JOIN clinisys_all.silver.view_pacientes c
                   ON d.prontuario = c.codigo
        """)
        
        logging.info("Updating Strategy D columns in mapped_patients...")
        con.execute("""
            UPDATE main.mapped_patients AS target
            SET prontuario_D = m.prontuario,
                clinisys_name_D = m.matched_name
            FROM matched_d_resolved m
            WHERE target.id = m.source_id AND COALESCE(target.name, '') = COALESCE(m.patient_name, '')
        """)
        logging.info("Strategy D updates applied.")

        # 3. Print final comparison stats
        total_rows = con.execute("SELECT COUNT(*) FROM main.mapped_patients").fetchone()[0]
        
        # Check if columns A and B exist to print full comparison
        has_ab = "prontuario_a" in existing_cols and "prontuario_b" in existing_cols
        
        if has_ab:
            stats = con.execute("""
                SELECT
                    COUNT(CASE WHEN prontuario_A != -1 THEN 1 END) AS matched_a,
                    COUNT(CASE WHEN prontuario_B != -1 THEN 1 END) AS matched_b,
                    COUNT(CASE WHEN prontuario_D != -1 THEN 1 END) AS matched_d,
                    COUNT(CASE WHEN prontuario_A != prontuario_D AND prontuario_A != -1 AND prontuario_D != -1 THEN 1 END) AS mismatch_ad,
                    COUNT(CASE WHEN prontuario_B != prontuario_D AND prontuario_B != -1 AND prontuario_D != -1 THEN 1 END) AS mismatch_bd
                FROM main.mapped_patients
            """).fetchone()
            
            matched_a, matched_b, matched_d, mismatch_ad, mismatch_bd = stats
            
            logging.info("="*60)
            logging.info("RUN COMPARISON SUMMARY (A vs B vs D)")
            logging.info("="*60)
            logging.info("Total rows in test table: %d", total_rows)
            logging.info("Matched by Strategy A  : %d (%.2f%%)", matched_a, (matched_a/total_rows*100) if total_rows else 0.0)
            logging.info("Matched by Strategy B  : %d (%.2f%%)", matched_b, (matched_b/total_rows*100) if total_rows else 0.0)
            logging.info("Matched by Strategy D  : %d (%.2f%%)", matched_d, (matched_d/total_rows*100) if total_rows else 0.0)
            logging.info("Conflicts A vs D       : %d", mismatch_ad)
            logging.info("Conflicts B vs D       : %d", mismatch_bd)
            logging.info("="*60)
        else:
            matched_d = con.execute("SELECT COUNT(CASE WHEN prontuario_D != -1 THEN 1 END) FROM main.mapped_patients").fetchone()[0]
            logging.info("="*60)
            logging.info("RUN SUMMARY (D ONLY)")
            logging.info("="*60)
            logging.info("Total rows in test table: %d", total_rows)
            logging.info("Matched by Strategy D  : %d (%.2f%%)", matched_d, (matched_d/total_rows*100) if total_rows else 0.0)
            logging.info("="*60)

        # 4. Validate the 17 conflict cases specifically
        logging.info("Checking the 17 conflict cases evaluation...")
        
        conflict_cases = [
            # (id, name, expected_pront_c)
            ("11200", "CANGUSSU, LARISSA PORTO", 611200),
            ("34002", "SILVA, MARIA JOSE", 634002),
            ("34335", "ALVES, RENATA", 634335),
            ("35242", "GARCIA, ANA L Q M", 635242),
            ("56678", "FERREIRA, ANA FLAVIA TEIXEIRA", 656678),
            ("62206", "ROSA, ANA FLAVIA APARECIDA", 662206),
            ("65830", "CAMARGO, ANA CAROLINA FRAGA", 665830),
            ("66058", "MEDIOLI, MARINA", 666058),
            ("69038", "PEREIRA, ANA FLAVIA LOPES", 669038),
            ("77767", "FALLES, LARISSA MAIA CAMPOS", 677767),
            ("56864", "DURAN, ANA CLARA F. L.", 155006),
            ("15941", "", 775941),
            ("51851", "", 150059),
            ("14458", "GABRIELA ROCHA ALBUQUERQUE MADRUGA", 113658),
            ("59666", "LUIZ FERNANDO SIQUEIRA ULHOA CINTRA", 157789),
            ("87229", "MARCELO FALCONE HANAN", 185171),
            ("87229", "THIAGO JARJOUR", 185841),
            ("916778", "LUANA OLIMPIO RIVA", 916778),
            ("916778", "SHUANGWANG JIANG", 784426),
        ]
        
        success_count = 0
        results_list = []
        
        for cid, cname, expected_p in conflict_cases:
            # Query the database
            row_db = con.execute("""
                SELECT prontuario_D, clinisys_name_D, prontuario_A, prontuario_B
                FROM main.mapped_patients
                WHERE id = ? AND name IS NOT DISTINCT FROM ?
                LIMIT 1
            """, (cid, cname)).fetchone()
            
            if row_db:
                p_d, name_d, p_a, p_b = row_db
                status = "PASS" if p_d == expected_p else "FAIL"
                if status == "PASS":
                    success_count += 1
                results_list.append({
                    "ID": cid,
                    "Name": cname,
                    "Expected": expected_p,
                    "Got_D": p_d,
                    "Got_A": p_a,
                    "Got_B": p_b,
                    "Status": status,
                    "Matched_Name": name_d
                })
            else:
                results_list.append({
                    "ID": cid,
                    "Name": cname,
                    "Expected": expected_p,
                    "Got_D": "NOT FOUND",
                    "Got_A": "N/A",
                    "Got_B": "N/A",
                    "Status": "FAIL",
                    "Matched_Name": ""
                })
                
        df_results = pd.DataFrame(results_list)
        logging.info("="*60)
        logging.info("CONFLICT CASES VERIFICATION RESULTS (%d/%d Passed)", success_count, len(conflict_cases))
        logging.info("="*60)
        print(df_results.to_string(index=False))
        logging.info("="*60)

    except Exception as e:
        logging.exception("Error running strategy D runner: %s", e)
        sys.exit(1)
    finally:
        # Clean up temp tables
        try:
            con.execute("DROP TABLE IF EXISTS matched_d_resolved")
        except Exception:
            pass
        con.close()
        logging.info("DuckDB connection closed.")

if __name__ == "__main__":
    main()
