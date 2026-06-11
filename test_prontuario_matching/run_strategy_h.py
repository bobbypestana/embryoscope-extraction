"""
run_strategy_h.py
=================
Runs Strategy H prontuario matching on main.mapped_patients in
database/test_mapped_patients.duckdb, and compares performance and accuracy.
"""

import os
import sys
import logging
import time
from datetime import datetime
import duckdb
import pandas as pd

# --- Logging Setup ---
_HERE = os.path.dirname(os.path.abspath(__file__))
_LOG_DIR = os.path.join(_HERE, "logs")
os.makedirs(_LOG_DIR, exist_ok=True)

log_file = os.path.join(_LOG_DIR, f"run_strategy_h_{datetime.now():%Y%m%d_%H%M%S}.log")
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
logging.info("Log file    : %s", log_file)

# Import matching engine
from prontuario_matching_v1 import run_strategy_l as run_strategy_h

def main():
    if not os.path.exists(CLINISYS_DB):
        logging.error("Clinisys DB not found at %s. Make sure it exists.", CLINISYS_DB)
        sys.exit(1)
    if not os.path.exists(SOURCE_DB):
        logging.error("Source DB not found at %s. Make sure it exists.", SOURCE_DB)
        sys.exit(1)

    con = duckdb.connect(SOURCE_DB)

    try:
        # 1. Reset columns if they already exist, so we do a fresh run
        cols_df = con.execute("PRAGMA table_info('main.mapped_patients')").df()
        existing_cols = {c.lower() for c in cols_df["name"]}
        
        for col in ["prontuario_H", "clinisys_name_H", "clinisys_matched_name_H"]:
            if col.lower() in existing_cols:
                if col == "prontuario_H":
                    logging.info("Resetting column %s to -1...", col)
                    con.execute("UPDATE main.mapped_patients SET prontuario_H = -1")
                else:
                    logging.info("Resetting column %s to NULL...", col)
                    con.execute(f"UPDATE main.mapped_patients SET {col} = NULL")

        # 2. Run Strategy H
        logging.info("--- RUNNING STRATEGY H ---")
        t_start = time.perf_counter()
        df_h = run_strategy_h(
            source_con=con,
            clinisys_db_path=CLINISYS_DB,
            source_schema="main",
            source_table="mapped_patients",
            id_col="id",
            name_col="name",
            birthdate_col="birthdate",
            cpf_col="cpf",
            label="mapped_patients_h",
            suffix="_H",
        )
        t_duration = time.perf_counter() - t_start
        logging.info("Strategy H completed in %.2f seconds.", t_duration)

        # 3. Print comparison stats
        total_rows = con.execute("SELECT COUNT(*) FROM main.mapped_patients").fetchone()[0]
        
        stats_queries = []
        strategies = []
        for strat in ["A", "B", "D", "E", "G"]:
            col_name = f"prontuario_{strat}"
            if col_name.lower() in existing_cols:
                stats_queries.append(f"COUNT(CASE WHEN {col_name} != -1 THEN 1 END) AS matched_{strat.lower()}")
                strategies.append(strat)
        
        stats_queries.append("COUNT(CASE WHEN prontuario_H != -1 THEN 1 END) AS matched_h")
        
        stats = con.execute(f"SELECT {', '.join(stats_queries)} FROM main.mapped_patients").fetchone()
        
        logging.info("="*60)
        logging.info("STRATEGY PERFORMANCE & ACCURACY COMPARISON (H)")
        logging.info("="*60)
        logging.info("Total rows in test table : %d", total_rows)
        for i, strat in enumerate(strategies):
            logging.info("Matched by Strategy %s    : %d (%.2f%%)", strat, stats[i], (stats[i]/total_rows*100) if total_rows else 0.0)
        logging.info("Matched by Strategy H    : %d (%.2f%%)", stats[-1], (stats[-1]/total_rows*100) if total_rows else 0.0)
        logging.info("-" * 60)
        
        # Check conflicts between G and H
        if "prontuario_g" in existing_cols:
            conflicts_gh = con.execute("""
                SELECT COUNT(*) FROM main.mapped_patients 
                WHERE prontuario_G != prontuario_H AND prontuario_G != -1 AND prontuario_H != -1
            """).fetchone()[0]
            logging.info("Conflicts: G vs H        : %d", conflicts_gh)
            
            if conflicts_gh > 0:
                logging.info("Sample mismatches (G vs H where both match):")
                sample_gh = con.execute("""
                    SELECT id, name, birthdate, cpf, prontuario_G, clinisys_name_G, prontuario_H, clinisys_name_H, clinisys_matched_name_H
                    FROM main.mapped_patients
                    WHERE prontuario_G != prontuario_H AND prontuario_G != -1 AND prontuario_H != -1
                    LIMIT 10
                """).df()
                print(sample_gh.to_string(index=False))
                print("\n")

        # 4. Validate the 17 conflict cases specifically
        logging.info("Checking the 17 conflict cases evaluation...")
        
        conflict_cases = [
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
                SELECT prontuario_H, clinisys_name_H, clinisys_matched_name_H
                FROM main.mapped_patients
                WHERE id = ? AND name IS NOT DISTINCT FROM ?
                LIMIT 1
            """, (cid, cname)).fetchone()
            
            if row_db:
                p_h, name_esposa, name_matched = row_db
                status = "PASS" if p_h == expected_p else "FAIL"
                if status == "PASS":
                    success_count += 1
                results_list.append({
                    "ID": cid,
                    "Name": cname,
                    "Expected": expected_p,
                    "Got_H": p_h,
                    "Status": status,
                    "Clinisys_Wife": name_esposa,
                    "Clinisys_Matched": name_matched
                })
            else:
                results_list.append({
                    "ID": cid,
                    "Name": cname,
                    "Expected": expected_p,
                    "Got_H": "NOT FOUND",
                    "Status": "FAIL",
                    "Clinisys_Wife": "",
                    "Clinisys_Matched": ""
                })
                
        df_results = pd.DataFrame(results_list)
        logging.info("="*60)
        logging.info("CONFLICT CASES VERIFICATION RESULTS (%d/%d Passed)", success_count, len(conflict_cases))
        logging.info("="*60)
        print(df_results.to_string(index=False))
        logging.info("="*60)

    except Exception as e:
        logging.exception("Error running strategy H runner: %s", e)
        sys.exit(1)
    finally:
        con.close()
        logging.info("DuckDB connection closed.")

if __name__ == "__main__":
    main()
