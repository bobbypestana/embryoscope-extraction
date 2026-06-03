"""
run_strategy_f.py
=================
Runs Strategy F prontuario matching on main.mapped_patients in
database/test_mapped_patients.duckdb, and writes the results to columns with
suffix _F. Then compares performance and accuracy against A, B, D, and E.
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

log_file = os.path.join(_LOG_DIR, f"run_strategy_f_{datetime.now():%Y%m%d_%H%M%S}.log")
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
from matching_engine_f import run_strategy_f

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
            ("prontuario_F", "BIGINT"),
            ("clinisys_name_F", "VARCHAR"),
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
                # If they already exist, reset to defaults
                if "prontuario" in col_name.lower():
                    logging.info("Resetting column %s to -1...", col_name)
                    con.execute(f"UPDATE main.mapped_patients SET {col_name} = -1")
                else:
                    logging.info("Resetting column %s to NULL...", col_name)
                    con.execute(f"UPDATE main.mapped_patients SET {col_name} = NULL")

        # 2. Run Strategy F
        logging.info("--- RUNNING STRATEGY F ---")
        t_start = time.perf_counter()
        df_f = run_strategy_f(
            source_con=con,
            clinisys_db_path=CLINISYS_DB,
            source_schema="main",
            source_table="mapped_patients",
            id_col="id",
            name_col="name",
            label="mapped_patients_f",
        )
        t_duration = time.perf_counter() - t_start
        logging.info("Strategy F completed in %.2f seconds.", t_duration)
        
        logging.info("Updating Strategy F columns in mapped_patients...")
        con.register("df_f_temp", df_f)
        con.execute("""
            UPDATE main.mapped_patients AS target
            SET prontuario_F = m.prontuario,
                clinisys_name_F = m.matched_name
            FROM df_f_temp m
            WHERE target.id = m.source_id
              AND target.name IS NOT DISTINCT FROM m.patient_name
        """)
        logging.info("Strategy F updates applied.")

        # Print final comparison stats
        total_rows = con.execute("SELECT COUNT(*) FROM main.mapped_patients").fetchone()[0]
        
        # Check matching rates
        stats = con.execute("""
            SELECT
                COUNT(CASE WHEN prontuario_A != -1 THEN 1 END) AS matched_a,
                COUNT(CASE WHEN prontuario_B != -1 THEN 1 END) AS matched_b,
                COUNT(CASE WHEN prontuario_D != -1 THEN 1 END) AS matched_d,
                COUNT(CASE WHEN prontuario_E != -1 THEN 1 END) AS matched_e,
                COUNT(CASE WHEN prontuario_F != -1 THEN 1 END) AS matched_f
            FROM main.mapped_patients
        """).fetchone()
        matched_a, matched_b, matched_d, matched_e, matched_f = stats
        
        # Check conflicts between E and F
        conflicts_ef = con.execute("""
            SELECT COUNT(*) FROM main.mapped_patients 
            WHERE prontuario_E != prontuario_F AND prontuario_E != -1 AND prontuario_F != -1
        """).fetchone()[0]

        logging.info("="*60)
        logging.info("STRATEGY PERFORMANCE & ACCURACY COMPARISON")
        logging.info("="*60)
        logging.info("Total rows in test table : %d", total_rows)
        logging.info("Matched by Strategy A    : %d (%.2f%%)", matched_a, (matched_a/total_rows*100) if total_rows else 0.0)
        logging.info("Matched by Strategy B    : %d (%.2f%%)", matched_b, (matched_b/total_rows*100) if total_rows else 0.0)
        logging.info("Matched by Strategy D    : %d (%.2f%%)", matched_d, (matched_d/total_rows*100) if total_rows else 0.0)
        logging.info("Matched by Strategy E    : %d (%.2f%%)", matched_e, (matched_e/total_rows*100) if total_rows else 0.0)
        logging.info("Matched by Strategy F    : %d (%.2f%%)", matched_f, (matched_f/total_rows*100) if total_rows else 0.0)
        logging.info("-" * 60)
        logging.info("Conflicts: E vs F        : %d", conflicts_ef)
        logging.info("="*60)

    except Exception as e:
        logging.exception("Error running strategy F: %s", e)
        sys.exit(1)
    finally:
        # Clean up temp table
        try:
            con.execute("DROP TABLE IF EXISTS df_f_temp")
        except Exception:
            pass
        con.close()
        logging.info("DuckDB connection closed.")

if __name__ == "__main__":
    main()
