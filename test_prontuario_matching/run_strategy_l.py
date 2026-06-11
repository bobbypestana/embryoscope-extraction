"""
run_strategy_l.py
=================
Runs Strategy L patient matching on main.mapped_patients in
database/test_mapped_patients.duckdb, and compares performance and outputs against Strategy J.
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

log_file = os.path.join(_LOG_DIR, f"run_strategy_l_{datetime.now():%Y%m%d_%H%M%S}.log")
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

# Import matching engine L
from matching_engine_l import run_strategy_l

def main():
    if not os.path.exists(CLINISYS_DB):
        logging.error("Clinisys DB not found. Make sure it exists.")
        sys.exit(1)
    if not os.path.exists(SOURCE_DB):
        logging.error("Source DB not found.")
        sys.exit(1)

    con = duckdb.connect(SOURCE_DB)

    try:
        # Reset columns if they exist
        cols_df = con.execute("PRAGMA table_info('main.mapped_patients')").df()
        existing_cols = {c.lower() for c in cols_df["name"]}
        
        for col in ["prontuario_L", "clinisys_name_L", "clinisys_matched_name_L"]:
            if col.lower() in existing_cols:
                if col == "prontuario_L":
                    con.execute("UPDATE main.mapped_patients SET prontuario_L = -1")
                else:
                    con.execute(f"UPDATE main.mapped_patients SET {col} = NULL")

        # Run Strategy L
        logging.info("--- RUNNING STRATEGY L (Spelling & Couple Fallbacks) ---")
        t_start = time.perf_counter()
        df_l = run_strategy_l(
            source_con=con,
            clinisys_db_path=CLINISYS_DB,
            source_schema="main",
            source_table="mapped_patients",
            id_col="id",
            name_col="name",
            birthdate_col="birthdate",
            cpf_col="cpf",
            label="mapped_patients_l",
        )
        t_duration = time.perf_counter() - t_start
        logging.info("Strategy L completed in %.2f seconds.", t_duration)

        # Print comparison stats
        total_rows = con.execute("SELECT COUNT(*) FROM main.mapped_patients").fetchone()[0]
        matched_j = con.execute("SELECT COUNT(CASE WHEN prontuario_J != -1 THEN 1 END) FROM main.mapped_patients").fetchone()[0] if "prontuario_j" in existing_cols else 0
        matched_l = con.execute("SELECT COUNT(CASE WHEN prontuario_L != -1 THEN 1 END) FROM main.mapped_patients").fetchone()[0]

        logging.info("="*60)
        logging.info("COMPARISON BETWEEN STRATEGY J AND L")
        logging.info("="*60)
        logging.info("Total rows in test table: %d", total_rows)
        logging.info("Matched by Strategy J (dist<=1, optimized): %d (%.2f%%)", matched_j, (matched_j/total_rows*100) if total_rows else 0.0)
        logging.info("Matched by Strategy L (fallbacks)         : %d (%.2f%%)", matched_l, (matched_l/total_rows*100) if total_rows else 0.0)
        
        new_matches_count = con.execute("""
            SELECT COUNT(*) FROM main.mapped_patients
            WHERE prontuario_J = -1 AND prontuario_L != -1
        """).fetchone()[0] if "prontuario_j" in existing_cols else 0
        logging.info("New matches recovered by Strategy L over J: %d", new_matches_count)
        
        # Verify specific case matches
        logging.info("="*60)
        logging.info("VERIFYING SPECIFIC BENCHMARK CASES")
        logging.info("="*60)
        
        spec_cases = con.execute("""
            SELECT id, name, prontuario_J, prontuario_L, clinisys_matched_name_L
            FROM main.mapped_patients
            WHERE id IN ('880422', '220979', '850997', '751182')
            GROUP BY id, name, prontuario_J, prontuario_L, clinisys_matched_name_L
        """).df()
        logging.info("\n%s", spec_cases.to_string(index=False))
        logging.info("="*60)

    except Exception as e:
        logging.exception("Error running strategy L: %s", e)
        sys.exit(1)
    finally:
        con.close()
        logging.info("DuckDB connection closed.")

if __name__ == "__main__":
    main()
