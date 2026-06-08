"""
run_strategy_j.py
=================
Runs Strategy J patient matching on main.mapped_patients in
database/test_mapped_patients.duckdb, and compares performance and outputs against Strategy I.
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

log_file = os.path.join(_LOG_DIR, f"run_strategy_j_{datetime.now():%Y%m%d_%H%M%S}.log")
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

# Import matching engine J
from matching_engine_j import run_strategy_j

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
        
        for col in ["prontuario_J", "clinisys_name_J", "clinisys_matched_name_J"]:
            if col.lower() in existing_cols:
                if col == "prontuario_J":
                    con.execute("UPDATE main.mapped_patients SET prontuario_J = -1")
                else:
                    con.execute(f"UPDATE main.mapped_patients SET {col} = NULL")

        # Run Strategy J
        logging.info("--- RUNNING STRATEGY J (Optimized Spelling-Tolerant) ---")
        t_start = time.perf_counter()
        df_j = run_strategy_j(
            source_con=con,
            clinisys_db_path=CLINISYS_DB,
            source_schema="main",
            source_table="mapped_patients",
            id_col="id",
            name_col="name",
            birthdate_col="birthdate",
            cpf_col="cpf",
            label="mapped_patients_j",
        )
        t_duration = time.perf_counter() - t_start
        logging.info("Strategy J completed in %.2f seconds.", t_duration)

        # Print comparison stats
        total_rows = con.execute("SELECT COUNT(*) FROM main.mapped_patients").fetchone()[0]
        matched_i = con.execute("SELECT COUNT(CASE WHEN prontuario_I != -1 THEN 1 END) FROM main.mapped_patients").fetchone()[0] if "prontuario_i" in existing_cols else 0
        matched_j = con.execute("SELECT COUNT(CASE WHEN prontuario_J != -1 THEN 1 END) FROM main.mapped_patients").fetchone()[0]

        logging.info("="*60)
        logging.info("COMPARISON BETWEEN STRATEGY I AND J")
        logging.info("="*60)
        logging.info("Total rows in test table: %d", total_rows)
        logging.info("Matched by Strategy I (dist<=1, on-the-fly): %d (%.2f%%)", matched_i, (matched_i/total_rows*100) if total_rows else 0.0)
        logging.info("Matched by Strategy J (dist<=1, optimized) : %d (%.2f%%)", matched_j, (matched_j/total_rows*100) if total_rows else 0.0)
        
        if "prontuario_i" in existing_cols:
            discrepancies = con.execute("""
                SELECT COUNT(*) FROM main.mapped_patients
                WHERE prontuario_I != prontuario_J
            """).fetchone()[0]
            logging.info("Total discrepancies between I and J: %d", discrepancies)
            if discrepancies == 0:
                logging.info("SUCCESS: Match outputs of Strategy I and J are 100% identical!")
            else:
                logging.error("FAILURE: Strategy J match results differ from Strategy I!")
                diff_df = con.execute("""
                    SELECT id, name, birthdate, prontuario_I, clinisys_name_I, prontuario_J, clinisys_name_J
                    FROM main.mapped_patients
                    WHERE prontuario_I != prontuario_J
                    LIMIT 20
                """).df()
                print(diff_df.to_string(index=False))
                sys.exit(1)
        else:
            logging.warning("Strategy I columns not found in database; skipping identity validation.")
        logging.info("="*60)

    except Exception as e:
        logging.exception("Error running strategy J: %s", e)
        sys.exit(1)
    finally:
        con.close()
        logging.info("DuckDB connection closed.")

if __name__ == "__main__":
    main()
