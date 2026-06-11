"""
run_production_strategy_v1.py
==============================
Standalone production runner for Strategy L patient matching.

Based on run_strategy_l.py, adapted for production use:
  - Updates only the suffix-less 'prontuario' column (no clinisys_name columns).
  - Does not reset existing matches — only fills rows where prontuario is NULL or -1.
  - Can be copied alongside prontuario_matching_v1.py to any folder and run there.

Usage
-----
  python run_production_strategy_v1.py

DB paths are resolved relative to this script's parent directory:
  ../database/test_mapped_patients.duckdb   (source table)
  ../database/clinisys_all.duckdb           (Clinisys reference)
"""

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

log_file = os.path.join(_LOG_DIR, f"run_production_strategy_v1_{datetime.now():%Y%m%d_%H%M%S}.log")
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

logging.info("Source DB   : %s", SOURCE_DB)
logging.info("Clinisys DB : %s", CLINISYS_DB)
logging.info("Log file    : %s", log_file)

# ── Import matching engine ───────────────────────────────────────────────────
from prontuario_matching_v1 import run_strategy_l


def main():
    if not os.path.exists(CLINISYS_DB):
        logging.error("Clinisys DB not found at %s. Aborting.", CLINISYS_DB)
        sys.exit(1)
    if not os.path.exists(SOURCE_DB):
        logging.error("Source DB not found at %s. Aborting.", SOURCE_DB)
        sys.exit(1)

    con = duckdb.connect(SOURCE_DB)

    try:
        # ── 1. Initialise NULL prontuario values to -1 so we only match unmatched rows
        cols_df = con.execute("PRAGMA table_info('main.mapped_patients')").df()
        existing_cols = {c.lower() for c in cols_df["name"]}

        if "prontuario" in existing_cols:
            logging.info("Initializing NULL prontuario values to -1...")
            con.execute("UPDATE main.mapped_patients SET prontuario = -1 WHERE prontuario IS NULL")

        # ── 2. Run Strategy L in production mode (suffix="" → only 'prontuario' column)
        logging.info("=" * 60)
        logging.info("RUNNING PRODUCTION STRATEGY L (V1)")
        logging.info("=" * 60)
        t_start = time.perf_counter()

        df_result = run_strategy_l(
            source_con=con,
            clinisys_db_path=CLINISYS_DB,
            source_schema="main",
            source_table="mapped_patients",
            id_col="id",
            name_col="name",
            birthdate_col="birthdate",
            cpf_col="cpf",
            label="mapped_patients_l_prod",
            suffix="",          # production: updates only `prontuario`
        )
        t_duration = time.perf_counter() - t_start
        logging.info("Production matching completed in %.2f seconds.", t_duration)

        # ── 3. Print match statistics
        total_rows   = con.execute("SELECT COUNT(*) FROM main.mapped_patients").fetchone()[0]
        matched_rows = con.execute(
            "SELECT COUNT(CASE WHEN prontuario != -1 THEN 1 END) FROM main.mapped_patients"
        ).fetchone()[0]
        unmatched    = total_rows - matched_rows

        logging.info("=" * 60)
        logging.info("PRODUCTION MATCHING STATS")
        logging.info("=" * 60)
        logging.info("Total rows in table      : %d", total_rows)
        logging.info("Matched by Strategy L    : %d (%.2f%%)",
                     matched_rows, (matched_rows / total_rows * 100) if total_rows else 0.0)
        logging.info("Unmatched (prontuario=-1): %d (%.2f%%)",
                     unmatched, (unmatched / total_rows * 100) if total_rows else 0.0)
        logging.info("=" * 60)

    except Exception as e:
        logging.exception("Error running production strategy: %s", e)
        sys.exit(1)
    finally:
        con.close()
        logging.info("DuckDB connection closed.")


if __name__ == "__main__":
    main()
