"""
run_strategy_e.py
=================
Runs Strategy E prontuario matching on main.mapped_patients in
database/test_mapped_patients.duckdb, and writes the results to columns with
suffix _E. Then compares performance and accuracy against A and B.
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

log_file = os.path.join(_LOG_DIR, f"run_strategy_e_{datetime.now():%Y%m%d_%H%M%S}.log")
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
from matching_engine_e import run_strategy_e

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
            ("prontuario_E", "BIGINT"),
            ("clinisys_name_E", "VARCHAR"),
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

        # 2. Run Strategy E
        logging.info("--- RUNNING STRATEGY E ---")
        t_start = time.perf_counter()
        df_e = run_strategy_e(
            source_con=con,
            clinisys_db_path=CLINISYS_DB,
            source_schema="main",
            source_table="mapped_patients",
            id_col="id",
            name_col="name",
            label="mapped_patients_e",
        )
        t_duration = time.perf_counter() - t_start
        logging.info("Strategy E completed in %.2f seconds.", t_duration)
        
        logging.info("Updating Strategy E columns in mapped_patients...")
        con.register("df_e_temp", df_e)
        con.execute("""
            UPDATE main.mapped_patients AS target
            SET prontuario_E = m.prontuario,
                clinisys_name_E = m.matched_name
            FROM df_e_temp m
            WHERE target.id = m.source_id
              AND target.name IS NOT DISTINCT FROM m.patient_name
        """)
        logging.info("Strategy E updates applied.")

        # Print final comparison stats
        total_rows = con.execute("SELECT COUNT(*) FROM main.mapped_patients").fetchone()[0]
        
        # Check matching rates
        stats = con.execute("""
            SELECT
                COUNT(CASE WHEN prontuario_A != -1 THEN 1 END) AS matched_a,
                COUNT(CASE WHEN prontuario_B != -1 THEN 1 END) AS matched_b,
                COUNT(CASE WHEN prontuario_D != -1 THEN 1 END) AS matched_d,
                COUNT(CASE WHEN prontuario_E != -1 THEN 1 END) AS matched_e
            FROM main.mapped_patients
        """).fetchone()
        matched_a, matched_b, matched_d, matched_e = stats
        
        # Check conflicts
        conflicts_ae = con.execute("""
            SELECT COUNT(*) FROM main.mapped_patients 
            WHERE prontuario_A != prontuario_E AND prontuario_A != -1 AND prontuario_E != -1
        """).fetchone()[0]

        conflicts_be = con.execute("""
            SELECT COUNT(*) FROM main.mapped_patients 
            WHERE prontuario_B != prontuario_E AND prontuario_B != -1 AND prontuario_E != -1
        """).fetchone()[0]

        conflicts_de = con.execute("""
            SELECT COUNT(*) FROM main.mapped_patients 
            WHERE prontuario_D != prontuario_E AND prontuario_D != -1 AND prontuario_E != -1
        """).fetchone()[0]

        logging.info("="*60)
        logging.info("STRATEGY PERFORMANCE & ACCURACY COMPARISON")
        logging.info("="*60)
        logging.info("Total rows in test table : %d", total_rows)
        logging.info("Matched by Strategy A    : %d (%.2f%%)", matched_a, (matched_a/total_rows*100) if total_rows else 0.0)
        logging.info("Matched by Strategy B    : %d (%.2f%%)", matched_b, (matched_b/total_rows*100) if total_rows else 0.0)
        logging.info("Matched by Strategy D    : %d (%.2f%%)", matched_d, (matched_d/total_rows*100) if total_rows else 0.0)
        logging.info("Matched by Strategy E    : %d (%.2f%%)", matched_e, (matched_e/total_rows*100) if total_rows else 0.0)
        logging.info("-" * 60)
        logging.info("Conflicts: A vs E        : %d", conflicts_ae)
        logging.info("Conflicts: B vs E        : %d", conflicts_be)
        logging.info("Conflicts: D vs E        : %d", conflicts_de)
        logging.info("="*60)

        # Show conflicts sample where A and E mismatch
        if conflicts_ae > 0:
            logging.info("Sample mismatches (A vs E where both match):")
            sample_ae = con.execute("""
                SELECT id, name, prontuario_A, clinisys_name_A, prontuario_E, clinisys_name_E
                FROM main.mapped_patients
                WHERE prontuario_A != prontuario_E AND prontuario_A != -1 AND prontuario_E != -1
                LIMIT 10
            """).df()
            print(sample_ae.to_string(index=False))
            print("\n")

        # Show conflicts sample where B and E mismatch
        if conflicts_be > 0:
            logging.info("Sample mismatches (B vs E where both match):")
            sample_be = con.execute("""
                SELECT id, name, prontuario_B, clinisys_name_B, prontuario_E, clinisys_name_E
                FROM main.mapped_patients
                WHERE prontuario_B != prontuario_E AND prontuario_B != -1 AND prontuario_E != -1
                LIMIT 10
            """).df()
            print(sample_be.to_string(index=False))
            print("\n")

        # Show conflicts sample where D and E mismatch
        if conflicts_de > 0:
            logging.info("Sample mismatches (D vs E where both match):")
            sample_de = con.execute("""
                SELECT id, name, prontuario_D, clinisys_name_D, prontuario_E, clinisys_name_E
                FROM main.mapped_patients
                WHERE prontuario_D != prontuario_E AND prontuario_D != -1 AND prontuario_E != -1
                LIMIT 10
            """).df()
            print(sample_de.to_string(index=False))
            print("\n")

    except Exception as e:
        logging.exception("Error running strategy E: %s", e)
        sys.exit(1)
    finally:
        # Clean up temp table
        try:
            con.execute("DROP TABLE IF EXISTS df_e_temp")
        except Exception:
            pass
        con.close()
        logging.info("DuckDB connection closed.")

if __name__ == "__main__":
    main()
