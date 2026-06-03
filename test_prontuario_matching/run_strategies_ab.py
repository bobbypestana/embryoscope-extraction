"""
run_strategies_ab.py
====================
Runs Strategy A and Strategy B prontuario matching on main.mapped_patients in
database/test_mapped_patients.duckdb, and writes the results to columns with
suffixes _A and _B.
"""

import os
import sys
import logging
from datetime import datetime
import duckdb
import pandas as pd

# --- Logging Setup ---
_HERE = os.path.dirname(os.path.abspath(__file__))
_LOG_DIR = os.path.join(_HERE, "logs")
os.makedirs(_LOG_DIR, exist_ok=True)

log_file = os.path.join(_LOG_DIR, f"run_strategies_ab_{datetime.now():%Y%m%d_%H%M%S}.log")
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

# Import matching engines
from matching_engine_a import run_strategy_a
from matching_engine_b import run_strategy_b

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
            ("prontuario_A", "BIGINT"),
            ("clinisys_name_A", "VARCHAR"),
            ("prontuario_B", "BIGINT"),
            ("clinisys_name_B", "VARCHAR"),
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
                # If they already exist, reset prontuario columns to -1
                if "prontuario" in col_name.lower():
                    logging.info("Resetting column %s to -1...", col_name)
                    con.execute(f"UPDATE main.mapped_patients SET {col_name} = -1")
                else:
                    logging.info("Resetting column %s to NULL...", col_name)
                    con.execute(f"UPDATE main.mapped_patients SET {col_name} = NULL")

        # 2. Run Strategy A
        logging.info("--- RUNNING STRATEGY A ---")
        df_a = run_strategy_a(
            source_con=con,
            clinisys_db_path=CLINISYS_DB,
            source_schema="main",
            source_table="mapped_patients",
            id_col="id",
            name_col="name",
            label="mapped_patients_a",
        )
        
        logging.info("Strategy A completed. Resolving matched names from Clinisys view...")
        con.register("df_a_temp", df_a)
        
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE matched_a_resolved AS
            SELECT
                a.source_id,
                a.prontuario,
                CASE
                    WHEN a.prontuario = -1 THEN NULL
                    WHEN a.match_type LIKE '%esposa%' THEN c.esposa_nome
                    WHEN a.match_type LIKE '%marido%' THEN c.marido_nome
                    WHEN a.match_type LIKE '%responsavel%' THEN c.responsavel_nome
                    ELSE COALESCE(c.esposa_nome, c.marido_nome, c.responsavel_nome)
                END AS matched_name
            FROM df_a_temp a
            LEFT JOIN clinisys_all.silver.view_pacientes c
                   ON a.prontuario = c.codigo
        """)
        
        logging.info("Updating Strategy A columns in mapped_patients...")
        con.execute("""
            UPDATE main.mapped_patients AS target
            SET prontuario_A = m.prontuario,
                clinisys_name_A = m.matched_name
            FROM matched_a_resolved m
            WHERE target.id = m.source_id
        """)
        logging.info("Strategy A updates applied.")

        # 3. Run Strategy B
        logging.info("--- RUNNING STRATEGY B ---")
        step_configs = [
            {
                "name": "id",
                "id_col": "id",
                "name_col_a": "name",
                "name_col_b": None,
                "is_numeric_id": False,
                "numeric_filter": False,
            }
        ]
        
        df_b = run_strategy_b(
            source_con=con,
            clinisys_db_path=CLINISYS_DB,
            source_schema="main",
            source_table="mapped_patients",
            step_configs=step_configs,
            label="mapped_patients_b",
        )
        
        logging.info("Strategy B completed. Resolving matched names from Clinisys view...")
        con.register("df_b_temp", df_b)
        
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE matched_b_resolved AS
            SELECT
                b.source_id,
                b.prontuario,
                CASE
                    WHEN b.prontuario = -1 THEN NULL
                    WHEN b.match_type LIKE '%esposa%' THEN c.esposa_nome
                    WHEN b.match_type LIKE '%marido%' THEN c.marido_nome
                    WHEN b.match_type LIKE '%responsavel%' THEN c.responsavel_nome
                    ELSE COALESCE(c.esposa_nome, c.marido_nome, c.responsavel_nome)
                END AS matched_name
            FROM df_b_temp b
            LEFT JOIN clinisys_all.silver.view_pacientes c
                   ON b.prontuario = c.codigo
        """)
        
        logging.info("Updating Strategy B columns in mapped_patients...")
        con.execute("""
            UPDATE main.mapped_patients AS target
            SET prontuario_B = m.prontuario,
                clinisys_name_B = m.matched_name
            FROM matched_b_resolved m
            WHERE target.id = m.source_id
        """)
        logging.info("Strategy B updates applied.")

        # Print final comparison stats
        total_rows = con.execute("SELECT COUNT(*) FROM main.mapped_patients").fetchone()[0]
        stats = con.execute("""
            SELECT
                COUNT(CASE WHEN prontuario_A != -1 THEN 1 END) AS matched_a,
                COUNT(CASE WHEN prontuario_B != -1 THEN 1 END) AS matched_b,
                COUNT(CASE WHEN prontuario_A != -1 AND prontuario_B != -1 THEN 1 END) AS matched_both,
                COUNT(CASE WHEN prontuario_A != prontuario_B AND prontuario_A != -1 AND prontuario_B != -1 THEN 1 END) AS mismatch_prontuario
            FROM main.mapped_patients
        """).fetchone()
        
        matched_a, matched_b, matched_both, mismatch_prontuario = stats
        
        logging.info("="*60)
        logging.info("RUN COMPARISON SUMMARY")
        logging.info("="*60)
        logging.info("Total rows in test table: %d", total_rows)
        logging.info("Matched by Strategy A  : %d (%.2f%%)", matched_a, (matched_a/total_rows*100) if total_rows else 0.0)
        logging.info("Matched by Strategy B  : %d (%.2f%%)", matched_b, (matched_b/total_rows*100) if total_rows else 0.0)
        logging.info("Matched by both        : %d", matched_both)
        logging.info("Conflicting prontuarios: %d", mismatch_prontuario)
        logging.info("="*60)

        # Show samples
        logging.info("Sample matches (Strategy A vs B):")
        sample_df = con.execute("""
            SELECT id, name, prontuario_A, clinisys_name_A, prontuario_B, clinisys_name_B
            FROM main.mapped_patients
            WHERE prontuario_A != -1 OR prontuario_B != -1
            LIMIT 15
        """).df()
        print(sample_df.to_string(index=False))

    except Exception as e:
        logging.exception("Error running strategies comparison: %s", e)
        sys.exit(1)
    finally:
        # Clean up temp tables
        try:
            con.execute("DROP TABLE IF EXISTS matched_a_resolved")
            con.execute("DROP TABLE IF EXISTS matched_b_resolved")
        except Exception:
            pass
        con.close()
        logging.info("DuckDB connection closed.")

if __name__ == "__main__":
    main()
