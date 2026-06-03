"""
run_mapped_patients.py
======================
Applies Strategy C prontuario matching to:
  DB    : database/test_mapped_patients.duckdb
  Table : main.mapped_patients
  id_col   = "id"
  name_col = "name"   (supports "LASTNAME, FIRSTNAME" format)

Results are written in-place:
  mapped_patients.prontuario    BIGINT  (-1 = unmatched)
  mapped_patients.clinisys_name VARCHAR

Logs are saved to test_prontuario_matching/logs/.
"""

import os
import sys
import logging
from datetime import datetime

# ── Logging setup ─────────────────────────────────────────────────────────────
_HERE    = os.path.dirname(os.path.abspath(__file__))
_LOG_DIR = os.path.join(_HERE, "logs")
os.makedirs(_LOG_DIR, exist_ok=True)

log_file  = os.path.join(_LOG_DIR, f"mapped_patients_{datetime.now():%Y%m%d_%H%M%S}.log")
formatter = logging.Formatter("%(asctime)s %(levelname)-8s %(message)s")

fh = logging.FileHandler(log_file, encoding="utf-8")
fh.setFormatter(formatter)
sh = logging.StreamHandler(sys.stdout)
sh.setFormatter(formatter)
logging.basicConfig(level=logging.INFO, handlers=[sh, fh])

# ── Paths ──────────────────────────────────────────────────────────────────────
DB_DIR      = os.path.abspath(os.path.join(_HERE, "..", "database"))
CLINISYS_DB = os.path.join(DB_DIR, "clinisys_all.duckdb")
SOURCE_DB   = os.path.join(DB_DIR, "test_mapped_patients.duckdb")

logging.info("Source DB : %s", SOURCE_DB)
logging.info("Clinisys  : %s", CLINISYS_DB)
logging.info("Log file  : %s", log_file)

# ── Run matching ───────────────────────────────────────────────────────────────
import duckdb
from match_prontuario import match_prontuario   # sibling module

con = duckdb.connect(SOURCE_DB)   # read-write so UPDATE can run

stats = match_prontuario(
    source_con    = con,
    db_path       = CLINISYS_DB,
    source_schema = "main",
    source_table  = "mapped_patients",
    id_col        = "id",
    name_col      = "name",
    label         = "mapped_patients",
)

# ── Report ─────────────────────────────────────────────────────────────────────
print("\n=== Results ===")
for k, v in stats.items():
    if k != "groups":
        print(f"  {k}: {v}")

print("\n=== Discovered groups ===")
for g in stats["groups"]:
    print(f"  [{g['role']}] nome={g['nome_col']}  prontuario_cols={g['prontuario_cols']}")

print("\n=== Sample matched rows ===")
sample = con.execute("""
    SELECT id, name, prontuario, clinisys_name
    FROM   main.mapped_patients
    WHERE  prontuario IS NOT NULL AND prontuario != -1
    LIMIT  10
""").df()
print(sample.to_string(index=False))

print("\n=== Sample unmatched rows ===")
unmatched = con.execute("""
    SELECT id, name, prontuario
    FROM   main.mapped_patients
    WHERE  prontuario = -1 OR prontuario IS NULL
    LIMIT  10
""").df()
print(unmatched.to_string(index=False))

con.close()
