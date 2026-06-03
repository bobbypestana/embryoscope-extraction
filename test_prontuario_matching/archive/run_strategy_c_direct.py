"""
Run strategy_c_mapped_patients.sql directly against test_mapped_patients.duckdb.
This is the baseline: pure SQL, SELECT only, no UPDATE overhead.
"""
import os, sys, time, logging
from datetime import datetime
import duckdb

_HERE    = os.path.dirname(os.path.abspath(__file__))
_LOG_DIR = os.path.join(_HERE, "logs")
os.makedirs(_LOG_DIR, exist_ok=True)

log_file  = os.path.join(_LOG_DIR, f"strategy_c_direct_{datetime.now():%Y%m%d_%H%M%S}.log")
formatter = logging.Formatter("%(asctime)s %(levelname)-8s %(message)s")
fh = logging.FileHandler(log_file, encoding="utf-8")
fh.setFormatter(formatter)
sh = logging.StreamHandler(sys.stdout)
sh.setFormatter(formatter)
logging.basicConfig(level=logging.INFO, handlers=[sh, fh])

DB_DIR      = os.path.abspath(os.path.join(_HERE, "..", "database"))
CLINISYS_DB = os.path.join(DB_DIR, "clinisys_all.duckdb")
SOURCE_DB   = os.path.join(DB_DIR, "test_mapped_patients.duckdb")
SQL_FILE    = os.path.join(_HERE, "strategy_c_mapped_patients.sql")

logging.info("Source DB : %s", SOURCE_DB)
logging.info("Clinisys  : %s", CLINISYS_DB)

con = duckdb.connect()   # in-memory — attach both DBs read-only

t0 = time.perf_counter()
con.execute(f"ATTACH '{CLINISYS_DB}' AS clinisys_all (READ_ONLY)")
logging.info("  [T] ATTACH clinisys_all       %4.0f ms", (time.perf_counter() - t0) * 1000)

t0 = time.perf_counter()
con.execute(f"ATTACH '{SOURCE_DB}' AS src (READ_ONLY)")
con.execute("ATTACH ':memory:' AS main_db")  # not needed, main is already in-memory
# Map the table into main schema so SQL can reference main.mapped_patients
con.execute("CREATE TABLE main.mapped_patients AS SELECT * FROM src.main.mapped_patients")
logging.info("  [T] Load mapped_patients       %4.0f ms", (time.perf_counter() - t0) * 1000)

sql = open(SQL_FILE, encoding="utf-8").read()

logging.info("Running strategy_c_mapped_patients.sql ...")
t0 = time.perf_counter()
result = con.execute(sql).df()
elapsed = (time.perf_counter() - t0) * 1000
logging.info("  [T] Query execution            %4.0f ms", elapsed)

total     = len(result)
matched   = (result["prontuario"] != -1).sum()
unmatched = total - matched
logging.info("Stats -- total: %s | matched: %s | unmatched: %s | rate: %.2f%%",
             total, matched, unmatched, matched / total * 100 if total else 0)

print("\n=== Sample matched ===")
print(result[result["prontuario"] != -1].head(10).to_string(index=False))
print("\n=== Sample unmatched ===")
print(result[result["prontuario"] == -1].head(10).to_string(index=False))

con.close()
