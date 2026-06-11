"""
run_all_strategies.py
=====================
Recreates the test DB and runs all strategies (A+B, D, E, G) in sequence,
then prints a combined comparison.
"""

import os
import sys
import logging
import time
import subprocess
from datetime import datetime

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(errors='replace')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(errors='replace')

_HERE = os.path.dirname(os.path.abspath(__file__))
_LOG_DIR = os.path.join(_HERE, "logs")
os.makedirs(_LOG_DIR, exist_ok=True)

log_file = os.path.join(_LOG_DIR, f"run_all_{datetime.now():%Y%m%d_%H%M%S}.log")
formatter = logging.Formatter("%(asctime)s %(levelname)-8s %(message)s")
fh = logging.FileHandler(log_file, encoding="utf-8")
fh.setFormatter(formatter)
sh = logging.StreamHandler(sys.stdout)
sh.setFormatter(formatter)
logging.basicConfig(level=logging.INFO, handlers=[sh, fh])


def run(script_name):
    logging.info("=" * 60)
    logging.info("RUNNING: %s", script_name)
    logging.info("=" * 60)
    t0 = time.perf_counter()
    result = subprocess.run(
        [sys.executable, os.path.join(_HERE, script_name)],
        capture_output=True, text=True, encoding="utf-8", errors="replace"
    )
    elapsed = time.perf_counter() - t0
    if result.stdout:
        for line in result.stdout.strip().splitlines():
            logging.info("[%s] %s", script_name, line)
    if result.stderr:
        for line in result.stderr.strip().splitlines():
            logging.warning("[%s] STDERR: %s", script_name, line)
    if result.returncode != 0:
        logging.error("[%s] FAILED with exit code %d", script_name, result.returncode)
        sys.exit(1)
    logging.info("[%s] Finished in %.1f seconds", script_name, elapsed)
    return elapsed


def main():
    logging.info("Recreating test database...")
    run("create_test_db.py")

    t_ab = run("run_strategies_ab.py")
    t_d  = run("run_strategy_d.py")
    t_e  = run("run_strategy_e.py")
    t_g  = run("run_strategy_g.py")
    t_h  = run("run_strategy_h.py")
    t_i  = run("run_strategy_i.py")
    t_j  = run("run_strategy_j.py")
    t_l  = run("run_strategy_l.py")

    # Final comparison
    import duckdb
    import pandas as pd

    DB_DIR = os.path.abspath(os.path.join(_HERE, "..", "database"))
    con = duckdb.connect(os.path.join(DB_DIR, "test_mapped_patients.duckdb"), read_only=True)

    total = con.execute("SELECT COUNT(*) FROM main.mapped_patients").fetchone()[0]

    info = con.execute("PRAGMA table_info('main.mapped_patients')").df()
    existing = {c.lower() for c in info["name"]}

    rows = []
    for strat, col in [("A","prontuario_A"),("B","prontuario_B"),("D","prontuario_D"),("E","prontuario_E"),("G","prontuario_G"),("H","prontuario_H"),("I","prontuario_I"),("J","prontuario_J"),("L","prontuario_L")]:
        if col.lower() in existing:
            n = con.execute(f"SELECT COUNT(CASE WHEN {col} != -1 THEN 1 END) FROM main.mapped_patients").fetchone()[0]
            rows.append({"Strategy": strat, "Matched": n, "Total": total, "Rate_%": round(n/total*100,2)})
    df_summary = pd.DataFrame(rows)

    logging.info("\n" + "="*60)
    logging.info("FINAL COMPARISON SUMMARY")
    logging.info("="*60)
    logging.info("\n%s", df_summary.to_string(index=False))
    logging.info("\nTiming: A+B=%.1fs  D=%.1fs  E=%.1fs  G=%.1fs  H=%.1fs  I=%.1fs  J=%.1fs  L=%.1fs", t_ab, t_d, t_e, t_g, t_h, t_i, t_j, t_l)

    # Conflict between E and G
    if "prontuario_e" in existing and "prontuario_g" in existing:
        n_conf = con.execute("""
            SELECT COUNT(*) FROM main.mapped_patients
            WHERE prontuario_E != prontuario_G AND prontuario_E != -1 AND prontuario_G != -1
        """).fetchone()[0]
        logging.info("E vs G conflicts (both matched, different): %d", n_conf)

    # Discrepancies between G and H
    if "prontuario_g" in existing and "prontuario_h" in existing:
        discrepancies = con.execute("""
            SELECT COUNT(*) FROM main.mapped_patients
            WHERE prontuario_G != prontuario_H
        """).fetchone()[0]
        logging.info("G vs H total discrepancies (different or one-sided match): %d", discrepancies)
        
        if discrepancies > 0:
            logging.warning("Discrepancies found between Strategy G and Strategy H!")
            sample_disc = con.execute("""
                SELECT id, name, birthdate, cpf, prontuario_G, clinisys_name_G, prontuario_H, clinisys_name_H
                FROM main.mapped_patients
                WHERE prontuario_G != prontuario_H
                LIMIT 20
            """).df()
            print(sample_disc.to_string(index=False))
        else:
            logging.info("SUCCESS: Strategy G and Strategy H matches are 100% identical!")

    # Discrepancies between H and I
    if "prontuario_h" in existing and "prontuario_i" in existing:
        discrepancies_hi = con.execute("""
            SELECT COUNT(*) FROM main.mapped_patients
            WHERE prontuario_H != prontuario_I
        """).fetchone()[0]
        logging.info("H vs I total discrepancies (different or one-sided match): %d", discrepancies_hi)
        
        if discrepancies_hi > 0:
            logging.info("Discrepancies found between Strategy H and Strategy I (due to spelling tolerance):")
            sample_disc_hi = con.execute("""
                SELECT id, name, birthdate, cpf, prontuario_H, clinisys_name_H, prontuario_I, clinisys_name_I
                FROM main.mapped_patients
                WHERE prontuario_H != prontuario_I
                LIMIT 20
            """).df()
            print(sample_disc_hi.to_string(index=False))
        else:
            logging.info("SUCCESS: Strategy H and Strategy I matches are 100% identical!")

    # Discrepancies between I and J (Identity validation)
    if "prontuario_i" in existing and "prontuario_j" in existing:
        discrepancies_ij = con.execute("""
            SELECT COUNT(*) FROM main.mapped_patients
            WHERE prontuario_I != prontuario_J
        """).fetchone()[0]
        logging.info("I vs J total discrepancies (should be 0): %d", discrepancies_ij)
        if discrepancies_ij == 0:
            logging.info("SUCCESS: Strategy I and Strategy J matches are 100% identical!")
        else:
            logging.error("FAILURE: Strategy I and Strategy J matches differ!")
            sample_disc_ij = con.execute("""
                SELECT id, name, birthdate, prontuario_I, clinisys_name_I, prontuario_J, clinisys_name_J
                FROM main.mapped_patients
                WHERE prontuario_I != prontuario_J
                LIMIT 20
            """).df()
            print(sample_disc_ij.to_string(index=False))

    # Reference cases
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

    results = []
    for cid, cname, expected in conflict_cases:
        cols_sel = []
        for strat, col in [("E","prontuario_E"),("G","prontuario_G"),("H","prontuario_H"),("I","prontuario_I"),("J","prontuario_J"),("L","prontuario_L")]:
            if col.lower() in existing:
                cols_sel.append(col)
        if not cols_sel:
            continue
        row = con.execute(
            f"SELECT {', '.join(cols_sel)} FROM main.mapped_patients WHERE id=? AND name IS NOT DISTINCT FROM ? LIMIT 1",
            (cid, cname)
        ).fetchone()
        rec = {"ID": cid, "Name": cname[:30], "Expected": expected}
        if row:
            for i, col in enumerate(cols_sel):
                strat = col.split("_")[1]
                val = row[i]
                rec[f"Got_{strat}"] = val
                rec[f"Pass_{strat}"] = "✓" if val == expected else "✗"
        results.append(rec)

    df_cases = pd.DataFrame(results)
    logging.info("\n%s", df_cases.to_string(index=False))
    con.close()


if __name__ == "__main__":
    main()
