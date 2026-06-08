"""
run_production_startegy_v1.py
==============================
Runs Strategy H patient matching in production mode on main.mapped_patients in
database/test_mapped_patients.duckdb.
Updates only the 'prontuario' column without any suffixes.
"""

from run_production_strategy_v1 import main

if __name__ == "__main__":
    main()
