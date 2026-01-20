#!/usr/bin/env python3
"""Investigate why data_ploidia_new has more rows"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.db_utils import get_connection

conn = get_connection(read_only=True)

print("Investigating row count difference...")
print("=" * 80)

# Check row counts
count_old = conn.execute("SELECT COUNT(*) FROM gold.data_ploidia").fetchone()[0]
count_new = conn.execute("SELECT COUNT(*) FROM gold.data_ploidia_new").fetchone()[0]

print(f"Original: {count_old} rows")
print(f"Refactored: {count_new} rows")
print(f"Difference: {count_new - count_old} extra rows")
print()

# Check for duplicates in new table
duplicates = conn.execute("""
    SELECT "Patient ID", "Slide ID", "Well", COUNT(*) as cnt
    FROM gold.data_ploidia_new
    GROUP BY "Patient ID", "Slide ID", "Well"
    HAVING COUNT(*) > 1
    ORDER BY cnt DESC
    LIMIT 10
""").df()

print(f"Duplicate rows in refactored table: {len(duplicates)}")
if len(duplicates) > 0:
    print(duplicates)
    print()

# Check sample data
print("Sample from refactored table:")
sample = conn.execute("""
    SELECT "Patient ID", "Slide ID", "Well", "Previus ET", "BMI", "Diagnosis"
    FROM gold.data_ploidia_new
    LIMIT 20
""").df()
print(sample)

conn.close()
