import duckdb
import pandas as pd

# Connect to database
db_path = "../database/huntington_data_lake.duckdb"
conn = duckdb.connect(db_path, read_only=True)

print("=== CHECKING FINOPS SUMMARY FLAGS ===\n")

# Get flag statistics
query = """
SELECT 
    COUNT(*) as total_patients,
    SUM(flag_cycles_no_payments) as cycles_no_payments,
    SUM(flag_more_cycles_than_payments) as more_cycles_than_payments,
    SUM(flag_no_cycles_but_payments) as no_cycles_but_payments,
    SUM(flag_less_cycles_than_payments) as less_cycles_than_payments,
    SUM(CASE WHEN cycle_total > 0 AND fiv_paid_count > 0 AND cycle_total = fiv_paid_count THEN 1 ELSE 0 END) as perfect_match
FROM gold.finops_summary
"""

stats = conn.execute(query).fetchdf()
print("Flag Statistics:")
print(f"  Total patients: {stats['total_patients'].iloc[0]:,}")
print(f"  Cycles but no payments: {stats['cycles_no_payments'].iloc[0]:,}")
print(f"  More cycles than payments: {stats['more_cycles_than_payments'].iloc[0]:,}")
print(f"  No cycles but payments: {stats['no_cycles_but_payments'].iloc[0]:,}")
print(f"  Less cycles than payments: {stats['less_cycles_than_payments'].iloc[0]:,}")
print(f"  Perfect match (cycles = payments): {stats['perfect_match'].iloc[0]:,}")

# Show some examples of each flag
print("\n=== EXAMPLES OF EACH FLAG ===")

# Flag 1: Cycles but no payments
print("\n1. Cycles but no payments (flag_cycles_no_payments = 1):")
query1 = """
SELECT prontuario, cycle_total, fiv_paid_count, fiv_paid_total
FROM gold.finops_summary 
WHERE flag_cycles_no_payments = 1 
ORDER BY cycle_total DESC 
LIMIT 5
"""
examples1 = conn.execute(query1).fetchdf()
print(examples1.to_string(index=False))

# Flag 2: More cycles than payments
print("\n2. More cycles than payments (flag_more_cycles_than_payments = 1):")
query2 = """
SELECT prontuario, cycle_total, fiv_paid_count, fiv_paid_total
FROM gold.finops_summary 
WHERE flag_more_cycles_than_payments = 1 
ORDER BY (cycle_total - fiv_paid_count) DESC 
LIMIT 5
"""
examples2 = conn.execute(query2).fetchdf()
print(examples2.to_string(index=False))

# Flag 3: No cycles but payments
print("\n3. No cycles but payments (flag_no_cycles_but_payments = 1):")
query3 = """
SELECT prontuario, cycle_total, fiv_paid_count, fiv_paid_total
FROM gold.finops_summary 
WHERE flag_no_cycles_but_payments = 1 
ORDER BY fiv_paid_total DESC 
LIMIT 5
"""
examples3 = conn.execute(query3).fetchdf()
print(examples3.to_string(index=False))

# Flag 4: Less cycles than payments
print("\n4. Less cycles than payments (flag_less_cycles_than_payments = 1):")
query4 = """
SELECT prontuario, cycle_total, fiv_paid_count, fiv_paid_total
FROM gold.finops_summary 
WHERE flag_less_cycles_than_payments = 1 
ORDER BY (fiv_paid_count - cycle_total) DESC 
LIMIT 5
"""
examples4 = conn.execute(query4).fetchdf()
print(examples4.to_string(index=False))

conn.close()
print("\n=== FLAG CHECK COMPLETE ===")
