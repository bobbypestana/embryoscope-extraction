#!/usr/bin/env python3
"""
Create gold.finops_summary table with patient-level FIV cycle analysis
"""

import duckdb as db
import pandas as pd
from datetime import datetime

def get_database_connection():
	"""Create and return a connection to the huntington_data_lake database"""
	# Resolve DB path relative to repository root
	import os
	repo_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
	path_to_db = os.path.join(repo_root, 'database', 'huntington_data_lake.duckdb')
	conn = db.connect(path_to_db)
	
	print(f"Connected to database: {path_to_db}")
	return conn

def create_finops_summary_table(conn):
	"""Create the gold.finops_summary table"""
	
	print("Creating gold.finops_summary table...")
	
	# Drop table if it exists
	conn.execute("DROP TABLE IF EXISTS gold.finops_summary")
	
	# Create the table with robust JSON parsing and normalization for Resultado do Tratamento
	create_table_query = """
	CREATE TABLE gold.finops_summary AS
	WITH fiv_events AS (
		SELECT
			prontuario,
			-- Extract possible JSON keys
			COALESCE(
				json_extract_string(additional_info, '$."ResultadoTratamento"'),
				json_extract_string(additional_info, '$."Resultado Tratamento"'),
				json_extract_string(additional_info, '$."Resultado do Tratamento"')
			) AS resultado_raw
		FROM gold.recent_patients_timeline
		WHERE reference_value = 'Ciclo a Fresco FIV'
			AND flag_date_estimated = FALSE
	),
	normalized AS (
		SELECT
			prontuario,
			-- normalize: trim spaces, lower, collapse multiple spaces
			LOWER(
				regexp_replace(
					COALESCE(resultado_raw, ''),
					'\\s+',
					' '
				)
			) AS resultado_norm
		FROM fiv_events
	),
	per_patient AS (
		SELECT
			prontuario,
			COUNT(CASE
				WHEN resultado_norm NOT IN ('no transfer', '') THEN 1
			END) AS cycle_with_transfer,
			COUNT(CASE
				WHEN resultado_norm IN (
					'no transfer',
					'sem transferencia',
					'sem transferência',
					'sem transfer',
					''
				) THEN 1
			END) AS cycle_without_transfer
		FROM normalized
		GROUP BY prontuario
	)
	SELECT 
		prontuario,
		cycle_with_transfer,
		cycle_without_transfer
	FROM per_patient
	ORDER BY prontuario
	"""
	
	conn.execute(create_table_query)
	
	# Verify the table was created
	table_stats = conn.execute("""
		SELECT 
			COUNT(*) as total_patients,
			SUM(cycle_with_transfer) as total_cycles_with_transfer,
			SUM(cycle_without_transfer) as total_cycles_without_transfer,
			COUNT(CASE WHEN cycle_with_transfer > 0 THEN 1 END) as patients_with_transfer,
			COUNT(CASE WHEN cycle_without_transfer > 0 THEN 1 END) as patients_without_transfer
		FROM gold.finops_summary
	""").fetchdf()
	
	print("Table created successfully.")
	print("Table Statistics:")
	print(f"   - Total patients: {table_stats['total_patients'].iloc[0]:,}")
	print(f"   - Total cycles with transfer: {table_stats['total_cycles_with_transfer'].iloc[0]:,}")
	print(f"   - Total cycles without transfer: {table_stats['total_cycles_without_transfer'].iloc[0]:,}")
	print(f"   - Patients with transfer cycles: {table_stats['patients_with_transfer'].iloc[0]:,}")
	print(f"   - Patients with no-transfer cycles: {table_stats['patients_without_transfer'].iloc[0]:,}")
	
	return table_stats

def analyze_finops_summary_data(conn):
	"""Analyze the data in the finops_summary table"""
	
	print("\nAnalyzing finops_summary data...")
	
	# Sample data
	sample_data = conn.execute("""
		SELECT *
		FROM gold.finops_summary
		WHERE cycle_with_transfer > 0 OR cycle_without_transfer > 0
		ORDER BY (cycle_with_transfer + cycle_without_transfer) DESC
		LIMIT 10
	""").fetchdf()
	
	print("\nSample Data (Top 10 patients by total cycles):")
	print(sample_data.to_string(index=False))
	
	# Distribution analysis
	distribution = conn.execute("""
		SELECT 
			CASE 
				WHEN cycle_with_transfer = 0 AND cycle_without_transfer = 0 THEN 'No FIV cycles'
				WHEN cycle_with_transfer > 0 AND cycle_without_transfer = 0 THEN 'Only transfer cycles'
				WHEN cycle_with_transfer = 0 AND cycle_without_transfer > 0 THEN 'Only no-transfer cycles'
				ELSE 'Mixed cycles'
			END as patient_type,
			COUNT(*) as patient_count,
			SUM(cycle_with_transfer) as total_transfer_cycles,
			SUM(cycle_without_transfer) as total_no_transfer_cycles
		FROM gold.finops_summary
		GROUP BY 
			CASE 
				WHEN cycle_with_transfer = 0 AND cycle_without_transfer = 0 THEN 'No FIV cycles'
				WHEN cycle_with_transfer > 0 AND cycle_without_transfer = 0 THEN 'Only transfer cycles'
				WHEN cycle_with_transfer = 0 AND cycle_without_transfer > 0 THEN 'Only no-transfer cycles'
				ELSE 'Mixed cycles'
			END
		ORDER BY patient_count DESC
	""").fetchdf()
	
	print("\nPatient Distribution by Cycle Type:")
	print(distribution.to_string(index=False))

def main():
	"""Main function to create the finops_summary table"""
	
	print("=== CREATING FINOPS SUMMARY TABLE ===")
	print(f"Timestamp: {datetime.now()}")
	print()
	
	try:
		# Connect to database
		conn = get_database_connection()
		
		# Create the table
		table_stats = create_finops_summary_table(conn)
		
		# Analyze the data
		analyze_finops_summary_data(conn)
		
		# Quick check for the reported patient
		row = conn.execute("SELECT * FROM gold.finops_summary WHERE prontuario = 175583").fetchdf()
		if not row.empty:
			print("\nCheck prontuario 175583:")
			print(row.to_string(index=False))
		
		print(f"\nSuccessfully created gold.finops_summary table")
		print(f"Table contains {table_stats['total_patients'].iloc[0]:,} patients")
		print(f"Total FIV cycles: {table_stats['total_cycles_with_transfer'].iloc[0] + table_stats['total_cycles_without_transfer'].iloc[0]:,}")
		
	except Exception as e:
		print(f"Error: {e}")
		raise
	finally:
		if 'conn' in locals():
			conn.close()

if __name__ == "__main__":
	main()
