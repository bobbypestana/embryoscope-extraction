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
	
	# Create the table with FIV cycle data AND billing data
	create_table_query = """
	CREATE TABLE gold.finops_summary AS
	WITH timeline_summary AS (
		SELECT
			prontuario,
			COUNT(CASE
				WHEN LOWER(COALESCE(resultado_tratamento, '')) NOT IN ('no transfer', '') THEN 1
			END) AS cycle_with_transfer,
			COUNT(CASE
				WHEN LOWER(COALESCE(resultado_tratamento, '')) IN (
					'no transfer',
					'sem transferencia',
					'sem transferência',
					'sem transfer',
					''
				) THEN 1
			END) AS cycle_without_transfer,
					MIN(event_date) AS timeline_first_date,
		MAX(event_date) AS timeline_last_date,
		-- Unidade for timeline events
		unidade AS timeline_unidade
		FROM gold.recent_patients_timeline
		WHERE reference_value IN ('Ciclo a Fresco FIV', 'Ciclo de Congelados')
			AND flag_date_estimated = FALSE
		GROUP BY prontuario, unidade
	),
	-- Define payment conditions in a flexible way
	billing_conditions AS (
		SELECT 
			prontuario,
			"Total",
			"DT Emissao",
			"Unidade",
			-- FIV Initial payment conditions
			CASE WHEN "Cta-Ctbl" LIKE '%RECEITA DE FIV%' THEN 1 ELSE 0 END AS is_fiv_initial,
			-- FIV Extra payment conditions  
			CASE WHEN "Descricao" = 'COLETA - DUOSTIM' THEN 1 ELSE 0 END AS is_fiv_extra,
			-- Egg Reception payment conditions
			CASE WHEN "Cta-Ctbl" LIKE '%RECEITA DE DOE%' THEN 1 ELSE 0 END AS is_cycle_egg_reception,
			-- Transfer Receipt payment conditions
			CASE WHEN "Cta-Ctbl" LIKE '%RECEITA DE TRANSFERENCIA - FET/FOT (EXCEDENTE)%' THEN 1 ELSE 0 END AS is_transfer_receipt,
			-- Add more payment categories here as needed
			-- CASE WHEN [condition] THEN 1 ELSE 0 END AS is_new_category,
			-- Combined FIV payment flag
			CASE WHEN "Cta-Ctbl" LIKE '%RECEITA DE FIV%' OR "Descricao" = 'COLETA - DUOSTIM' THEN 1 ELSE 0 END AS is_fiv_payment
		FROM silver.diario_vendas
		WHERE prontuario IS NOT NULL
	),
	billing_summary AS (
		SELECT
			prontuario,
			-- FIV Initial billing
			SUM(is_fiv_initial) AS fiv_initial_paid_count,
			SUM(CASE WHEN is_fiv_initial = 1 THEN "Total" ELSE 0 END) AS fiv_initial_paid_total,
			-- FIV Extra billing
			SUM(is_fiv_extra) AS fiv_extra_paid_count,
			SUM(CASE WHEN is_fiv_extra = 1 THEN "Total" ELSE 0 END) AS fiv_extra_paid_total,
			-- Egg Reception billing
			SUM(is_cycle_egg_reception) AS cycle_egg_reception_paid_count,
			SUM(CASE WHEN is_cycle_egg_reception = 1 THEN "Total" ELSE 0 END) AS cycle_egg_reception_paid_total,
			-- Transfer Receipt billing
			SUM(is_transfer_receipt) AS transfer_receipt_paid_count,
			SUM(CASE WHEN is_transfer_receipt = 1 THEN "Total" ELSE 0 END) AS transfer_receipt_paid_total,
			-- Add more payment categories here as needed
			-- SUM(is_new_category) AS new_category_paid_count,
			-- SUM(CASE WHEN is_new_category = 1 THEN "Total" ELSE 0 END) AS new_category_paid_total,
					-- Date ranges for billing (all FIV payments)
		MIN(CASE WHEN is_fiv_payment = 1 OR is_cycle_egg_reception = 1 OR is_transfer_receipt = 1 THEN "DT Emissao" END) AS billing_first_date,
		MAX(CASE WHEN is_fiv_payment = 1 OR is_cycle_egg_reception = 1 OR is_transfer_receipt = 1 THEN "DT Emissao" END) AS billing_last_date,
		-- Unidade for FIV payments
		"Unidade" AS billing_unidade
		FROM billing_conditions
		GROUP BY prontuario, "Unidade"
	)
	SELECT 
		COALESCE(t.prontuario, b.prontuario) AS prontuario,
		COALESCE(t.cycle_with_transfer, 0) AS cycle_with_transfer,
		COALESCE(t.cycle_without_transfer, 0) AS cycle_without_transfer,
		COALESCE(t.cycle_with_transfer, 0) + COALESCE(t.cycle_without_transfer, 0) AS cycle_total,
		COALESCE(b.fiv_initial_paid_count, 0) AS fiv_initial_paid_count,
		COALESCE(b.fiv_initial_paid_total, 0) AS fiv_initial_paid_total,
		COALESCE(b.fiv_extra_paid_count, 0) AS fiv_extra_paid_count,
		COALESCE(b.fiv_extra_paid_total, 0) AS fiv_extra_paid_total,
		COALESCE(b.cycle_egg_reception_paid_count, 0) AS cycle_egg_reception_paid_count,
		COALESCE(b.cycle_egg_reception_paid_total, 0) AS cycle_egg_reception_paid_total,
		COALESCE(b.transfer_receipt_paid_count, 0) AS transfer_receipt_paid_count,
		COALESCE(b.transfer_receipt_paid_total, 0) AS transfer_receipt_paid_total,
		-- Combined totals
		COALESCE(b.fiv_initial_paid_count, 0) + COALESCE(b.fiv_extra_paid_count, 0) + COALESCE(b.cycle_egg_reception_paid_count, 0) + COALESCE(b.transfer_receipt_paid_count, 0) AS fiv_paid_count,
		COALESCE(b.fiv_initial_paid_total, 0) + COALESCE(b.fiv_extra_paid_total, 0) + COALESCE(b.cycle_egg_reception_paid_total, 0) + COALESCE(b.transfer_receipt_paid_total, 0) AS fiv_paid_total,
		-- Date ranges
		t.timeline_first_date,
		t.timeline_last_date,
		b.billing_first_date,
		b.billing_last_date,
		-- Unidade information
		t.timeline_unidade,
		b.billing_unidade,
		-- Flags
		CASE WHEN (COALESCE(t.cycle_with_transfer, 0) + COALESCE(t.cycle_without_transfer, 0)) > 0 
		     AND (COALESCE(b.fiv_initial_paid_count, 0) + COALESCE(b.fiv_extra_paid_count, 0) + COALESCE(b.cycle_egg_reception_paid_count, 0) + COALESCE(b.transfer_receipt_paid_count, 0)) = 0 
		     THEN 1 ELSE 0 END AS flag_cycles_no_payments,
		CASE WHEN (COALESCE(t.cycle_with_transfer, 0) + COALESCE(t.cycle_without_transfer, 0)) > 
		          (COALESCE(b.fiv_initial_paid_count, 0) + COALESCE(b.fiv_extra_paid_count, 0) + COALESCE(b.cycle_egg_reception_paid_count, 0) + COALESCE(b.transfer_receipt_paid_count, 0)) 
		     THEN 1 ELSE 0 END AS flag_more_cycles_than_payments,
		CASE WHEN (COALESCE(t.cycle_with_transfer, 0) + COALESCE(t.cycle_without_transfer, 0)) = 0 
		     AND (COALESCE(b.fiv_initial_paid_count, 0) + COALESCE(b.fiv_extra_paid_count, 0) + COALESCE(b.cycle_egg_reception_paid_count, 0) + COALESCE(b.transfer_receipt_paid_count, 0)) > 0 
		     THEN 1 ELSE 0 END AS flag_no_cycles_but_payments,
		CASE WHEN (COALESCE(t.cycle_with_transfer, 0) + COALESCE(t.cycle_without_transfer, 0)) < 
		          (COALESCE(b.fiv_initial_paid_count, 0) + COALESCE(b.fiv_extra_paid_count, 0) + COALESCE(b.cycle_egg_reception_paid_count, 0) + COALESCE(b.transfer_receipt_paid_count, 0)) 
		     THEN 1 ELSE 0 END AS flag_less_cycles_than_payments
	FROM timeline_summary t
	FULL OUTER JOIN billing_summary b ON t.prontuario = b.prontuario
	ORDER BY COALESCE(t.prontuario, b.prontuario)
	"""
	
	conn.execute(create_table_query)
	
	# Verify the table was created
	table_stats = conn.execute("""
		SELECT 
			COUNT(*) as total_patients,
			SUM(cycle_with_transfer) as total_cycles_with_transfer,
			SUM(cycle_without_transfer) as total_cycles_without_transfer,
			SUM(fiv_initial_paid_count) as total_fiv_initial_paid_count,
			SUM(fiv_initial_paid_total) as total_fiv_initial_paid_total,
			SUM(fiv_extra_paid_count) as total_fiv_extra_paid_count,
			SUM(fiv_extra_paid_total) as total_fiv_extra_paid_total,
			SUM(cycle_egg_reception_paid_count) as total_cycle_egg_reception_paid_count,
			SUM(cycle_egg_reception_paid_total) as total_cycle_egg_reception_paid_total,
					SUM(transfer_receipt_paid_count) as total_transfer_receipt_paid_count,
		SUM(transfer_receipt_paid_total) as total_transfer_receipt_paid_total,
			SUM(fiv_paid_count) as total_fiv_paid_count,
			SUM(fiv_paid_total) as total_fiv_paid_total,
			COUNT(CASE WHEN cycle_with_transfer > 0 OR cycle_without_transfer > 0 THEN 1 END) as patients_with_timeline,
			COUNT(CASE WHEN fiv_paid_count > 0 THEN 1 END) as patients_with_billing
		FROM gold.finops_summary
	""").fetchdf()
	
	print("Table created successfully.")
	print("Table Statistics:")
	print(f"   - Total patients: {table_stats['total_patients'].iloc[0]:,}")
	print(f"   - Patients with timeline data: {table_stats['patients_with_timeline'].iloc[0]:,}")
	print(f"   - Patients with billing data: {table_stats['patients_with_billing'].iloc[0]:,}")
	print(f"   - Total FIV cycles (timeline): {table_stats['total_cycles_with_transfer'].iloc[0] + table_stats['total_cycles_without_transfer'].iloc[0]:,}")
	print(f"   - Total FIV billing items: {table_stats['total_fiv_paid_count'].iloc[0]:,}")
	print(f"   - Total FIV billing amount: R$ {table_stats['total_fiv_paid_total'].iloc[0]:,.2f}")
	
	return table_stats

def analyze_finops_summary_data(conn):
	"""Analyze the data in the finops_summary table"""
	
	print("\nAnalyzing finops_summary data...")
	
	# Sample data
	sample_data = conn.execute("""
		SELECT *
		FROM gold.finops_summary
		WHERE (cycle_with_transfer > 0 OR cycle_without_transfer > 0) OR (fiv_paid_count > 0)
		ORDER BY (cycle_with_transfer + cycle_without_transfer + fiv_paid_count) DESC
		LIMIT 10
	""").fetchdf()
	
	print("\nSample Data (Top 10 patients by total activity):")
	print(sample_data.to_string(index=False))
	
	# Distribution analysis
	distribution = conn.execute("""
		SELECT 
			CASE 
				WHEN (cycle_with_transfer > 0 OR cycle_without_transfer > 0) AND fiv_paid_count > 0 THEN 'Timeline + Billing'
				WHEN cycle_with_transfer > 0 OR cycle_without_transfer > 0 THEN 'Timeline only'
				WHEN fiv_paid_count > 0 THEN 'Billing only'
				ELSE 'No activity'
			END as patient_type,
			COUNT(*) as patient_count,
			SUM(cycle_with_transfer) as total_transfer_cycles,
			SUM(cycle_without_transfer) as total_no_transfer_cycles,
			SUM(fiv_paid_count) as total_billing_items,
			SUM(fiv_paid_total) as total_billing_amount
		FROM gold.finops_summary
		GROUP BY 
			CASE 
				WHEN (cycle_with_transfer > 0 OR cycle_without_transfer > 0) AND fiv_paid_count > 0 THEN 'Timeline + Billing'
				WHEN cycle_with_transfer > 0 OR cycle_without_transfer > 0 THEN 'Timeline only'
				WHEN fiv_paid_count > 0 THEN 'Billing only'
				ELSE 'No activity'
			END
		ORDER BY patient_count DESC
	""").fetchdf()
	
	print("\nPatient Distribution by Data Type:")
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
		
		print(f"\nSuccessfully created gold.finops_summary table with billing data")
		print(f"Table contains {table_stats['total_patients'].iloc[0]:,} patients")
		print(f"Total FIV cycles: {table_stats['total_cycles_with_transfer'].iloc[0] + table_stats['total_cycles_without_transfer'].iloc[0]:,}")
		print(f"Total FIV billing: R$ {table_stats['total_fiv_paid_total'].iloc[0]:,.2f}")
		
	except Exception as e:
		print(f"Error: {e}")
		raise
	finally:
		if 'conn' in locals():
			conn.close()

if __name__ == "__main__":
	main()
