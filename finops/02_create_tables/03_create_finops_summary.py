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
	
	# Attach clinisys_all database
	import os
	repo_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
	clinisys_db_path = os.path.join(repo_root, 'database', 'clinisys_all.duckdb')
	conn.execute(f"ATTACH '{clinisys_db_path}' AS clinisys_all")
	print(f"Attached clinisys_all database: {clinisys_db_path}")
	
	# Drop table if it exists
	conn.execute("DROP TABLE IF EXISTS gold.finops_summary")
	
	# Create the table with FIV cycle data AND billing data
	create_table_query = """
	CREATE TABLE gold.finops_summary AS
	WITH
	-- Get all patients with their most frequent unit
	patient_units AS (
		SELECT 
			prontuario,
			unidade,
			COUNT(*) as unit_count
		FROM gold.recent_patients_timeline
		WHERE unidade IS NOT NULL
		GROUP BY prontuario, unidade
	),
	patient_primary_units AS (
		SELECT 
			prontuario,
			unidade as timeline_unidade
		FROM (
			SELECT 
				prontuario,
				unidade,
				ROW_NUMBER() OVER (PARTITION BY prontuario ORDER BY unit_count DESC) as rn
			FROM patient_units
		) ranked
		WHERE rn = 1
	),
	-- Map timeline units to billing units for proper joining
	-- patient_units_mapped AS (
	-- 	SELECT 
	-- 		prontuario,
	-- 		timeline_unidade,
	-- 		-- Map timeline units to billing unit names
	-- 		CASE 
	-- 			WHEN timeline_unidade LIKE '%HTT SP - Ibirapuera%' THEN 'Ibirapuera'
	-- 			WHEN timeline_unidade LIKE '%HTT SP - Vila Mariana%' THEN 'Vila Mariana'
	-- 			WHEN timeline_unidade LIKE '%HTT SP - Campinas%' THEN 'Campinas'
	-- 			WHEN timeline_unidade LIKE '%HTT SP - ProFIV%' THEN 'Santa Joana'
	-- 			WHEN timeline_unidade LIKE '%HTT Brasília%' OR timeline_unidade LIKE '%HTT Brasilia%' THEN 'FIV Brasilia'
	-- 			WHEN timeline_unidade LIKE '%HTT SP - DOE%' THEN 'Santa Joana'
	-- 			WHEN timeline_unidade LIKE '%HTT Belo Horizonte%' THEN 'Belo Horizonte'
	-- 			WHEN timeline_unidade LIKE '%Unidade Salvador%' AND timeline_unidade NOT LIKE '%Insemina%' AND timeline_unidade NOT LIKE '%Camaçari%' AND timeline_unidade NOT LIKE '%Feira de Santana%' THEN 'Salvador - Cenafert'
	-- 			WHEN timeline_unidade LIKE '%Unidade Salvador Insemina%' THEN 'Salvador - Cenafert'
	-- 			WHEN timeline_unidade LIKE '%HTT SP - Alphaville%' THEN 'Santa Joana'
	-- 			WHEN timeline_unidade LIKE '%HTT Goiânia%' THEN 'Santa Joana'
	-- 			WHEN timeline_unidade LIKE '%HTT SP - Bauru%' THEN 'Santa Joana'
	-- 			WHEN timeline_unidade LIKE '%Unidade Salvador Camaçari%' THEN 'Salvador - Cenafert'
	-- 			WHEN timeline_unidade LIKE '%Unidade Salvador Feira de Santana%' THEN 'Salvador - Cenafert'
	-- 			ELSE timeline_unidade
	-- 		END as timeline_unidade_mapped
	-- 	FROM patient_primary_units
	-- ),
	timeline_summary AS (
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
			MAX(event_date) AS timeline_last_date
		FROM gold.recent_patients_timeline
		WHERE reference_value IN ('Ciclo a Fresco FIV', 'Ciclo de Congelados', 'Ciclo a Fresco Vitrificação')
			AND flag_date_estimated = FALSE
		GROUP BY prontuario
	),
	-- Get donor flags from clinisys_all database
	donor_flags AS (
		SELECT DISTINCT prontuario_doadora as prontuario
		FROM clinisys_all.silver.view_tratamentos
		WHERE prontuario_doadora IS NOT NULL
	),
	-- Define payment conditions using Descrição Gerencial field
	billing_conditions AS (
		SELECT 
			prontuario,
			"Total",
			"DT Emissao",
			"Unidade",
			-- All treatment categories as one unified category
			CASE WHEN "Descrição Gerencial" IN (
				'Coleta - Adicional', 'Coleta - Crio', 'FET / FOT', 
				'Pró-Fiv', 'FIV', 'Inseminação', 'Ovodoação', 'Embriodoação'
			) THEN 1 ELSE 0 END AS is_treatment_payment
		FROM silver.mesclada_vendas
		WHERE prontuario IS NOT NULL
	),
	billing_summary AS (
		SELECT
			prontuario,
			-- Treatment payment totals
			SUM(is_treatment_payment) AS treatment_paid_count,
			SUM(CASE WHEN is_treatment_payment = 1 THEN "Total" ELSE 0 END) AS treatment_paid_total,
			-- Date ranges for billing
			MIN(CASE WHEN is_treatment_payment = 1 THEN "DT Emissao" END) AS billing_first_date,
			MAX(CASE WHEN is_treatment_payment = 1 THEN "DT Emissao" END) AS billing_last_date
			-- Unidade for treatment payments
			-- "Unidade" AS billing_unidade
		FROM billing_conditions
		GROUP BY prontuario
	)
	SELECT 
		COALESCE(t.prontuario, b.prontuario) AS prontuario,
		COALESCE(t.cycle_with_transfer, 0) AS cycle_with_transfer,
		COALESCE(t.cycle_without_transfer, 0) AS cycle_without_transfer,
		COALESCE(t.cycle_with_transfer, 0) + COALESCE(t.cycle_without_transfer, 0) AS cycle_total,
		-- Treatment payment totals
		COALESCE(b.treatment_paid_count, 0) AS treatment_paid_count,
		COALESCE(b.treatment_paid_total, 0) AS treatment_paid_total,
		-- Date ranges
		t.timeline_first_date,
		t.timeline_last_date,
		b.billing_first_date,
		b.billing_last_date,
		-- Unidade information
		-- p.timeline_unidade,
		-- b.billing_unidade,
		-- Donor flag
		CASE WHEN d.prontuario IS NOT NULL THEN 1 ELSE 0 END AS flag_is_donor,
		-- Flags
		CASE WHEN (COALESCE(t.cycle_with_transfer, 0) + COALESCE(t.cycle_without_transfer, 0)) > 0 
		     AND COALESCE(b.treatment_paid_count, 0) = 0 
		     THEN 1 ELSE 0 END AS flag_cycles_no_payments,
		CASE WHEN (COALESCE(t.cycle_with_transfer, 0) + COALESCE(t.cycle_without_transfer, 0)) > 
		          COALESCE(b.treatment_paid_count, 0) 
		     THEN 1 ELSE 0 END AS flag_more_cycles_than_payments,
		CASE WHEN (COALESCE(t.cycle_with_transfer, 0) + COALESCE(t.cycle_without_transfer, 0)) = 0 
		     AND COALESCE(b.treatment_paid_count, 0) > 0 
		     THEN 1 ELSE 0 END AS flag_no_cycles_but_payments,
		CASE WHEN (COALESCE(t.cycle_with_transfer, 0) + COALESCE(t.cycle_without_transfer, 0)) < 
		          COALESCE(b.treatment_paid_count, 0) 
		     THEN 1 ELSE 0 END AS flag_less_cycles_than_payments
	FROM timeline_summary t
	FULL OUTER JOIN billing_summary b ON t.prontuario = b.prontuario
	-- LEFT JOIN patient_units_mapped p ON COALESCE(t.prontuario, b.prontuario) = p.prontuario
	LEFT JOIN donor_flags d ON COALESCE(t.prontuario, b.prontuario) = d.prontuario
	-- WHERE 
	-- 	-- Only include rows where timeline and billing units match, or where one side is missing
	-- 	(p.timeline_unidade_mapped = b.billing_unidade OR p.timeline_unidade_mapped IS NULL OR b.billing_unidade IS NULL)
	ORDER BY COALESCE(t.prontuario, b.prontuario)
	"""
	
	conn.execute(create_table_query)
	
	# Add timeline_unidade by joining with view_pacientes and view_unidades
	print("Adding timeline_unidade from patient data...")
	add_unidade_query = """
	ALTER TABLE gold.finops_summary ADD COLUMN timeline_unidade VARCHAR;
	"""
	conn.execute(add_unidade_query)
	
	# Update the timeline_unidade column with data from view_pacientes and view_unidades
	update_unidade_query = """
	UPDATE gold.finops_summary 
	SET timeline_unidade = patient_units.unidade_nome
	FROM (
		SELECT 
			p.codigo as prontuario,
			u.nome as unidade_nome
		FROM clinisys_all.silver.view_pacientes p
		LEFT JOIN clinisys_all.silver.view_unidades u ON p.unidade_origem = u.id
	) patient_units
	WHERE finops_summary.prontuario = patient_units.prontuario
	"""
	conn.execute(update_unidade_query)
	
	# Verify the table was created
	table_stats = conn.execute("""
		SELECT 
			COUNT(*) as total_patients,
			SUM(cycle_with_transfer) as total_cycles_with_transfer,
			SUM(cycle_without_transfer) as total_cycles_without_transfer,
			SUM(treatment_paid_count) as total_treatment_paid_count,
			SUM(treatment_paid_total) as total_treatment_paid_total,
			COUNT(CASE WHEN cycle_with_transfer > 0 OR cycle_without_transfer > 0 THEN 1 END) as patients_with_timeline,
			COUNT(CASE WHEN treatment_paid_count > 0 THEN 1 END) as patients_with_billing,
			COUNT(CASE WHEN flag_is_donor = 1 THEN 1 END) as patients_donors
		FROM gold.finops_summary
	""").fetchdf()
	
	print("Table created successfully.")
	print("Table Statistics:")
	print(f"   - Total patients: {table_stats['total_patients'].iloc[0]:,}")
	print(f"   - Patients with timeline data: {table_stats['patients_with_timeline'].iloc[0]:,}")
	print(f"   - Patients with billing data: {table_stats['patients_with_billing'].iloc[0]:,}")
	print(f"   - Patients who are donors: {table_stats['patients_donors'].iloc[0]:,}")
	print(f"   - Total FIV cycles (timeline): {table_stats['total_cycles_with_transfer'].iloc[0] + table_stats['total_cycles_without_transfer'].iloc[0]:,}")
	print(f"   - Total treatment billing items: {table_stats['total_treatment_paid_count'].iloc[0]:,}")
	print(f"   - Total treatment billing amount: R$ {table_stats['total_treatment_paid_total'].iloc[0]:,.2f}")
	
	return table_stats

def analyze_finops_summary_data(conn):
	"""Analyze the data in the finops_summary table"""
	
	print("\nAnalyzing finops_summary data...")
	
	# Sample data
	sample_data = conn.execute("""
		SELECT *
		FROM gold.finops_summary
		WHERE (cycle_with_transfer > 0 OR cycle_without_transfer > 0) OR (treatment_paid_count > 0)
		ORDER BY (cycle_with_transfer + cycle_without_transfer + treatment_paid_count) DESC
		LIMIT 10
	""").fetchdf()
	
	print("\nSample Data (Top 10 patients by total activity):")
	print(sample_data.to_string(index=False))
	
	# Distribution analysis
	distribution = conn.execute("""
		SELECT 
			CASE 
				WHEN (cycle_with_transfer > 0 OR cycle_without_transfer > 0) AND treatment_paid_count > 0 THEN 'Timeline + Billing'
				WHEN cycle_with_transfer > 0 OR cycle_without_transfer > 0 THEN 'Timeline only'
				WHEN treatment_paid_count > 0 THEN 'Billing only'
				ELSE 'No activity'
			END as patient_type,
			COUNT(*) as patient_count,
			SUM(cycle_with_transfer) as total_transfer_cycles,
			SUM(cycle_without_transfer) as total_no_transfer_cycles,
			SUM(treatment_paid_count) as total_billing_items,
			SUM(treatment_paid_total) as total_billing_amount
		FROM gold.finops_summary
		GROUP BY 
			CASE 
				WHEN (cycle_with_transfer > 0 OR cycle_without_transfer > 0) AND treatment_paid_count > 0 THEN 'Timeline + Billing'
				WHEN cycle_with_transfer > 0 OR cycle_without_transfer > 0 THEN 'Timeline only'
				WHEN treatment_paid_count > 0 THEN 'Billing only'
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
		print(f"Patients who are donors: {table_stats['patients_donors'].iloc[0]:,}")
		print(f"Total FIV cycles: {table_stats['total_cycles_with_transfer'].iloc[0] + table_stats['total_cycles_without_transfer'].iloc[0]:,}")
		print(f"Total treatment billing: R$ {table_stats['total_treatment_paid_total'].iloc[0]:,.2f}")
		
	except Exception as e:
		print(f"Error: {e}")
		raise
	finally:
		if 'conn' in locals():
			conn.close()

if __name__ == "__main__":
	main()
