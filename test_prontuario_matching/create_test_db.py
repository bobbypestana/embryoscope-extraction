import duckdb
import os
import sys

def find_repo_root():
    """Finds the root of the repository by looking for the database directory."""
    current = os.path.abspath(os.path.dirname(__file__) if '__file__' in globals() else os.getcwd())
    while True:
        if os.path.exists(os.path.join(current, 'database')):
            return current
        parent = os.path.dirname(current)
        if parent == current:
            # Fallback to current directory
            return os.path.abspath(os.getcwd())
        current = parent

def main():
    repo_root = find_repo_root()
    source_db_path = os.path.join(repo_root, "database", "huntington_data_lake.duckdb")
    target_db_path = os.path.join(repo_root, "database", "test_mapped_patients.duckdb")

    print(f"Repository Root: {repo_root}")
    print(f"Source Database: {source_db_path}")
    print(f"Target Database: {target_db_path}")

    # Ensure database folder exists
    os.makedirs(os.path.join(repo_root, "database"), exist_ok=True)

    # 1. Connect to target database
    print(f"Connecting to target database: {target_db_path}")
    target_conn = duckdb.connect(target_db_path)

    try:
        # 2. Attach source database in read-only mode
        print(f"Attaching source database: {source_db_path} (READ_ONLY)")
        escaped_source_path = source_db_path.replace("'", "''")
        target_conn.execute(f"ATTACH '{escaped_source_path}' AS src (READ_ONLY)")

        # 3. Create schema and table in target database
        print("Creating table mapped_patients...")
        target_conn.execute("DROP TABLE IF EXISTS mapped_patients")
        target_conn.execute("""
            CREATE TABLE mapped_patients (
                id VARCHAR,
                name VARCHAR,
                birthdate VARCHAR,
                cpf VARCHAR,
                prontuario_old VARCHAR,
                reference VARCHAR,
                source VARCHAR
            )
        """)

        # 4. Ingest from Embryoscope
        print("\nIngesting from Embryoscope (silver_embryoscope.patients)...")
        target_conn.execute("""
            INSERT INTO mapped_patients (id, name, birthdate, cpf, prontuario_old, reference, source)
            SELECT DISTINCT
                CAST(PatientID AS VARCHAR) as id,
                CAST(FirstName AS VARCHAR) as name,
                CAST(CAST(DateOfBirth AS DATE) AS VARCHAR) as birthdate,
                CAST(NULL AS VARCHAR) as cpf,
                CAST(prontuario AS VARCHAR) as prontuario_old,
                CAST(_location AS VARCHAR) as reference,
                'embryoscope' as source
            FROM src.silver_embryoscope.patients
            WHERE PatientID IS NOT NULL
        """)
        rows = target_conn.execute("SELECT COUNT(*) FROM mapped_patients WHERE source = 'embryoscope'").fetchone()[0]
        print(f" -> Ingested {rows} records from Embryoscope.")

        # 5. Ingest from Protheus (Paciente) - Leave birthdate empty
        print("\nIngesting from Protheus (Paciente)...")
        target_conn.execute("""
            INSERT INTO mapped_patients (id, name, birthdate, cpf, prontuario_old, reference, source)
            SELECT DISTINCT
                CAST(Paciente AS VARCHAR) as id,
                CAST("Nom Paciente" AS VARCHAR) as name,
                CAST(NULL AS VARCHAR) as birthdate,
                CAST(CPF AS VARCHAR) as cpf,
                CAST(prontuario AS VARCHAR) as prontuario_old,
                'paciente' as reference,
                'protheus' as source
            FROM src.gold.protheus_mesclada_vendas
            WHERE Paciente IS NOT NULL AND CAST(Paciente AS VARCHAR) != ''
        """)
        rows = target_conn.execute("SELECT COUNT(*) FROM mapped_patients WHERE source = 'protheus' AND reference = 'paciente'").fetchone()[0]
        print(f" -> Ingested {rows} patient records from Protheus.")

        # 6. Ingest from Protheus (Cliente) - Leave birthdate empty
        print("Ingesting from Protheus (Cliente)...")
        target_conn.execute("""
            INSERT INTO mapped_patients (id, name, birthdate, cpf, prontuario_old, reference, source)
            SELECT DISTINCT
                CAST(Cliente AS VARCHAR) as id,
                CAST(Nome AS VARCHAR) as name,
                CAST(NULL AS VARCHAR) as birthdate,
                CAST(CPF AS VARCHAR) as cpf,
                CAST(prontuario AS VARCHAR) as prontuario_old,
                'cliente' as reference,
                'protheus' as source
            FROM src.gold.protheus_mesclada_vendas
            WHERE Cliente IS NOT NULL AND CAST(Cliente AS VARCHAR) != ''
        """)
        rows = target_conn.execute("SELECT COUNT(*) FROM mapped_patients WHERE source = 'protheus' AND reference = 'cliente'").fetchone()[0]
        print(f" -> Ingested {rows} client records from Protheus.")

        # 7. Ingest from Redlara (joining silver.redlara_unified with bronze tables)
        print("\nIngesting from Redlara (joining silver.redlara_unified with bronze tables for birthdate)...")
        redlara_tables = target_conn.execute("""
            SELECT table_name 
            FROM duckdb_tables()
            WHERE database_name = 'src' AND schema_name = 'bronze' AND table_name LIKE 'redlara_%'
        """).fetchall()
        redlara_table_names = [t[0] for t in redlara_tables]

        unidade_map = {
            'ibirapuera': 'Ibirapuera',
            'sj': 'Sj',
            'vm': 'Vm'
        }

        for t_name in sorted(redlara_table_names):
            parts = t_name.split('_')
            unidade_part = parts[1]
            year_part = int(parts[2])
            unidade_silver = unidade_map[unidade_part]
            
            # Describe to find column names
            cols_info = target_conn.execute(f"DESCRIBE src.bronze.{t_name}").fetchall()
            cols = [c[0] for c in cols_info]
            cols_upper = [c.upper() for c in cols]
            
            # Match chart_or_pin
            id_col = None
            for orig_col, upper_col in zip(cols, cols_upper):
                if upper_col in ['CHART_OR_PIN', 'CHART OF PIN', 'CHART OR PIN']:
                    id_col = f'"{orig_col}"'
                    break
            
            # Match birthdate
            birth_col = None
            for orig_col, upper_col in zip(cols, cols_upper):
                if upper_col in ['DATE_OF_BIRTH', 'DATE OF BIRTH', "WOMAN'S DATE OF BIRTH"]:
                    birth_col = f'"{orig_col}"'
                    break
            
            if not id_col or not birth_col:
                print(f"  - Warning: Missing columns in {t_name} (id_col: {id_col}, birth_col: {birth_col}). Skipping.")
                continue

            target_conn.execute(f"""
                INSERT INTO mapped_patients (id, name, birthdate, cpf, prontuario_old, reference, source)
                SELECT DISTINCT
                    CAST(r.chart_or_pin AS VARCHAR) as id,
                    '' as name,
                    CAST(
                        COALESCE(
                            TRY_CAST(replace(replace(b.{birth_col}, '\\', '/'), '//', '/') AS DATE),
                            TRY_CAST(try_strptime(replace(replace(b.{birth_col}, '\\', '/'), '//', '/'), '%d/%m/%Y') AS DATE)
                        ) AS VARCHAR
                    ) as birthdate,
                    CAST(NULL AS VARCHAR) as cpf,
                    CAST(r.prontuario AS VARCHAR) as prontuario_old,
                    CAST(r.unidade AS VARCHAR) as reference,
                    'redlara' as source
                FROM src.silver.redlara_unified r
                JOIN src.bronze.{t_name} b ON r.chart_or_pin = b.{id_col}
                WHERE r.unidade = '{unidade_silver}' AND r.year = {year_part} AND r.chart_or_pin IS NOT NULL
            """)
            print(f"  - Ingested from {t_name:40} (id_col: {id_col}, birth_col: {birth_col})")

        rows = target_conn.execute("SELECT COUNT(*) FROM mapped_patients WHERE source = 'redlara'").fetchone()[0]
        print(f" -> Ingested {rows} records from Redlara.")

        # 8. Ingest from Planilha Embriologia (bronze tables starting with planilha_)
        print("\nIngesting from Planilha Embriologia (bronze.planilha_* tables)...")
        # List all tables in bronze schema starting with planilha_
        tables = target_conn.execute("""
            SELECT table_name 
            FROM duckdb_tables()
            WHERE database_name = 'src' AND schema_name = 'bronze' AND table_name LIKE 'planilha_%'
        """).fetchall()
        table_names = [t[0] for t in tables]

        for table_name in sorted(table_names):
            full_table = f"src.bronze.{table_name}"
            # Check columns to decide name column
            cols = [c[0].upper() for c in target_conn.execute(f"DESCRIBE {full_table}").fetchall()]
            
            # Decide NOME column
            if 'NOME' in cols:
                name_col = '"NOME"'
            elif 'NOME DA PACIENTE' in cols:
                name_col = '"NOME DA PACIENTE"'
            else:
                name_col = 'CAST(NULL AS VARCHAR)'
            
            # Decide birthdate column
            if 'DATA DE NASC.' in cols:
                birth_col = '"DATA DE NASC."'
            elif 'DATA DE NASCIMENTO' in cols:
                birth_col = '"DATA DE NASCIMENTO"'
            else:
                birth_col = 'CAST(NULL AS VARCHAR)'
            
            # Execute insert
            target_conn.execute(f"""
                INSERT INTO mapped_patients (id, name, birthdate, cpf, prontuario_old, reference, source)
                SELECT DISTINCT
                    CAST(PIN AS VARCHAR) as id,
                    CAST({name_col} AS VARCHAR) as name,
                    CAST(
                        COALESCE(
                            TRY_CAST(replace(replace({birth_col}, '\\', '/'), '//', '/') AS DATE),
                            TRY_CAST(try_strptime(replace(replace({birth_col}, '\\', '/'), '//', '/'), '%d/%m/%Y') AS DATE)
                        ) AS VARCHAR
                    ) as birthdate,
                    CAST(NULL AS VARCHAR) as cpf,
                    CAST(NULL AS VARCHAR) as prontuario_old,
                    CAST(sheet_name AS VARCHAR) as reference,
                    'planilha_embriologia' as source
                FROM {full_table}
                WHERE PIN IS NOT NULL AND CAST(PIN AS VARCHAR) != ''
            """)
            print(f"  - Ingested from {table_name:40} (name: {name_col}, dob: {birth_col})")
            
        total_planilha_rows = target_conn.execute("SELECT COUNT(*) FROM mapped_patients WHERE source = 'planilha_embriologia'").fetchone()[0]
        print(f" -> Total ingested {total_planilha_rows} records from Planilha Embriologia.")

        # 9. Print final statistics
        print("\n" + "="*80)
        print("FINAL TABLE STATISTICS (mapped_patients)")
        print("="*80)
        stats = target_conn.execute("""
            SELECT source, reference, COUNT(*) as total_count, COUNT(birthdate) as has_birthdate, COUNT(cpf) as has_cpf
            FROM mapped_patients 
            GROUP BY source, reference 
            ORDER BY source, reference
        """).fetchall()
        for source, ref, total, has_dob, has_cpf in stats:
            pct_dob = (has_dob / total * 100) if total > 0 else 0.0
            pct_cpf = (has_cpf / total * 100) if total > 0 else 0.0
            print(f"Source: {source:20} | Ref: {ref:20} | Total: {total:,} | DOB: {has_dob:,} ({pct_dob:.1f}%) | CPF: {has_cpf:,} ({pct_cpf:.1f}%)")
        
        total_rows = target_conn.execute("SELECT COUNT(*) FROM mapped_patients").fetchone()[0]
        total_birthdates = target_conn.execute("SELECT COUNT(birthdate) FROM mapped_patients").fetchone()[0]
        total_cpfs = target_conn.execute("SELECT COUNT(cpf) FROM mapped_patients").fetchone()[0]
        print(f"\nTotal rows in target table: {total_rows:,}")
        print(f"Total birthdates populated: {total_birthdates:,} ({(total_birthdates/total_rows*100):.1f}%)")
        print(f"Total CPFs populated:       {total_cpfs:,} ({(total_cpfs/total_rows*100):.1f}%)")
        print("="*80)

        # 10. Show a few sample rows from each source
        print("\nSAMPLE ROWS:")
        for src_name in ['embryoscope', 'protheus', 'redlara', 'planilha_embriologia']:
            print(f"\n--- Sample from {src_name} ---")
            samples = target_conn.execute(f"""
                SELECT id, name, birthdate, cpf, prontuario_old, reference 
                FROM mapped_patients 
                WHERE source = '{src_name}' AND (birthdate IS NOT NULL OR cpf IS NOT NULL)
                LIMIT 3
            """).df()
            if samples.empty:
                samples = target_conn.execute(f"""
                    SELECT id, name, birthdate, cpf, prontuario_old, reference 
                    FROM mapped_patients 
                    WHERE source = '{src_name}'
                    LIMIT 3
                """).df()
            print(samples)

    except Exception as e:
        print(f"\nERROR: Ingestion failed! {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        target_conn.close()
        print("\nTarget database connection closed.")

if __name__ == "__main__":
    main()
