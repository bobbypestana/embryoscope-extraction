import duckdb
from pathlib import Path

# Updated DB Path to 'database' folder
DB_PATH = Path(r"g:\My Drive\projetos_individuais\Huntington\database\huntington_data_lake.duckdb")
TARGET_TABLE = "silver.redlara_unified"

# ==============================================================================
# EXPLICT PER-TABLE CONFIGURATIONS ("BY HAND")
# ==============================================================================
# Use this section to map specific columns for individual tables.
# Mappings format: 'target_column': 'source_column_in_bronze'
# Set to None if column does not exist or needs manual check.
# ==============================================================================

TABLE_CONFIGS = {
    'redlara_ibirapuera_2022': {
        'mapping': {
            'outcome': 'outcome',
            'outcome_type': 'outcome_type',
            'chart_or_pin': 'chart_or_pin',
            'date_when_embryos_were_cryopreserved': 'date_when_embryos_were_cryopreserved',
            'date_of_embryo_transfer': 'date_of_embryo_transfer',
            'date_of_delivery': 'date_of_delivery',
            'gestational_age_at_delivery': 'gestational_age_at_delivery',
            'type_of_delivery': 'type_of_delivery',
            'number_of_newborns': 'number_of_newborns',
            'baby_1_weight': 'baby1_weight',
            'baby_2_weight': 'baby2_weight',
            'baby_3_weight': 'baby3_weight',
            'complications_of_pregnancy_specify': 'complications_of_pregnancy_specify',
            # Metadata keys (usually present)
            'year': 'year',
            'unidade': 'unidade',
            'prontuario': None
        }
    },
    'redlara_ibirapuera_2023': {
        'mapping': {
            'outcome': 'Outcome',
            'outcome_type': 'Outcome Type',
            'chart_or_pin': 'Chart or pin',
            'date_when_embryos_were_cryopreserved': 'Date when embryos were cryopreserved',
            'date_of_embryo_transfer': 'Date of embryo transfer',
            'date_of_delivery': 'Date of delivery',
            'gestational_age_at_delivery': 'Gestational age at delivery',
            'type_of_delivery': 'Type of delivery',
            'number_of_newborns': 'Number of newborns',
            'baby_1_weight': 'Baby 1 weight',
            'baby_2_weight': 'Baby 2 - weight',
            'baby_3_weight': 'Baby 3 - Weight',
            'complications_of_pregnancy_specify': 'Complications of pregnancy specify',
            'year': 'year',
            'unidade': 'unidade',
            'prontuario': None
        }
    },
    'redlara_ibirapuera_2024': {
        'mapping': {
            'outcome': 'Outcome',
            'outcome_type': 'Outcome Type',
            'chart_or_pin': 'Chart or pin',
            'date_when_embryos_were_cryopreserved': 'Date when embryos were cryopreserved',
            'date_of_embryo_transfer': 'Date of embryo transfer',
            'date_of_delivery': 'Date of delivery',
            'gestational_age_at_delivery': 'Gestational age at delivery',
            'type_of_delivery': 'Type of delivery',
            'number_of_newborns': 'Number of newborns',
            'baby_1_weight': 'Baby 1 weight',
            'baby_2_weight': 'Baby 2 - weight',
            'baby_3_weight': 'Baby 3 - Weight',
            'complications_of_pregnancy_specify': 'Complications of pregnancy specify',
            'year': 'year',
            'unidade': 'unidade',
            'prontuario': None
        }
    },
    'redlara_sj_2022': {
        'mapping': {
            'outcome': 'Outcome',
            'outcome_type': 'Outcome Type',
            'chart_or_pin': 'Chart of PIN',
            'date_when_embryos_were_cryopreserved': 'Date when embryos were cryopreserved',
            'date_of_embryo_transfer': 'Date of embryo transfer',
            'date_of_delivery': 'Date of delivery',
            'gestational_age_at_delivery': 'Getacional Age at Delivery',
            'type_of_delivery': 'Type of delivery',
            'number_of_newborns': 'Number of newborns',
            'baby_1_weight': 'Weight.1',
            'baby_2_weight': 'Weight.2',
            'baby_3_weight': 'Weight.3',
            'complications_of_pregnancy_specify': 'Complications (Other \ Specify)',
            'year': 'year',
            'unidade': 'unidade',
            'prontuario': None
        }
    },
    'redlara_sj_2023': {
        'mapping': {
            'outcome': 'Outcome',
            'outcome_type': 'Outcome Type',
            'chart_or_pin': 'Chart of PIN',
            'date_when_embryos_were_cryopreserved': 'Date when embryos were cryopreserved',
            'date_of_embryo_transfer': 'Date of embryo transfer',
            'date_of_delivery': 'Date of delivery',
            'gestational_age_at_delivery': 'Getacional Age at Delivery',
            'type_of_delivery': 'Type of delivery',
            'number_of_newborns': 'Number of newborns',
            'baby_1_weight': 'Weight.1',
            'baby_2_weight': 'Weight.2',
            'baby_3_weight': 'Weight.3',
            'complications_of_pregnancy_specify': 'Complications (Other \ Specify)',
            'year': 'year',
            'unidade': 'unidade',
            'prontuario': None
        }
    },
    'redlara_sj_2024': {
        'mapping': {
            'outcome': 'Outcome',
            'outcome_type': 'Outcome TYPE',
            'chart_or_pin': 'Chart or pin',
            'date_when_embryos_were_cryopreserved': 'Date when embryos were cryopreserved (Arrumar)',
            'date_of_embryo_transfer': 'Date of embryo transfer (VER QUEM TRANSFERIU )',
            'date_of_delivery': 'Date of delivery',
            'gestational_age_at_delivery': 'Gestacional age at delivery',
            'type_of_delivery': 'Type of delivery',
            'number_of_newborns': 'Number of newborns',
            'baby_1_weight': 'Baby 1 - Weight',
            'baby_2_weight': 'Baby 2 - Weight',
            'baby_3_weight': 'Baby 3 - Weight',
            'complications_of_pregnancy_specify': 'Complications of pregnancy specify',
            'year': 'year',
            'unidade': 'unidade',
            'prontuario': None
        }
    },
    'redlara_vm_2022': {
        'mapping': {
            'outcome': 'Outcome',
            'outcome_type': 'Outcome Type',
            'chart_or_pin': 'Chart or pin',
            'date_when_embryos_were_cryopreserved': 'Date when embryos were cryopreserved (Arrumar)',
            'date_of_embryo_transfer': 'Date of embryo transfer (VER QUEM TRANSFERIU )',
            'date_of_delivery': 'Date of delivery',
            'gestational_age_at_delivery': 'Gestational age at delivery',
            'type_of_delivery': 'Type of delivery',
            'number_of_newborns': 'Number of newborns',
            'baby_1_weight': 'Baby 1 - Weight',
            'baby_2_weight': 'Baby 2 - Weight',
            'baby_3_weight': 'Baby 3 - Weight',
            'complications_of_pregnancy_specify': 'Complications of pregnancy specify',
            'year': 'year',
            'unidade': 'unidade',
            'prontuario': None
        }
    },
    'redlara_vm_2023': {
        'mapping': {
            'outcome': 'Outcome',
            'outcome_type': 'Outcome Type',
            'chart_or_pin': 'Chart or pin',
            'date_when_embryos_were_cryopreserved': 'Date when embryos were cryopreserved (Arrumar)',
            'date_of_embryo_transfer': 'Date of embryo transfer (VER QUEM TRANSFERIU )',
            'date_of_delivery': 'Date of delivery',
            'gestational_age_at_delivery': 'Gestacional age at delivery',
            'type_of_delivery': 'Type of delivery',
            'number_of_newborns': 'Number of newborns',
            'baby_1_weight': 'Baby 1 - Weight',
            'baby_2_weight': 'Baby 2 - Weight',
            'baby_3_weight': 'Baby 3 - Weight',
            'complications_of_pregnancy_specify': 'Complications of pregnancy specify',
            'year': 'year',
            'unidade': 'unidade',
            'prontuario': None
        }
    },
    'redlara_vm_2024': {
        'mapping': {
            'outcome': 'Outcome',
            'outcome_type': 'Outcome TYPE',
            'chart_or_pin': 'Chart or pin',
            'date_when_embryos_were_cryopreserved': 'Date when embryos were cryopreserved (Arrumar)',
            'date_of_embryo_transfer': 'Date of embryo transfer (VER QUEM TRANSFERIU )',
            'date_of_delivery': 'Date of delivery',
            'gestational_age_at_delivery': 'Gestacional age at delivery',
            'type_of_delivery': 'Type of delivery',
            'number_of_newborns': 'Number of newborns',
            'baby_1_weight': 'Baby 1 - Weight',
            'baby_2_weight': 'Baby 2 - Weight',
            'baby_3_weight': 'Baby 3 - Weight',
            'complications_of_pregnancy_specify': 'Complications of pregnancy specify',
            'year': 'year',
            'unidade': 'unidade',
            'prontuario': None
        }
    }
}

def unify_tables():
    conn = duckdb.connect(str(DB_PATH))
    conn.execute("CREATE SCHEMA IF NOT EXISTS silver")
    
    union_parts = []
    
    print(f"Propagating tables to Silver based on TABLE_CONFIGS...")
    
    for table_name, config in TABLE_CONFIGS.items():
        print(f"  Processing {table_name}...")
        
        # Check if table exists
        exists = conn.execute(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='bronze' AND table_name='{table_name}'").fetchone()[0]
        if not exists:
            print(f"  Warning: Table {table_name} defined in config but not found in DB. Skipping.")
            continue
            
        mapping = config.get('mapping', {})
        select_clause = []
        
        for target_col, source_col in mapping.items():
            if target_col == 'prontuario':
                target_type = 'INTEGER'
            elif 'date' in target_col or target_col == 'date_of_delivery':
                target_type = 'DATE'
            elif target_col == 'year':
                target_type = 'INTEGER'
            else:
                target_type = 'VARCHAR'
                
            if source_col:
                # Quote source column to handle spaces etc.
                select_clause.append(f'TRY_CAST("{source_col}" AS {target_type}) AS {target_col}')
            else:
                select_clause.append(f"CAST(NULL AS {target_type}) AS {target_col}")
        
        if not select_clause:
            print("  Warning: No columns mapped.")
            continue
            
        query = f"SELECT {', '.join(select_clause)} FROM bronze.{table_name}"
        union_parts.append(query)
    
    if union_parts:
        full_query = " UNION ALL ".join(union_parts)
        ddl = f"CREATE OR REPLACE TABLE {TARGET_TABLE} AS {full_query}"
        
        print(f"Creating {TARGET_TABLE}...")
        try:
            conn.execute(ddl)
            count = conn.execute(f"SELECT COUNT(*) FROM {TARGET_TABLE}").fetchone()[0]
            print(f"Successfully created {TARGET_TABLE} with {count} rows.")
            
            # --- Prontuario Matching Logic ---
            print("Matching chart_or_pin with view_pacientes to populate prontuario...")
            clinisys_db_path = DB_PATH.parent / "clinisys_all.duckdb"
            if not clinisys_db_path.exists():
                print(f"  Warning: {clinisys_db_path} not found. Skipping prontuario matching.")
            else:
                conn.execute(f"ATTACH '{clinisys_db_path}' AS clinisys_all (READ_ONLY)")
                
                # Logic adapted from planilha_embriologia pipeline
                update_sql = f"""
                WITH pin_matches AS (
                    SELECT DISTINCT
                        p.chart_or_pin,
                        COALESCE(
                            -- Try direct match with codigo (main prontuario)
                            (SELECT codigo FROM clinisys_all.silver.view_pacientes v 
                             WHERE TRY_CAST(p.chart_or_pin AS INTEGER) = v.codigo AND v.inativo = 0 LIMIT 1),
                            -- Try match with prontuario_esposa
                            (SELECT codigo FROM clinisys_all.silver.view_pacientes v 
                             WHERE TRY_CAST(p.chart_or_pin AS INTEGER) = v.prontuario_esposa AND v.inativo = 0 LIMIT 1),
                            -- Try match with prontuario_marido
                            (SELECT codigo FROM clinisys_all.silver.view_pacientes v 
                             WHERE TRY_CAST(p.chart_or_pin AS INTEGER) = v.prontuario_marido AND v.inativo = 0 LIMIT 1),
                            -- Try match with prontuario_responsavel1
                            (SELECT codigo FROM clinisys_all.silver.view_pacientes v 
                             WHERE TRY_CAST(p.chart_or_pin AS INTEGER) = v.prontuario_responsavel1 AND v.inativo = 0 LIMIT 1),
                            -- Try match with prontuario_responsavel2
                            (SELECT codigo FROM clinisys_all.silver.view_pacientes v 
                             WHERE TRY_CAST(p.chart_or_pin AS INTEGER) = v.prontuario_responsavel2 AND v.inativo = 0 LIMIT 1)
                        ) as matched_prontuario
                    FROM {TARGET_TABLE} p
                    WHERE p.chart_or_pin IS NOT NULL
                )
                UPDATE {TARGET_TABLE}
                SET prontuario = m.matched_prontuario
                FROM pin_matches m
                WHERE {TARGET_TABLE}.chart_or_pin = m.chart_or_pin
                  AND m.matched_prontuario IS NOT NULL
                """
                conn.execute(update_sql)
                
                stats = conn.execute(f"""
                    SELECT 
                        COUNT(*) as total,
                        COUNT(CASE WHEN prontuario IS NOT NULL THEN 1 END) as matched
                    FROM {TARGET_TABLE}
                    WHERE chart_or_pin IS NOT NULL
                """).fetchone()
                
                print(f"  Matched {stats[1]} out of {stats[0]} rows with PIN/Chart ({stats[1]/stats[0]*100 if stats[0]>0 else 0:.1f}%)")
                conn.execute("DETACH clinisys_all")

        except Exception as e:
            print(f"Error creating/matching table: {e}")
    else:
        print("Nothing to union.")
        
    conn.close()

if __name__ == "__main__":
    unify_tables()
