import duckdb
import pandas as pd

def generate_report():
    db_path = 'database/huntington_data_lake.duckdb'
    conn = duckdb.connect(db_path)
    
    table_name = 'gold.planilha_embryoscope_combined'
    
    # Using FILTER for cleaner syntax in DuckDB
    query = f"""
    SELECT 
        -- GLOBAL LINE METRICS
        COUNT(*) as total_lines,
        COUNT(outcome_type) as lines_outcome_nn,
        COUNT(fet_gravidez_clinica) as lines_fet_nn,
        COUNT(trat2_resultado_tratamento) as lines_trat2_nn,
        COUNT(*) FILTER (WHERE outcome_type IS NOT NULL OR fet_gravidez_clinica IS NOT NULL OR trat2_resultado_tratamento IS NOT NULL) as lines_any_nn,
        COUNT(*) FILTER (WHERE (outcome_type IS NOT NULL OR fet_gravidez_clinica IS NOT NULL OR trat2_resultado_tratamento IS NOT NULL) AND embryo_EmbryoID IS NOT NULL) as lines_any_eid_nn,
        
        -- GLOBAL OOCITO METRICS
        COUNT(DISTINCT oocito_id) as total_oocitos,
        COUNT(DISTINCT oocito_id) FILTER (WHERE outcome_type IS NOT NULL) as oocitos_outcome_nn,
        COUNT(DISTINCT oocito_id) FILTER (WHERE fet_gravidez_clinica IS NOT NULL) as oocitos_fet_nn,
        COUNT(DISTINCT oocito_id) FILTER (WHERE trat2_resultado_tratamento IS NOT NULL) as oocitos_trat2_nn,
        COUNT(DISTINCT oocito_id) FILTER (WHERE outcome_type IS NOT NULL OR fet_gravidez_clinica IS NOT NULL OR trat2_resultado_tratamento IS NOT NULL) as oocitos_any_nn,
        COUNT(DISTINCT oocito_id) FILTER (WHERE (outcome_type IS NOT NULL OR fet_gravidez_clinica IS NOT NULL OR trat2_resultado_tratamento IS NOT NULL) AND embryo_EmbryoID IS NOT NULL) as oocitos_any_eid_nn,

        -- SUBSET (trat1_id IS NOT NULL) LINE METRICS
        COUNT(*) FILTER (WHERE trat1_id IS NOT NULL) as total_lines_s1,
        COUNT(outcome_type) FILTER (WHERE trat1_id IS NOT NULL) as lines_outcome_nn_s1,
        COUNT(fet_gravidez_clinica) FILTER (WHERE trat1_id IS NOT NULL) as lines_fet_nn_s1,
        COUNT(trat2_resultado_tratamento) FILTER (WHERE trat1_id IS NOT NULL) as lines_trat2_nn_s1,
        COUNT(*) FILTER (WHERE trat1_id IS NOT NULL AND (outcome_type IS NOT NULL OR fet_gravidez_clinica IS NOT NULL OR trat2_resultado_tratamento IS NOT NULL)) as lines_any_nn_s1,
        COUNT(*) FILTER (WHERE trat1_id IS NOT NULL AND (outcome_type IS NOT NULL OR fet_gravidez_clinica IS NOT NULL OR trat2_resultado_tratamento IS NOT NULL) AND embryo_EmbryoID IS NOT NULL) as lines_any_eid_nn_s1,
        
        -- SUBSET (trat1_id IS NOT NULL) OOCITO METRICS
        COUNT(DISTINCT oocito_id) FILTER (WHERE trat1_id IS NOT NULL) as total_oocitos_s1,
        COUNT(DISTINCT oocito_id) FILTER (WHERE trat1_id IS NOT NULL AND outcome_type IS NOT NULL) as oocitos_outcome_nn_s1,
        COUNT(DISTINCT oocito_id) FILTER (WHERE trat1_id IS NOT NULL AND fet_gravidez_clinica IS NOT NULL) as oocitos_fet_nn_s1,
        COUNT(DISTINCT oocito_id) FILTER (WHERE trat1_id IS NOT NULL AND trat2_resultado_tratamento IS NOT NULL) as oocitos_trat2_nn_s1,
        COUNT(DISTINCT oocito_id) FILTER (WHERE trat1_id IS NOT NULL AND (outcome_type IS NOT NULL OR fet_gravidez_clinica IS NOT NULL OR trat2_resultado_tratamento IS NOT NULL)) as oocitos_any_nn_s1,
        COUNT(DISTINCT oocito_id) FILTER (WHERE trat1_id IS NOT NULL AND (outcome_type IS NOT NULL OR fet_gravidez_clinica IS NOT NULL OR trat2_resultado_tratamento IS NOT NULL) AND embryo_EmbryoID IS NOT NULL) as oocitos_any_eid_nn_s1
    FROM {table_name}
    """
    
    try:
        results = conn.execute(query).df().iloc[0]
        
        def print_section(title, total, metrics, total_label):
            print(f"\n{title}:")
            print(f"  * {total_label}: {total:,.0f}")
            for label, count in metrics.items():
                pct = (count / total * 100) if total > 0 else 0
                print(f"  * {label}: {count:,.0f} ({pct:.2f}%)")

        print("-" * 60)
        print("DATA LAKE QUALITY REPORT (EXPANDED)")
        print(f"Table: {table_name}")
        print("-" * 60)

        # Global Lines
        print_section("Global Metrics (Line Counts)", results['total_lines'], {
            "outcome_type not null": results['lines_outcome_nn'],
            "fet_gravidez_clinica not null": results['lines_fet_nn'],
            "trat2_resultado_tratamento not null": results['lines_trat2_nn'],
            "AT LEAST ONE VALID CONDITION": results['lines_any_nn'],
            "VALID CONDITION AND embryo_EmbryoID NOT NULL": results['lines_any_eid_nn']
        }, "Total lines")

        # Global Oocitos
        print_section("Global Metrics (Unique oocito_id)", results['total_oocitos'], {
            "outcome_type not null": results['oocitos_outcome_nn'],
            "fet_gravidez_clinica not null": results['oocitos_fet_nn'],
            "trat2_resultado_tratamento not null": results['oocitos_trat2_nn'],
            "AT LEAST ONE VALID CONDITION": results['oocitos_any_nn'],
            "VALID CONDITION AND embryo_EmbryoID NOT NULL": results['oocitos_any_eid_nn']
        }, "Total unique oocitos")

        print("\n" + "="*30)
        print("SUBSET: trat1_id is NOT NULL")
        print("="*30)

        # Subset Lines
        print_section("Subset Metrics (Line Counts)", results['total_lines_s1'], {
            "outcome_type not null": results['lines_outcome_nn_s1'],
            "fet_gravidez_clinica not null": results['lines_fet_nn_s1'],
            "trat2_resultado_tratamento not null": results['lines_trat2_nn_s1'],
            "AT LEAST ONE VALID CONDITION": results['any_nn_s1'] if 'any_nn_s1' in results else results['lines_any_nn_s1'],
            "VALID CONDITION AND embryo_EmbryoID NOT NULL": results['lines_any_eid_nn_s1']
        }, "Total lines")

        # Subset Oocitos
        print_section("Subset Metrics (Unique oocito_id)", results['total_oocitos_s1'], {
            "outcome_type not null": results['oocitos_outcome_nn_s1'],
            "fet_gravidez_clinica not null": results['oocitos_fet_nn_s1'],
            "trat2_resultado_tratamento not null": results['oocitos_trat2_nn_s1'],
            "AT LEAST ONE VALID CONDITION": results['oocitos_any_nn_s1'],
            "VALID CONDITION AND embryo_EmbryoID NOT NULL": results['oocitos_any_eid_nn_s1']
        }, "Total unique oocitos")



        print("-" * 60)
        
    except Exception as e:
        print(f"Error generating report: {e}")
    finally:
        conn.close()


if __name__ == "__main__":
    generate_report()
