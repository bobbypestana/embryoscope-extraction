import os
import sys
import re
import logging
import duckdb
import pandas as pd
import numpy as np
from pyathena import connect
from pyathena.pandas.cursor import PandasCursor

# Add current directory to path to import hooks
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from validation_hooks import scrub_pii, archive_report

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("gold_athena_comparison")

DB_PATH = "database/huntington_data_lake.duckdb"
ATHENA_REGION = "sa-east-1"
ATHENA_WORKGROUP = "datalake-admins"
ATHENA_DB = "gold_huntington_staging"

# Tables to compare config
TABLES_CONFIG = {
    "patient_info": {
        "keys": ["prontuario"],
        "value_cols": [],
        "date_col": None,
        "group_col": "unidade_nome"
    },
    "finops_summary": {
        "keys": ["prontuario"],
        "value_cols": ["cycle_with_transfer", "cycle_without_transfer", "cycle_total", "treatment_paid_count", "treatment_paid_total"],
        "date_col": "timeline_first_date",
        "group_col": "timeline_unidade"
    },
    "biopsy_pgta_timeline": {
        "keys": ["prontuario", "period_month"],
        "value_cols": ["biopsy_count", "pgta_test_count", "cumulative_biopsy_count", "cumulative_pgta_test_count"],
        "date_col": "period_month",
        "group_col": None
    },
    "consultas_timeline": {
        "keys": ["prontuario", "period_month"],
        "value_cols": ["consultas_events_count", "billing_events_count", "total_billing_amount", "total_quantity"],
        "date_col": "period_month",
        "group_col": None
    },
    "cryopreservation_events_timeline": {
        "keys": ["prontuario", "period_month"],
        "value_cols": ["freezing_events_count", "billing_events_count", "total_billing_amount"],
        "date_col": "period_month",
        "group_col": None
    },
    "embryo_freeze_timeline": {
        "keys": ["prontuario", "period_month"],
        "value_cols": ["embryos_frozen_count", "embryos_unfrozen_count", "embryos_discarded_count", "embryos_storage_balance"],
        "date_col": "period_month",
        "group_col": None
    },
    "embryoscope_timeline": {
        "keys": ["prontuario", "period_month"],
        "value_cols": ["embryoscope_usage_count", "billing_events_count", "total_billing_amount"],
        "date_col": "period_month",
        "group_col": None
    },
    "all_patients_timeline": {
        "keys": ["prontuario", "event_id", "event_date", "reference"],
        "value_cols": [],
        "date_col": "event_date",
        "group_col": "unidade"
    },
    "recent_patients_timeline": {
        "keys": ["prontuario", "event_id", "event_date", "reference"],
        "value_cols": [],
        "date_col": "event_date",
        "group_col": "unidade"
    },
    "embryoscope_embrioes": {
        "keys": ["embryo_embryoid"],
        "value_cols": [],
        "date_col": "embryo_embryodate",
        "group_col": "patient_unit_huntington"
    },
    "clinisys_embrioes": {
        "keys": ["oocito_id", "emb_cong_id"],
        "value_cols": [],
        "date_col": "micro_data_procedimento",
        "group_col": "nome_medico"
    },
    "embryos_with_prescription_long": {
        "keys": ["oocito_id", "presc_id"],
        "value_cols": ["presc_dose", "presc_dose_total", "presc_numero_dias"],
        "date_col": "presc_data_inicial",
        "group_col": "presc_grupo_medicamento"
    },
    "embryos_with_prescription_wide": {
        "keys": ["oocito_id"],
        "value_cols": [],
        "date_col": "micro_data_procedimento",
        "group_col": "nome_medico"
    },
    "embryoscope_clinisys_combined": {
        "keys": ["oocito_id", "emb_cong_id", "embryo_embryoid"],
        "value_cols": [],
        "date_col": "micro_data_procedimento",
        "group_col": "nome_medico"
    },
    "planilha_embryoscope_combined": {
        "keys": ["oocito_id", "emb_cong_id", "embryo_embryoid"],
        "value_cols": [],
        "date_col": "micro_data_procedimento",
        "group_col": "nome_medico"
    },
    "redlara_planilha_combined": {
        "keys": ["prontuario", "transfer_date", "chart_or_pin", "incubadora_padronizada"],
        "value_cols": ["merged_numero_de_nascidos"],
        "date_col": "transfer_date",
        "group_col": "incubadora_padronizada"
    },
    "protheus_mesclada_vendas": {
        "keys": ["loja", "numero", "serie_docto", "produto", "cliente"],
        "value_cols": ["qntd", "total"],
        "date_col": "data_emissao",
        "group_col": "loja"
    }
}

IGNORE_COLS = {
    "extraction_timestamp", "bronze_updated_at", "prontuario_match_tier", "table_order", 
    "redlara_planilha_join_step", "flag_date_estimated", "additional_info"
}

CUSTOM_MAPPINGS = {
    'cliente': 'cliente_codms',
    'paciente': 'paciente_codms',
    'nom paciente': 'nome_paciente',
    'dt emissao': 'data_emissao',
    'descricao': 'produto_descricao',
    'qntd.': 'qntd',
    'descrio gerencial': 'descricao_gerencial',
    'descriçao gerencial': 'descricao_gerencial',
    'tipo da nota': 'tipo_nota',
    'numero': 'numero_nota',
    'serie docto.': 'serie',
    'vend. 1': 'vendedor_codigo',
    'custo unit': 'custo_unit',
    'cta-ctbl': 'cta_ctbl',
    'cta-ctbl eugin': 'cta_ctbl_eugin',
    'fez ciclo?': 'fez_ciclo',
    'data do ciclo': 'data_do_ciclo',
    'patient_patientid': 'patient_id',
    'patient_patientidx': 'patient_id_x',
    'patient_firstname': 'patient_first_name',
    'patient_lastname': 'patient_last_name',
    'patient_dateofbirth': 'patient_date_of_birth',
    'patient_yearofbirth': 'patient_year_of_birth',
    'treatment_treatmentname': 'treatment_name',
    'embryo_embryoid': 'embryo_embryo_id',
    'embryo_embryodate': 'embryo_embryo_date',
    'embryo_wellnumber': 'embryo_well_number',
    'embryo_fertilizationtime': 'embryo_fertilization_time',
    'embryo_description': 'embryo_description',
    'embryo_embryofate': 'embryo_embryo_fate',
    'embryo_position': 'embryo_position',
    'embryo_instrumentnumber': 'embryo_instrument_number',
    'embryo_embryodescriptionid': 'embryo_embryo_description_id',
    'embryo_kidscore': 'embryo_kid_score',
    'embryo_kiddate': 'embryo_kid_date',
    'embryo_kidversion': 'embryo_kid_version',
    'embryo_kiduser': 'embryo_kid_user',
    'idascore_idascore': 'idascore_idascore',
    'idascore_idatime': 'idascore_idatime',
    'idascore_idaversion': 'idascore_idaversion',
    'chart_or_pin': 'redlara_chart_or_pin',
    'outcome': 'redlara_outcome'
}

def normalize_col(name):
    # 1. Lowercase
    s = name.lower()
    
    # 2. Translate accents/diacritics manually
    diacritics = {
        'á': 'a', 'à': 'a', 'â': 'a', 'ã': 'a', 'ä': 'a',
        'é': 'e', 'è': 'e', 'ê': 'e', 'ë': 'e',
        'í': 'i', 'ì': 'i', 'î': 'i', 'ï': 'i',
        'ó': 'o', 'ò': 'o', 'ô': 'o', 'õ': 'o', 'ö': 'o',
        'ú': 'u', 'ù': 'u', 'û': 'u', 'ü': 'u',
        'ç': 'c',
    }
    for char, repl in diacritics.items():
        s = s.replace(char, repl)
        
    # Remove any other non-ascii-alphanumeric chars or special signs
    s = re.sub(r'[^a-z0-9_]', ' ', s)
    s = re.sub(r'\s+', '_', s)
    s = s.strip('_')
    s = re.sub(r'_+', '_', s)
    
    # Common abbreviation/encoding overrides
    if s == "mdico":
        s = "medico"
    if s == "descrio":
        s = "descricao"
    if s == "descrio_gerencial":
        s = "descricao_gerencial"
    if s == "descrio_mapping_actividad":
        s = "descricao_mapping_actividad"
        
    return s

def clean_series(s):
    # Convert series to object first to safely handle fillna
    s_obj = s.astype(object).fillna("null")
    # Convert to string, strip, and lowercase
    s_str = s_obj.astype(str).str.strip().str.lower()
    
    # Map common null representations to "null"
    s_str = s_str.replace({"": "null", "none": "null", "nan": "null", "<na>": "null", "nat": "null"})
    
    # Fast float normalization:
    has_dot = s_str.str.contains('.', regex=False)
    if has_dot.any():
        def clean_float_str(val_str):
            try:
                f = float(val_str)
                if f.is_integer():
                    return str(int(f))
                return f"{f:.4f}"
            except ValueError:
                return val_str
        clean_mask = has_dot & (s_str != "null")
        unique_vals = s_str[clean_mask].unique()
        val_map = {}
        for uv in unique_vals:
            val_map[uv] = clean_float_str(uv)
        s_str[clean_mask] = s_str[clean_mask].map(val_map)
        
    return s_str

def build_renaming_dict(local_cols, ath_cols_set):
    # ath_cols_set contains mixed-case original Athena columns
    ath_normalized_map = {}
    for ac in ath_cols_set:
        norm_ac = normalize_col(ac)
        ath_normalized_map[norm_ac] = ac
        
    mapping = {}
    for c in local_cols:
        norm_c = normalize_col(c)
        c_low = c.lower()
        
        # 1. Check exact normalized match
        if norm_c in ath_normalized_map:
            mapping[c] = ath_normalized_map[norm_c]
            continue
            
        # 2. Check exact case-insensitive match
        if c_low in ath_cols_set:
            mapping[c] = c_low
            continue
            
        # 3. Check custom mappings (normalized or case-insensitive)
        mapped_target = None
        if norm_c in CUSTOM_MAPPINGS:
            mapped_target = CUSTOM_MAPPINGS[norm_c]
        elif c_low in CUSTOM_MAPPINGS:
            mapped_target = CUSTOM_MAPPINGS[c_low]
            
        if mapped_target:
            norm_target = normalize_col(mapped_target)
            if norm_target in ath_normalized_map:
                mapping[c] = ath_normalized_map[norm_target]
                continue
            elif mapped_target in ath_cols_set:
                mapping[c] = mapped_target
                continue
            elif mapped_target.lower() in ath_cols_set:
                mapping[c] = mapped_target.lower()
                continue
                
        # 4. Convert CamelCase to snake_case
        s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', c)
        s2 = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()
        s2 = s2.replace('__', '_')
        norm_s2 = normalize_col(s2)
        
        if norm_s2 in ath_normalized_map:
            mapping[c] = ath_normalized_map[norm_s2]
            continue
            
        # 5. Check prefix deduplication
        s3 = s2
        for prefix in ['patient_', 'treatment_', 'embryo_', 'idascore_']:
            if s3.startswith(prefix) and s3[len(prefix):].startswith(prefix):
                s3 = prefix + s3[len(prefix)*2:]
        norm_s3 = normalize_col(s3)
        if norm_s3 in ath_normalized_map:
            mapping[c] = ath_normalized_map[norm_s3]
            continue
            
    return mapping

def run_comparison():
    logger.info("Connecting to databases...")
    local_conn = duckdb.connect(DB_PATH, read_only=True)
    ath_conn = connect(region_name=ATHENA_REGION, work_group=ATHENA_WORKGROUP, cursor_class=PandasCursor)
    ath_cur = ath_conn.cursor()

    comparison_results = {}

    for t_name, cfg in TABLES_CONFIG.items():
        logger.info(f"Comparing table: {t_name}")
        keys = cfg["keys"]
        value_cols = cfg["value_cols"]
        date_col = cfg["date_col"]
        group_col = cfg["group_col"]

        try:
            # 1. Fetch column names from both databases
            local_cols = [desc[0] for desc in local_conn.execute(f"SELECT * FROM gold.{t_name} LIMIT 0").description]
            
            # Temporary cursor to inspect columns
            ath_cur.execute(f"SELECT * FROM {ATHENA_DB}.{t_name} LIMIT 0")
            ath_cols_set = {desc[0] for desc in ath_cur.description}

            # Map DuckDB columns to Athena columns
            mapping = build_renaming_dict(local_cols, ath_cols_set)
            local_cols_lower_map = {c.lower(): c for c in local_cols}
            
            # Map configuration columns (keys, date, group) using normalized match
            keys_ath = []
            for k in keys:
                norm_k = normalize_col(k)
                k_orig = None
                for c in local_cols:
                    if normalize_col(c) == norm_k:
                        k_orig = c
                        break
                if k_orig is None:
                    k_orig = local_cols_lower_map.get(k.lower(), k)
                keys_ath.append(mapping.get(k_orig, k))
            
            date_col_ath = None
            if date_col:
                norm_d = normalize_col(date_col)
                date_orig = None
                for c in local_cols:
                    if normalize_col(c) == norm_d:
                        date_orig = c
                        break
                if date_orig is None:
                    date_orig = local_cols_lower_map.get(date_col.lower(), date_col)
                date_col_ath = mapping.get(date_orig, date_col)
                
            group_col_ath = None
            if group_col:
                norm_g = normalize_col(group_col)
                group_orig = None
                for c in local_cols:
                    if normalize_col(c) == norm_g:
                        group_orig = c
                        break
                if group_orig is None:
                    group_orig = local_cols_lower_map.get(group_col.lower(), group_col)
                group_col_ath = mapping.get(group_orig, group_col)

            value_cols_ath = []
            for vc in value_cols:
                norm_vc = normalize_col(vc)
                vc_orig = None
                for c in local_cols:
                    if normalize_col(c) == norm_vc:
                        vc_orig = c
                        break
                if vc_orig is None:
                    vc_orig = local_cols_lower_map.get(vc.lower(), vc)
                value_cols_ath.append(mapping.get(vc_orig, vc))

            # Construct selects
            mapped_cols_dict = {c: mapping[c] for c in local_cols if c in mapping}
            common_cols_ath = sorted(list(set(mapped_cols_dict.values()) - IGNORE_COLS))
            common_cols_local = [c for c in local_cols if c in mapped_cols_dict and mapped_cols_dict[c] in common_cols_ath]

            local_select_sql = ", ".join(f'"{c}"' for c in common_cols_local)
            ath_select_sql = ", ".join(f'"{c}"' for c in common_cols_ath)

            # Fetch Local Data
            df_local = local_conn.execute(f"SELECT {local_select_sql} FROM gold.{t_name}").df()
            # Rename columns to match Athena
            df_local = df_local.rename(columns=mapped_cols_dict)

            # Fetch Athena Data using PandasCursor
            df_ath = ath_cur.execute(f"SELECT {ath_select_sql} FROM {ATHENA_DB}.{t_name}").as_pandas()

            # Deduplicate DataFrames on keys
            # To handle NaNs in keys, fill with standard sentinel values
            df_local_clean = df_local.copy()
            df_ath_clean = df_ath.copy()
            
            for k in keys_ath:
                df_local_clean[k] = df_local_clean[k].astype(object).fillna("null").astype(str).str.strip().str.lower()
                df_ath_clean[k] = df_ath_clean[k].astype(object).fillna("null").astype(str).str.strip().str.lower()

            # Drop duplicates based on mapped Athena keys
            df_local_dedup = df_local_clean.drop_duplicates(subset=keys_ath)
            df_ath_dedup = df_ath_clean.drop_duplicates(subset=keys_ath)

            # 2. Key hashes
            df_local_dedup["_comp_key"] = df_local_dedup[keys_ath].agg("||".join, axis=1)
            df_ath_dedup["_comp_key"] = df_ath_dedup[keys_ath].agg("||".join, axis=1)

            local_keys_set = set(df_local_dedup["_comp_key"])
            ath_keys_set = set(df_ath_dedup["_comp_key"])

            overlap_keys = local_keys_set.intersection(ath_keys_set)
            local_only_keys = local_keys_set - ath_keys_set
            ath_only_keys = ath_keys_set - local_only_keys

            overlap_count = len(overlap_keys)
            local_only_count = len(local_only_keys)
            ath_only_count = len(ath_only_keys)

            # 3. Compare overlapping row values
            df_local_overlap = df_local_dedup[df_local_dedup["_comp_key"].isin(overlap_keys)].set_index("_comp_key")
            df_ath_overlap = df_ath_dedup[df_ath_dedup["_comp_key"].isin(overlap_keys)].set_index("_comp_key")

            # Reindex to ensure identical alignment
            df_ath_overlap = df_ath_overlap.reindex(df_local_overlap.index)

            # Columns comparison
            discrepancies = []
            mismatch_counts = {}
            mismatches_by_column = {}

            compare_cols = [c for c in common_cols_ath if c not in keys_ath]

            # Track mismatch details for reporting
            mismatch_rows = []

            for col in compare_cols:
                col_local_clean = clean_series(df_local_overlap[col])
                col_ath_clean = clean_series(df_ath_overlap[col])

                mismatches = col_local_clean != col_ath_clean
                mismatch_count = mismatches.sum()

                if mismatch_count > 0:
                    mismatch_counts[col] = mismatch_count
                    # Save samples of mismatches
                    mismatch_idx = mismatches[mismatches].index
                    mismatches_by_column[col] = []
                    for idx in mismatch_idx[:10]:
                        key_vals = dict(zip(keys_ath, df_local_overlap.loc[idx, keys_ath]))
                        mismatches_by_column[col].append({
                            "key": key_vals,
                            "local": df_local_overlap.loc[idx, col],
                            "ath": df_ath_overlap.loc[idx, col]
                        })
                    
                    # Record for grouping analysis
                    for idx in mismatch_idx:
                        date_val = "N/A"
                        if date_col_ath and date_col_ath in df_local_overlap.columns:
                            dt = df_local_overlap.loc[idx, date_col_ath]
                            if dt != "null" and not pd.isna(dt):
                                try:
                                    date_val = str(pd.to_datetime(dt).year)
                                except:
                                    date_val = str(dt)[:4]
                        
                        group_val = "N/A"
                        if group_col_ath and group_col_ath in df_local_overlap.columns:
                            gv = df_local_overlap.loc[idx, group_col_ath]
                            if gv != "null" and not pd.isna(gv):
                                group_val = str(gv)

                        mismatch_rows.append({
                            "key": idx,
                            "column": col,
                            "date": date_val,
                            "group": group_val,
                            "local": df_local_overlap.loc[idx, col],
                            "ath": df_ath_overlap.loc[idx, col]
                        })

            # Value alignments (Financial / Numeric totals)
            val_alignments = []
            for idx, vc_ath in enumerate(value_cols_ath):
                vc_loc = value_cols[idx]
                if vc_ath in df_local_overlap.columns and vc_ath in df_ath_overlap.columns:
                    sum_local = pd.to_numeric(df_local_overlap[vc_ath], errors='coerce').sum()
                    sum_ath = pd.to_numeric(df_ath_overlap[vc_ath], errors='coerce').sum()
                    abs_diff = abs(sum_local - sum_ath)
                    rate = (1.0 - (abs_diff / sum_ath)) * 100.0 if sum_ath > 0 else 100.0
                    val_alignments.append({
                        "column": vc_loc,
                        "sum_local": sum_local,
                        "sum_ath": sum_ath,
                        "diff": sum_local - sum_ath,
                        "alignment_rate": rate
                    })

            # Group discrepancies by date/group
            discrepancies_by_date = {}
            discrepancies_by_group = {}
            if mismatch_rows:
                df_mismatches = pd.DataFrame(mismatch_rows)
                discrepancies_by_date = df_mismatches.groupby("date")["key"].nunique().to_dict()
                discrepancies_by_group = df_mismatches.groupby("group")["key"].nunique().to_dict()

            comparison_results[t_name] = {
                "local_count": len(df_local),
                "ath_count": len(df_ath),
                "overlap_count": overlap_count,
                "local_only_count": local_only_count,
                "ath_only_count": ath_only_count,
                "key_match_rate_local": (overlap_count / len(local_keys_set)) * 100.0 if local_keys_set else 100.0,
                "key_match_rate_ath": (overlap_count / len(ath_keys_set)) * 100.0 if ath_keys_set else 100.0,
                "mismatch_counts": mismatch_counts,
                "mismatches_by_column": mismatches_by_column,
                "val_alignments": val_alignments,
                "discrepancies_by_date": discrepancies_by_date,
                "discrepancies_by_group": discrepancies_by_group,
                "schema_diff": {
                    "only_local": sorted(list(set(local_cols) - set(mapping.keys()))),
                    "only_ath": sorted(list(ath_cols_set - set(mapping.values())))
                }
            }

        except Exception as e:
            logger.error(f"Failed to compare {t_name}: {e}", exc_info=True)
            comparison_results[t_name] = {"error": str(e)}

    local_conn.close()
    ath_conn.close()

    # Generate Markdown Report
    logger.info("Compiling markdown report...")
    report_md = """# Data Lake Reconciliation Report: DuckDB Gold vs. AWS Athena Gold

> [!NOTE]
> ### 📊 Global Reconciliation Dashboard
"""
    
    # Global metrics
    total_tables = len(TABLES_CONFIG)
    perfect_match_tables = 0
    warning_tables = 0
    error_tables = 0

    for t_name, res in comparison_results.items():
        if "error" in res:
            error_tables += 1
        elif (res["local_count"] == res["ath_count"]) and (res["local_only_count"] == 0) and (res["ath_only_count"] == 0) and not res["mismatch_counts"]:
            perfect_match_tables += 1
        else:
            warning_tables += 1

    report_md += f"""> * **Total Tables Audited**: **{total_tables}**
> * **Perfect Value Alignment**: **{perfect_match_tables}** tables
> * **Variance / Discrepancy Detected**: **{warning_tables}** tables
> * **Audit Execution Failures**: **{error_tables}** tables
"""

    report_md += "\nThis report presents a detailed reconciliation audit between the local DuckDB database (`gold` schema) and the AWS Athena database (`gold_huntington_staging`).\n\n---\n\n## 1. Table Volume and Key Alignment Summary\n\n"
    report_md += "| Table Name | Columns Missing (Exclude Metadata) | DuckDB Count | Athena Count | Overlap | DuckDB Only | Athena Only | DuckDB Match Rate | Athena Match Rate | Status |\n"
    report_md += "| :--- | :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :--- |\n"

    for t_name, res in comparison_results.items():
        if "error" in res:
            report_md += f"| `{t_name}` | - | *Error* | *Error* | - | - | - | - | - | ❌ FAILED |\n"
        else:
            # Filter out ignored metadata columns
            only_local_clean = sorted(list(set(res["schema_diff"]["only_local"]) - IGNORE_COLS))
            only_ath_clean = sorted(list(set(res["schema_diff"]["only_ath"]) - IGNORE_COLS))
            
            missing_cols_list = []
            if only_local_clean:
                missing_cols_list.append(f"DuckDB only: {', '.join(only_local_clean)}")
            if only_ath_clean:
                missing_cols_list.append(f"Athena only: {', '.join(only_ath_clean)}")
                
            missing_cols_str = "; ".join(missing_cols_list) if missing_cols_list else "Aligned"
            
            has_mismatch = len(res["mismatch_counts"]) > 0 or res["local_only_count"] > 0 or res["ath_only_count"] > 0
            status = "✅ PERFECT" if not has_mismatch else "⚠️ VARIANCE"
            report_md += f"| `{t_name}` | {missing_cols_str} | {res['local_count']:,} | {res['ath_count']:,} | {res['overlap_count']:,} | {res['local_only_count']:,} | {res['ath_only_count']:,} | {res['key_match_rate_local']:.2f}% | {res['key_match_rate_ath']:.2f}% | {status} |\n"

    report_md += "\n---\n\n## 2. Table-by-Table Mismatch Diagnostics & Group Isolation\n\n"

    for t_name, res in comparison_results.items():
        if "error" in res:
            report_md += f"### `{t_name}`\n> [!CAUTION]\n> **Reconciliation failed with error**: {res['error']}\n\n"
            continue

        has_mismatch = len(res["mismatch_counts"]) > 0 or res["local_only_count"] > 0 or res["ath_only_count"] > 0
        if not has_mismatch:
            continue

        report_md += f"### `{t_name}`\n"
        
        # Schema deviations
        if res["schema_diff"]["only_local"] or res["schema_diff"]["only_ath"]:
            report_md += "#### **Schema Mismatches**\n"
            if res["schema_diff"]["only_local"]:
                report_md += f"* Columns in DuckDB Only: {', '.join(f'`{c}`' for c in res['schema_diff']['only_local'])}\n"
            if res["schema_diff"]["only_ath"]:
                report_md += f"* Columns in Athena Only: {', '.join(f'`{c}`' for c in res['schema_diff']['only_ath'])}\n"

        # Value sum alignments
        if res["val_alignments"]:
            report_md += "#### **Numeric Metric Alignment**\n"
            report_md += "| Column Name | DuckDB Sum | Athena Sum | Difference | Alignment Rate |\n"
            report_md += "| :--- | :---: | :---: | :---: | :---: |\n"
            for va in res["val_alignments"]:
                report_md += f"| `{va['column']}` | {va['sum_local']:.2f} | {va['sum_ath']:.2f} | {va['diff']:+.2f} | {va['alignment_rate']:.4f}% |\n"
            report_md += "\n"

        # Discrepancy isolation/groups
        if res["discrepancies_by_date"] or res["discrepancies_by_group"]:
            report_md += "#### **Discrepancy Group Isolation**\n"
            if res["discrepancies_by_date"]:
                report_md += "* **By Year/Date**:\n"
                for dt, count in sorted(res["discrepancies_by_date"].items()):
                    report_md += f"  * Year **{dt}**: {count} discrepant records\n"
            if res["discrepancies_by_group"]:
                report_md += "* **By Dimension Group**:\n"
                for gp, count in sorted(res["discrepancies_by_group"].items()):
                    report_md += f"  * Group **{gp}**: {count} discrepant records\n"
            report_md += "\n"

        # Mismatch examples
        if res["mismatches_by_column"]:
            report_md += "#### **Anonymized Column Discrepancy Samples**\n"
            report_md += "| Composite Key | Column Name | DuckDB Value | Athena Value |\n"
            report_md += "| :--- | :--- | :--- | :--- |\n"
            for col, samples in res["mismatches_by_column"].items():
                for s in samples:
                    key_str = ", ".join(f"{k}={v}" for k, v in s["key"].items())
                    key_str = scrub_pii(key_str)
                    loc_val = scrub_pii(str(s["local"]))
                    ath_val = scrub_pii(str(s["ath"]))
                    report_md += f"| {key_str} | `{col}` | `{loc_val}` | `{ath_val}` |\n"
            report_md += "\n"

    report_md += """
---

## 3. Actionable Recommendations
1. **Investigate Athena Sync Lag**: For tables with mismatched row counts (such as `protheus_mesclada_vendas` or `redlara_planilha_combined`), check if the S3 promotion pipelines or Athena crawlers are running out of sync or ignoring deletions.
2. **Resolve Column Mappings**: Ensure schema promotion scripts explicitly map DuckDB fields to standard Athena lowercase naming conventions.
"""

    report_path = archive_report(report_md, "gold_athena_reconciliation")
    print(f"\nReconciliation report saved to: {report_path}")

if __name__ == "__main__":
    run_comparison()
