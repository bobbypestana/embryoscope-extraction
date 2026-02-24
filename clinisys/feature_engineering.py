"""
Feature engineering utilities for Silver layer
Handles table-specific feature creation and transformations
"""

import logging

logger = logging.getLogger(__name__)


def feature_creation(con, table):
    """
    Apply table-specific feature engineering to the silver table.

    Args:
        con: DuckDB connection
        table: Table name to apply features to

    Returns:
        None (modifies the silver table in place)
    """
    if table == 'view_micromanipulacao_oocitos':

        # Add embryo_number: row_number per id_micromanipulacao ordered by id (for all rows)
        logger.info(f"Adding embryo_number to silver.{table} (for all rows)")
        con.execute(f"""
            CREATE OR REPLACE TABLE silver.{table} AS
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY id_micromanipulacao ORDER BY id) AS embryo_number
            FROM silver.{table}
        """)
        logger.info(f"embryo_number added to silver.{table}")


    elif table == 'view_tratamentos':
        # Add calculated columns: BMI, previous_et, previous_et_od
        logger.info(f"Adding calculated columns to silver.{table}")

        con.execute(f"""
            CREATE OR REPLACE TABLE silver.{table} AS
            SELECT *,
                   -- BMI: weight(kg) / height(m)^2
                   CASE
                       WHEN peso_paciente IS NOT NULL
                            AND altura_paciente IS NOT NULL
                            AND altura_paciente > 0
                       THEN ROUND(CAST(peso_paciente AS DOUBLE) / POWER(CAST(altura_paciente AS DOUBLE), 2), 2)
                       ELSE NULL
                   END AS bmi,

                   -- Previous ET: count of previous embryo transfers for this patient
                   -- Excludes: 'No transfer', 'Cancelado', 'Congelamento de Óvulos'
                   COALESCE((
                       SELECT COUNT(*)
                       FROM silver.{table} AS prev
                       WHERE prev.prontuario = {table}.prontuario
                         AND COALESCE(prev.data_transferencia, prev.data_procedimento, prev.data_dum)
                             < COALESCE({table}.data_transferencia, {table}.data_procedimento, {table}.data_dum)
                         AND (prev.resultado_tratamento IS NULL
                              OR prev.resultado_tratamento NOT IN ('No transfer', 'Cancelado', 'Congelamento de Óvulos'))
                   ), 0) AS previous_et,

                   -- Previous OD ET: count of previous egg donation embryo transfers
                   COALESCE((
                       SELECT COUNT(*)
                       FROM silver.{table} AS prev
                       WHERE prev.prontuario = {table}.prontuario
                         AND COALESCE(prev.data_transferencia, prev.data_procedimento, prev.data_dum)
                             < COALESCE({table}.data_transferencia, {table}.data_procedimento, {table}.data_dum)
                         AND prev.doacao_ovulos = 'Sim'
                         AND (prev.resultado_tratamento IS NULL
                              OR prev.resultado_tratamento NOT IN ('No transfer', 'Cancelado', 'Congelamento de Óvulos'))
                   ), 0) AS previous_et_od

            FROM silver.{table}
        """)

        # unidade is a FK integer (references view_unidades.id) in view_tratamentos,
        # but was removed from global int_columns because in view_medicamentos_prescricoes
        # it holds free-text unit strings. Re-cast it to INTEGER here.
        logger.info(f"Re-casting unidade to INTEGER in silver.{table}")
        con.execute(f"""
            CREATE OR REPLACE TABLE silver.{table} AS
            SELECT * EXCLUDE (unidade),
                   TRY_CAST(unidade AS INTEGER) AS unidade
            FROM silver.{table}
        """)

        logger.info(f"Added bmi, previous_et, previous_et_od, re-cast unidade to silver.{table}")


    elif table == 'view_medicamentos_prescricoes':
        # ── Step 1: medicamento is a FK integer ID (references view_medicamentos.id).
        # It was removed from global int_columns because in view_medicamentos the same
        # column holds free-text drug names. Re-cast it to INTEGER here.
        logger.info(f"Re-casting medicamento to INTEGER in silver.{table}")
        con.execute(f"""
            CREATE OR REPLACE TABLE silver.{table} AS
            SELECT * EXCLUDE (medicamento),
                   TRY_CAST(medicamento AS INTEGER) AS medicamento
            FROM silver.{table}
        """)
        logger.info(f"medicamento cast to INTEGER in silver.{table}")

        # ── Steps 2-7: Enrichment — add med_nome, dose (cleaned DOUBLE),
        #   unidade (VARCHAR), unidade_padronizada, numero_dias, dose_total,
        #   grupo_medicamento.  All done in DuckDB SQL.
        #   Mirrors analisys_v8.ipynb cells 13–50.
        logger.info(f"Running prescription enrichment for silver.{table}")

        # Unit lookup lists (from notebook cell 34).
        _ui      = ["UI","ui","Ui","U","u","IU","uii","UI ","ui ","unidades","unidade","UNIDADE","UNIDADES","UN"]
        _mg      = ["mg","Mg","MG","mg ","miligrama","mh","g","10mg","200mg","10MG","200MG","2mg","0,25mg"]
        _mcg     = ["mgc","mgg","mcg","Mcg","MCG","MgG"]
        _comp    = ["comprimido","comprimidos","comprimido ","comprimidos ","comp","Comp","COMP","COMPRIMIDO",
                    "COMPRIMIDOS","COMPRIMIDOS ","COMPRMIDOS","comprimido de 50","comprimido - 5mg",
                    "Comprimido - 5mg","Comprimido - 20mg","compirmidos ","comprimido vermelho",
                    "comprimidos omprimidos omprimido"," comprimido","6omprimido","comp de 200 mg","comp ",
                    "comp."," comp","3 comp","1comp","2comprimido","cpr","Cpg","CPR","CPR ","Cpr","Comprimido",
                    "COMPRIMIDO ","Comprimido vermelho","Comprimidos","3 comprimidos omprimidos omprimido",
                    "Comprimido ","3comp","Comp ","com","Comprimido de 5 mg","compridos","compimido",
                    "Comprimido vermelho","Comprimidos"]
        _cap     = ["cápsula","cp","CP","Cp","cp ","1cp","2cp","cap","caps","capsula","capsulas","cápsulas",
                    "cps","CAPS","CAPSULAS ","capsulas ","CAPsulas","Caps","caps.","CAPS.","cápsula",
                    "capsule","CPS","1 CP","cápsulas ","Capsulas"]
        _ampola  = ["ampola","ampolas","Ampola","AMPOLA","ampolas ","ampOLAS","ampolasI","ampolass",
                    "ampolaS","ampola ","AMPOLAS ","AMPOLAS","amp.","am","ampo","01 ampola","1 ampola",
                    "3 ampolas","amp","AMP","1 AMPOLA","Amp","Ampolas","AMP ","2 amp","AMPOLA "]
        _ovulo   = ["Ovulo Vaginal","ovulos ","ovulos","óvulos","Ovulos","Óvulo","óvulo","OVULOS","OVULO",
                    "ovulo","ovulos ","0VULOS","0VULOS ","2OVULOS","1 OVULO","ÓVULOS","ÓVULO","OVULOS ",
                    "cápsulas via vaginal"," 0vulos","ÓVULOS "]
        _pump    = ["pump","pumps","PUMP","pumpS","pumps ","Pumps","pumpss","pump (medida)","PUMPS","Pump "]
        _puff    = ["puff","puffs","pufss","pufs","puf","púmp","puff (1,25mg)"]
        _seringa = ["seringa","seringas","SERINGAS","Seringas","seringas ","SERINGA","Injetável"]
        _caneta  = ["caneta","CANETA","canetas","Caneta","Canetas","CANETAS","caneta ","canetas ",
                    "canetaS","CanetaS","Canetas ","CANETAS ","canetaS ","CanetaS "]
        _frasco  = ["fr","frasco"]

        def _sql_list(vals):
            """Return a SQL IN-list literal from a Python list of strings."""
            return ", ".join(f"'{v.replace(chr(39), chr(39)+chr(39))}'" for v in vals)

        # Searched CASE expression using CAST(p.unidade AS VARCHAR) in each WHEN clause.
        # This guards against the column still being typed as INTEGER in the materialized table.
        unidade_padronizada_sql = (
            "CASE"
            + f"\n                WHEN CAST(p.unidade AS VARCHAR) IN ({_sql_list(_ui)})      THEN 'UI'"
            + f"\n                WHEN CAST(p.unidade AS VARCHAR) IN ({_sql_list(_mg)})      THEN 'mg'"
            + f"\n                WHEN CAST(p.unidade AS VARCHAR) IN ({_sql_list(_mcg)})     THEN 'mcg'"
            + f"\n                WHEN CAST(p.unidade AS VARCHAR) IN ({_sql_list(_comp)})    THEN 'comp'"
            + f"\n                WHEN CAST(p.unidade AS VARCHAR) IN ({_sql_list(_cap)})     THEN 'cap'"
            + f"\n                WHEN CAST(p.unidade AS VARCHAR) IN ({_sql_list(_ampola)})  THEN 'ampola'"
            + f"\n                WHEN CAST(p.unidade AS VARCHAR) IN ({_sql_list(_ovulo)})   THEN 'ovulo'"
            + f"\n                WHEN CAST(p.unidade AS VARCHAR) IN ({_sql_list(_pump)})    THEN 'pump'"
            + f"\n                WHEN CAST(p.unidade AS VARCHAR) IN ({_sql_list(_puff)})    THEN 'puff'"
            + f"\n                WHEN CAST(p.unidade AS VARCHAR) IN ({_sql_list(_seringa)}) THEN 'seringa'"
            + f"\n                WHEN CAST(p.unidade AS VARCHAR) IN ({_sql_list(_caneta)})  THEN 'caneta'"
            + f"\n                WHEN CAST(p.unidade AS VARCHAR) IN ({_sql_list(_frasco)})  THEN 'frasco'"
            + "\n                ELSE 'NÃO DEFINIDO'"
            + "\n            END"
        )

        con.execute(f"""
            CREATE OR REPLACE TABLE silver.{table} AS
            WITH base AS (
                SELECT
                    p.* EXCLUDE (dose, unidade),

                    -- Ensure unidade is stored as VARCHAR (bronze may have typed it as INTEGER)
                    CAST(p.unidade AS VARCHAR) AS unidade,

                    -- Join drug name text from view_medicamentos
                    m.medicamento AS med_nome,

                    -- Clean dose: strip non-numeric chars (except comma/period),
                    -- then treat '.' as thousands separator and ',' as decimal.
                    -- Mirrors: clean_value() + pd.to_numeric() in notebook cells 13-14.
                    TRY_CAST(
                        REPLACE(
                            REPLACE(
                                REGEXP_REPLACE(CAST(p.dose AS VARCHAR), '[^0-9,.]', '', 'g'),
                                '.', ''
                            ),
                            ',', '.'
                        ) AS DOUBLE
                    ) AS dose,

                    -- Map raw unit string to a canonical category.
                    -- Mirrors: notebook cell 35 (unidades_padronizadas).
                    {unidade_padronizada_sql} AS unidade_padronizada,

                    -- numero_dias = data_final - data_inicial + 1
                    -- Mirrors: dias_de_uso in notebook cell 39.
                    CASE
                        WHEN p.data_inicial IS NOT NULL AND p.data_final IS NOT NULL
                        THEN DATEDIFF('day', p.data_inicial::DATE, p.data_final::DATE) + 1
                        ELSE NULL
                    END AS numero_dias,

                    -- grupo_medicamento: first significant word of the drug name
                    -- (upper-cased, hyphens replaced by spaces). If the first word
                    -- is a single letter, take the second word instead.
                    -- Mirrors: process_medication_names() in notebook cells 49-50.
                    CASE
                        WHEN m.medicamento IS NULL THEN NULL
                        WHEN LENGTH(
                            SPLIT_PART(UPPER(REPLACE(m.medicamento, '-', ' ')), ' ', 1)
                        ) <= 1
                        THEN SPLIT_PART(UPPER(REPLACE(m.medicamento, '-', ' ')), ' ', 2)
                        ELSE SPLIT_PART(UPPER(REPLACE(m.medicamento, '-', ' ')), ' ', 1)
                    END AS grupo_medicamento

                FROM silver.{table} p
                LEFT JOIN silver.view_medicamentos m ON m.id = p.medicamento
            )
            SELECT
                *,
                -- dose_total = |numero_dias * dose * (24 / intervalo)|
                -- Mirrors: np.abs(dias_de_uso * dose * (24. / intervalo)) in notebook cell 44.
                CASE
                    WHEN numero_dias IS NOT NULL
                         AND dose IS NOT NULL
                         AND intervalo IS NOT NULL
                         AND intervalo <> 0
                    THEN ABS(CAST(numero_dias AS DOUBLE) * dose * (24.0 / intervalo))
                    ELSE NULL
                END AS dose_total
            FROM base
        """)

        logger.info(
            f"Enrichment complete for silver.{table}: "
            f"med_nome, dose (cleaned DOUBLE), unidade (VARCHAR), unidade_padronizada, "
            f"numero_dias, dose_total, grupo_medicamento"
        )

    # Add more table-specific features here as needed
