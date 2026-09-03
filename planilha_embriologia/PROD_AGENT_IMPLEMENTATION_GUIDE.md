# Production Engineering Guide: Planilha Embriologia Alignment
### Target: AWS Athena (`silver_embriologia_staging`) $\rightarrow$ Full Parity with Local DuckDB Silver (`silver.planilha_embriologia_*`)

This guide is written specifically for the **Production Data Engineering Agent** maintaining the AWS Athena / Glue / dbt data lake. It provides the exact root-cause breakdown, architectural changes, SQL/dbt models, ingestion regexes, and verification queries needed to achieve 100% data parity between Athena production staging and the enhanced local Silver layer.

---

## 📌 Executive Summary & Target Parity Benchmarks

Following our data reconciliation audit, Athena production staging (`silver_embriologia_staging`) already achieves **100% exact parity on modern cohorts (2024–2026)** for core procedures. However, **4 critical gaps** cause the total numbers to diverge:

1. **Missing Dedicated Clinical Sheets in Bronze (2024–2026)**:
   Modern Excel workbooks (2023–2026) split procedures into dedicated tabs: `DOADORAS`, `FP (cong ovulos e tecidos)`, `FP (cong de Semen)`, and `IIU`. Athena prod currently **drops these tabs during Bronze ingestion**, resulting in **0 rows for 2024–2026 in `egg_freezing` and `iui`**, and **zero tables for `doadoras` and `fp_semen`**.
2. **Missing Salvador 2022 Monthly Sheets**:
   In `CASOS 2022 SSA.xlsx`, Salvador recorded procedures across 12 monthly sheets (`FIV JANEIRO`–`FIV DEZEMBRO`, `TEC JANEIRO`–`TEC DEZEMBRO`, `IIU`). Athena missed these monthly sheets.
3. **Restricted Multi-Clinic Ingestion in 2023**:
   Local consolidates multi-clinic workbooks (`Total 2023 Nova`, `GERAL 2023`) that were partially filtered out in Athena.
4. **Outcome String Casing & Normalization**:
   Athena preserves raw string artifacts like `'x'` vs `'X'`, `'0.0'` vs `'0'`, and untrimmed whitespace.

### Target Numbers to Match:

| Procedure Stream | Athena Current Target Table | Current Athena Rows | Target Local Silver Rows | Expected Variance to Resolve | Target Prontuário Match (%) |
| :--- | :--- | :---: | :---: | :---: | :---: |
| **FRESH (FIV)** | `new_planilha_embriologia_fresh` | 15,515 | **19,755** | **+4,240** (Salvador monthly + 2023 multi-clinic) | **$\ge 96.5\%$** |
| **FET (TEC)** | `new_planilha_embriologia_fet` | 15,669 | **18,149** | **+2,480** (Salvador monthly + 2023 multi-clinic) | **$\ge 97.0\%$** |
| **RECEP** | `new_planilha_embriologia_recep` | 2,846 | **1,639** | **-1,207** (Strict procedure tab isolation) | **$\ge 97.5\%$** |
| **FOT** | `new_planilha_embriologia_fot` | 2,304 | **2,868** | **+564** (Multi-clinic FOT sheets) | **$\ge 98.0\%$** |
| **FP ÓVULOS / EGG FREEZING** | `new_planilha_embriologia_egg_freezing` | 2,141 | **2,873** | **+732** (+2,706 modern rows 2024–26 from dedicated tab) | **$\ge 98.5\%$** |
| **IIU / IUI** | `new_planilha_embriologia_iui` | 138 | **179** | **+41** (+156 modern rows 2024–26 from dedicated tab) | **$\ge 97.0\%$** |
| **DOADORAS** | `new_planilha_embriologia_doadoras` | *None (0)* | **971** | **+971 (NEW TABLE)** | **$\ge 99.0\%$** |
| **FP SÊMEN** | `new_planilha_embriologia_fp_semen` | *None (0)* | **1,030** | **+1,030 (NEW TABLE)** | **$\ge 98.0\%$** |

---

## 🛠️ Step 1: Update Bronze Ingestion (AWS Glue / Python Ingestion DAG)

### 1.1 Expand Sheet Regex Pattern Matching
In your Excel extractor script or Glue job, ensure sheet discovery matches the dedicated clinical sheets across all clinic workbooks (`IBIRA`, `ITAIM`, `BH`, `BSB`, `SSA`):

```python
# Updated Bronze Sheet Discovery Mapping
SHEET_DISCOVERY_RULES = {
    'fresh': r'^(FRESH|FIV)$',
    'fet': r'^(FET|TEC)$',
    'recep': r'^RECEP',
    'fot': r'^FOT',
    'doadoras': r'^DOADORAS$',
    'fp_ovulos': r'^FP(\s*\(cong\s*ovulos.*|\s*\(cong\s*ovulo.*|\s*\(cong\s*ovulos\s*e\s*tecido.*)?$',
    'fp_semen': r'^(FP\s*\(cong\s*de\s*Semen\)|FP\s*\(cong\s*de\s*semen\)|SEMEN)$',
    'iiu': r'^IIU$'
}
```

### 1.2 Implement Salvador 2022 Multi-Sheet Union
In `CASOS 2022 SSA.xlsx`, iterate through all sheets and load each monthly batch into Bronze:

```python
def extract_ssa_2022_sheets(workbook_path):
    """
    Extracts all monthly sheets from CASOS 2022 SSA.xlsx
    """
    xl = pd.ExcelFile(workbook_path)
    for sheet in xl.sheet_names:
        sheet_clean = sheet.strip().upper()
        if re.match(r'^FIV\s+', sheet_clean):
            # Ingest to bronze_embriologia_staging.casos_2022_ssa_fiv
            ingest_sheet(xl, sheet, target_stream='fresh', year=2022, clinic='SSA')
        elif re.match(r'^TEC\s+', sheet_clean):
            # Ingest to bronze_embriologia_staging.casos_2022_ssa_tec
            ingest_sheet(xl, sheet, target_stream='fet', year=2022, clinic='SSA')
        elif sheet_clean == 'IIU':
            # Ingest to bronze_embriologia_staging.casos_2022_ssa_iiu
            ingest_sheet(xl, sheet, target_stream='iiu', year=2022, clinic='SSA')
```

---

## 🔄 Step 2: Update dbt Routing Model (`int_new_planilha__routed.sql`)

Update the routing model in dbt / SQL (`silver_embriologia_staging.int_new_planilha__routed`) to assign records to **8 distinct `cycle_bucket` values** instead of 6:

```sql
-- models/intermediate/int_new_planilha__routed.sql

WITH raw_bronze AS (
    SELECT * FROM {{ ref('bronze_embriologia_consolidated') }}
),

classified AS (
    SELECT
        *,
        CASE
            -- 1. Dedicated Procedure Sheets (Strict Priority Routing)
            WHEN UPPER(TRIM(_source_sheet)) LIKE '%DOADORA%' THEN 'doadoras'
            WHEN UPPER(TRIM(_source_sheet)) LIKE '%SEMEN%' THEN 'fp_semen'
            WHEN UPPER(TRIM(_source_sheet)) LIKE '%OVULO%' OR UPPER(TRIM(_source_sheet)) = 'FP' THEN 'egg_freezing'
            WHEN UPPER(TRIM(_source_sheet)) = 'IIU' THEN 'iui'
            WHEN UPPER(TRIM(_source_sheet)) IN ('FOT', 'DESCONGELAMENTO DE OVULOS') THEN 'fot'
            WHEN UPPER(TRIM(_source_sheet)) IN ('RECEP', 'RECEPTORA', 'OVODONACAO') THEN 'recep'
            WHEN UPPER(TRIM(_source_sheet)) IN ('FET', 'TEC') OR UPPER(TRIM(_source_sheet)) LIKE 'TEC %' THEN 'fet'
            WHEN UPPER(TRIM(_source_sheet)) IN ('FRESH', 'FIV') OR UPPER(TRIM(_source_sheet)) LIKE 'FIV %' THEN 'fresh'
            
            -- 2. Shared Sheets (TOTAL, GERAL, ANUAL) Classified by TIPO 1 Prefix
            WHEN UPPER(TRIM(tipo_1)) LIKE '%TEC%' OR UPPER(TRIM(tipo_1)) LIKE '%FET%' THEN 'fet'
            WHEN UPPER(TRIM(tipo_1)) LIKE '%RECEP%' OR UPPER(TRIM(tipo_1)) LIKE '%DOAÇÃO%' THEN 'recep'
            WHEN UPPER(TRIM(tipo_1)) LIKE '%FOT%' THEN 'fot'
            WHEN UPPER(TRIM(tipo_1)) LIKE '%CONG%OVULO%' OR UPPER(TRIM(tipo_1)) LIKE '%PRESERV%' THEN 'egg_freezing'
            WHEN UPPER(TRIM(tipo_1)) LIKE '%IIU%' OR UPPER(TRIM(tipo_1)) LIKE '%INSEMINA%' THEN 'iui'
            WHEN UPPER(TRIM(tipo_1)) LIKE '%DOADORA%' THEN 'doadoras'
            WHEN UPPER(TRIM(tipo_1)) LIKE '%FIV%' OR UPPER(TRIM(tipo_1)) LIKE '%ICSI%' OR opu IS NOT NULL THEN 'fresh'
            
            ELSE 'unrouted'
        END AS cycle_bucket
    FROM raw_bronze
)

SELECT * FROM classified;
```

---

## 🏗️ Step 3: Deploy Dedicated Silver Models in Athena

### 3.1 Model: `new_planilha_embriologia_doadoras.sql` (NEW)
Create this model in Athena to expose oocyte donor cycles:

```sql
-- models/silver/new_planilha_embriologia_doadoras.sql

SELECT
    row_key,
    pin,
    UPPER(TRIM(nome_da_paciente)) AS nome_da_paciente,
    data_de_nasc,
    data_do_procedimento,
    TRY_CAST(opu AS BIGINT) AS opu,
    TRY_CAST(COALESCE(total_de_mii, mii_total) AS BIGINT) AS mii_total,
    TRY_CAST(COALESCE(mii_doados_fresco, mii_doados_a_fresco) AS BIGINT) AS mii_doados_fresco,
    TRY_CAST(COALESCE(mii_doados_crio, mii_doados_criopreservados) AS BIGINT) AS mii_doados_crio,
    TRY_CAST(mii_restantes_crio AS BIGINT) AS mii_restantes_crio,
    tipo_da_doacao,
    fator_1,
    medico,
    unidade,
    year,
    file_name,
    sheet_name,
    prontuario,
    bronze_ingested_at
FROM {{ ref('int_new_planilha__routed') }}
WHERE cycle_bucket = 'doadoras'
  AND (pin IS NOT NULL OR nome_da_paciente IS NOT NULL);
```

### 3.2 Model: `new_planilha_embriologia_fp_semen.sql` (NEW)
Create this model in Athena to expose sperm cryopreservation cycles:

```sql
-- models/silver/new_planilha_embriologia_fp_semen.sql

SELECT
    row_key,
    pin,
    UPPER(TRIM(nome_da_paciente)) AS nome_da_paciente,
    data_de_nasc,
    data_do_procedimento,
    motivo_do_congelamento,
    concentr,
    motilid,
    morfo,
    TRY_CAST(COALESCE(no_de_palhetas_vials_crio, n_de_palhetas_vials_crio) AS BIGINT) AS no_de_palhetas_vials_crio,
    metodo_crio,
    obs,
    medico,
    unidade,
    year,
    file_name,
    sheet_name,
    prontuario,
    bronze_ingested_at
FROM {{ ref('int_new_planilha__routed') }}
WHERE cycle_bucket = 'fp_semen'
  AND (pin IS NOT NULL OR nome_da_paciente IS NOT NULL);
```

### 3.3 Model: `new_planilha_embriologia_egg_freezing.sql` (UPDATED)
Ensure the model selects from `cycle_bucket = 'egg_freezing'` so it captures both 2021–2023 shared carve-outs AND the 2024–2026 dedicated sheets:

```sql
-- models/silver/new_planilha_embriologia_egg_freezing.sql

SELECT
    row_key,
    pin,
    UPPER(TRIM(nome_da_paciente)) AS nome_da_paciente,
    data_de_nasc,
    COALESCE(data_da_puncao, data_do_procedimento) AS data_do_procedimento,
    data_crio,
    tipo_1,
    fator_1,
    incubadora,
    idade_mulher,
    TRY_CAST(opu AS BIGINT) AS opu,
    TRY_CAST(COALESCE(total_de_mii, mii_crio) AS BIGINT) AS mii_crio,
    TRY_CAST(mi_crio AS BIGINT) AS mi_crio,
    TRY_CAST(numero_de_fragmentos_crio AS BIGINT) AS numero_de_fragmentos_crio,
    UPPER(TRIM(ohss)) AS ohss,
    UPPER(TRIM(hemorragia)) AS hemorragia,
    UPPER(TRIM(infeccao)) AS infeccao,
    tipo_cancer,
    amostra_do_tecido,
    dia_cryo,
    unidade,
    year,
    file_name,
    sheet_name,
    prontuario,
    bronze_ingested_at
FROM {{ ref('int_new_planilha__routed') }}
WHERE cycle_bucket = 'egg_freezing'
  AND (pin IS NOT NULL OR nome_da_paciente IS NOT NULL);
```

### 3.4 Model: `new_planilha_embriologia_iui.sql` (UPDATED)
Include dedicated columns and pregnancy sac tracking (`no_sg`):

```sql
-- models/silver/new_planilha_embriologia_iui.sql

SELECT
    row_key,
    pin,
    UPPER(TRIM(nome_da_paciente)) AS nome_da_paciente,
    data_de_nasc,
    data_do_procedimento,
    tipo_1,
    idade_mulher,
    fator_1,
    indicacao_clinica,
    medicamento_indutor,
    tecnica_de_preparo,
    total_sptz_amostra_final,
    houve_transferencia,
    
    -- Cleaned Outcome Strings
    CASE 
        WHEN UPPER(TRIM(result)) IN ('NÃO ENGRAVIDOU', 'NAO ENGRAVIDOU', 'NEGATIVO', '0', '0.0') THEN 'NÃO ENGRAVIDOU'
        WHEN UPPER(TRIM(result)) IN ('POSITIVO', '1', '1.0') THEN 'POSITIVO'
        WHEN UPPER(TRIM(result)) LIKE '%BIOQUIMICA%' THEN 'BIOQUIMICA'
        WHEN UPPER(TRIM(result)) LIKE '%NASCIMENTO%' THEN 'NASCIMENTO'
        WHEN UPPER(TRIM(result)) LIKE '%ABORTO%' THEN 'ABORTO'
        ELSE UPPER(TRIM(result))
    END AS result,
    
    tipo_do_resultado,
    gravidez_clinica,
    gravidez_bioquimica,
    TRY_CAST(COALESCE(no_sg, n_sg) AS BIGINT) AS no_sg,
    TRY_CAST(COALESCE(no_nascidos, n_de_nascidos) AS BIGINT) AS no_nascidos,
    data_parto,
    tipo_de_parto,
    unidade,
    year,
    file_name,
    sheet_name,
    prontuario,
    bronze_ingested_at
FROM {{ ref('int_new_planilha__routed') }}
WHERE cycle_bucket = 'iui'
  AND (pin IS NOT NULL OR nome_da_paciente IS NOT NULL);
```

---

## 🧼 Step 4: Standardize Outcome Columns Across All Tables

To eliminate divergences in outcome counts and null distributions, apply these SQL standards across all Athena models:

### 4.1 Categorical `result` Standardizations:
```sql
CASE
    WHEN result IS NULL OR TRIM(result) IN ('', 'null', 'None', '-', '/') THEN NULL
    WHEN UPPER(TRIM(result)) IN ('EMBRYO TRANSFER', 'ET') THEN 'EMBRYO TRANSFER'
    WHEN UPPER(TRIM(result)) IN ('NO EMBRYO TRANSFER', 'NO ET', 'NO ET ') THEN 'NO EMBRYO TRANSFER'
    WHEN UPPER(TRIM(result)) IN ('EMBRYO VITRI', 'CRIO', 'CRIOPRESERVAÇÃO') THEN 'EMBRYO VITRI'
    WHEN UPPER(TRIM(result)) IN ('POSITIVO', 'POS', '1', '1.0') THEN 'POSITIVO'
    WHEN UPPER(TRIM(result)) IN ('NEGATIVO', 'NEG', '0', '0.0') THEN 'NEGATIVO'
    WHEN UPPER(TRIM(result)) IN ('REVITRI') THEN 'REVITRI'
    ELSE UPPER(TRIM(result))
END AS result
```

### 4.2 Pregnancy Indicators (`gravidez_clinica`, `gravidez_bioquimica`):
Standardize binary flags so `'X'`, `'x'`, `'1'`, `'1.0'` map to a consistent representation:
```sql
CASE 
    WHEN UPPER(TRIM(gravidez_clinica)) IN ('X', '1', '1.0', 'SIM') THEN '1'
    WHEN UPPER(TRIM(gravidez_clinica)) IN ('0', '0.0', 'NAO', 'NÃO') THEN '0'
    ELSE NULL
END AS gravidez_clinica
```

### 4.3 Numeric Casting (`BIGINT`):
Always wrap embryological counts with `TRY_CAST(... AS BIGINT)` to prevent float rendering (e.g. `5.0` $\rightarrow$ `5`):
* `TRY_CAST(opu AS BIGINT) AS opu`
* `TRY_CAST(total_de_mii AS BIGINT) AS total_de_mii`
* `TRY_CAST(qtd_blasto AS BIGINT) AS qtd_blasto`
* `TRY_CAST(no_et AS BIGINT) AS no_et`
* `TRY_CAST(no_nascidos AS BIGINT) AS no_nascidos`

---

## 🎯 Step 5: Verification Queries for the Prod Agent

After applying the changes in Athena, run these automated validation queries to confirm parity with Local Silver:

### 1. Row Count & Volume Verification
```sql
SELECT 'fresh' as stream, count(*) as cnt FROM silver_embriologia_staging.new_planilha_embriologia_fresh -- Expected: 19,755
UNION ALL
SELECT 'fet' as stream, count(*) as cnt FROM silver_embriologia_staging.new_planilha_embriologia_fet     -- Expected: 18,149
UNION ALL
SELECT 'recep' as stream, count(*) as cnt FROM silver_embriologia_staging.new_planilha_embriologia_recep -- Expected: 1,639
UNION ALL
SELECT 'fot' as stream, count(*) as cnt FROM silver_embriologia_staging.new_planilha_embriologia_fot     -- Expected: 2,868
UNION ALL
SELECT 'egg_freezing' as stream, count(*) as cnt FROM silver_embriologia_staging.new_planilha_embriologia_egg_freezing -- Expected: 2,873
UNION ALL
SELECT 'iui' as stream, count(*) as cnt FROM silver_embriologia_staging.new_planilha_embriologia_iui     -- Expected: 179
UNION ALL
SELECT 'doadoras' as stream, count(*) as cnt FROM silver_embriologia_staging.new_planilha_embriologia_doadoras -- Expected: 971
UNION ALL
SELECT 'fp_semen' as stream, count(*) as cnt FROM silver_embriologia_staging.new_planilha_embriologia_fp_semen -- Expected: 1,030;
```

### 2. Clinical Outcomes Proof Verification (Sums)
```sql
-- FRESH Oocyte & Embryo Sums
SELECT 
    SUM(TRY_CAST(opu AS BIGINT)) AS sum_opu,               -- Expected: ~164,948
    SUM(TRY_CAST(total_de_mii AS BIGINT)) AS sum_mii,      -- Expected: ~127,965
    SUM(TRY_CAST(qtd_blasto AS BIGINT)) AS sum_blasto      -- Expected: ~57,011
FROM silver_embriologia_staging.new_planilha_embriologia_fresh;

-- FET Transfer Proofs
SELECT 
    COUNT(CASE WHEN result = 'EMBRYO TRANSFER' THEN 1 END) AS et_transfers, -- Expected: 8,487
    SUM(TRY_CAST(no_et AS BIGINT)) AS sum_embryos_transferred              -- Expected: ~21,174
FROM silver_embriologia_staging.new_planilha_embriologia_fet;

-- FP Óvulos Cryopreserved MII Sum
SELECT 
    SUM(TRY_CAST(opu AS BIGINT)) AS sum_fp_opu,            -- Expected: ~33,519
    SUM(TRY_CAST(mii_crio AS BIGINT)) AS sum_fp_mii_crio   -- Expected: ~24,969
FROM silver_embriologia_staging.new_planilha_embriologia_egg_freezing;
```

### 3. Patient Resolution (Strategy L) Rate Verification
```sql
SELECT 
    table_name,
    COUNT(*) as total_rows,
    COUNT(CASE WHEN prontuario IS NOT NULL AND prontuario != -1 THEN 1 END) as matched_rows,
    ROUND(COUNT(CASE WHEN prontuario IS NOT NULL AND prontuario != -1 THEN 1 END) * 100.0 / COUNT(*), 2) as match_pct
FROM (
    SELECT 'fresh' as table_name, prontuario FROM silver_embriologia_staging.new_planilha_embriologia_fresh
    UNION ALL
    SELECT 'fet', prontuario FROM silver_embriologia_staging.new_planilha_embriologia_fet
    UNION ALL
    SELECT 'egg_freezing', prontuario FROM silver_embriologia_staging.new_planilha_embriologia_egg_freezing
    UNION ALL
    SELECT 'doadoras', prontuario FROM silver_embriologia_staging.new_planilha_embriologia_doadoras
    UNION ALL
    SELECT 'fp_semen', prontuario FROM silver_embriologia_staging.new_planilha_embriologia_fp_semen
)
GROUP BY 1;
-- Target: All streams must report match_pct >= 96.5%
```
