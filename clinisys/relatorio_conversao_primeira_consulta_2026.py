#!/usr/bin/env python3
"""
Relatório de Taxa de Conversão de Primeira Consulta (2026)
Foco Exclusivo: Consulta de Reprodução Humana e Consulta de Preservação da Fertilidade
Ordenado por Total de Conversões
--------------------------------------------------------------------------------------
Fonte: gold.extrato_atendimento_central (huntington_data_lake.duckdb)
Regras de Negócio:
  1. Foco no ano de 2026 (EXTRACT(year FROM data) = 2026)
  2. Apenas procedimentos executados (chegou = 'Atendido')
  3. Foco exclusivo em: Consulta de Reprodução Humana e Consulta Preservação da Fertilidade
  4. Conversão: Se houver outros procedimentos executados para aquele prontuário APÓS a primeira consulta
  5. Não é conversão: Se o único outro procedimento for ultrassom (US) ou realizado no mesmo dia
  6. Cálculo da Taxa de Conversão = (Consultas Convertidas / Total Primeiras Consultas) * 100
  7. Ordenação: Por Total de Conversões decrescente
  8. Se o banco estiver bloqueado (locked), aguarda ativamente até ser liberado
"""

import os
import sys
import time
import datetime
import argparse
import duckdb
import pandas as pd

# Paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(BASE_DIR, ".."))
DB_PATH = os.path.join(PROJECT_ROOT, "database", "huntington_data_lake.duckdb")
OUTPUT_CSV_MEDICOS = os.path.join(BASE_DIR, "relatorio_conversao_medicos_2026.csv")
OUTPUT_CSV_DETALHES = os.path.join(BASE_DIR, "relatorio_conversao_detalhado_2026.csv")
OUTPUT_MD = os.path.join(BASE_DIR, "relatorio_conversao_primeira_consulta_2026.md")

# Regex pattern for ultrasounds in Portuguese
ULTRASOUND_REGEX = r"(\bus\b|\busg\b|ultrassom|ultra-som|ecografia|ecotransvaginal|retorno p[oó]s ultrassom|avalia[çc][aã]o us|us\s*-|[0-9]+[ºª]?\s*us\b)"

def connect_with_lock_handling(db_path, wait_if_locked=True, retry_interval=10):
    """Attempt to connect to DuckDB, waiting patiently if file is locked."""
    print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Conectando ao DuckDB: {db_path}", flush=True)
    
    attempts = 0
    while True:
        try:
            con = duckdb.connect(db_path, read_only=True)
            print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Conexão estabelecida com sucesso em modo read-only!", flush=True)
            return con
        except Exception as e:
            err_msg = str(e)
            if "used by another process" in err_msg or "Could not set lock" in err_msg or "locked" in err_msg.lower():
                if wait_if_locked:
                    attempts += 1
                    print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Banco de dados bloqueado por outro processo (Tentativa {attempts}). Aguardando liberação em {retry_interval}s...", flush=True)
                    time.sleep(retry_interval)
                    continue
                else:
                    raise
            else:
                raise

def run_conversion_analysis(con):
    """Run the conversion rate SQL query focusing strictly on Reprodução and Preservação."""
    print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Executando consulta de conversão (Reprodução & Preservação) no gold.extrato_atendimento_central...", flush=True)

    query = f"""
    WITH all_executed AS (
        SELECT 
            agendamento_id,
            prontuario,
            CAST(data AS DATE) AS proc_date,
            data AS proc_timestamp,
            medico,
            medico_nome,
            medico_sobrenome,
            TRIM(COALESCE(medico_nome, '') || ' ' || COALESCE(medico_sobrenome, '')) AS medico_completo,
            centro_custos,
            centro_custos_nome,
            procedimento_nome,
            CASE 
                WHEN regexp_matches(lower(procedimento_nome), '{ULTRASOUND_REGEX}') THEN 1 
                ELSE 0 
            END AS is_ultrasound,
            CASE 
                WHEN lower(procedimento_nome) LIKE '%fiv%'
                  OR lower(procedimento_nome) LIKE '%fet%'
                  OR lower(procedimento_nome) LIKE '%aspira%'
                  OR lower(procedimento_nome) LIKE '%transfer%'
                  OR lower(procedimento_nome) LIKE '%congelamento%'
                  OR lower(procedimento_nome) LIKE '%descongelamento%'
                  OR lower(procedimento_nome) LIKE '%insemin%'
                  OR lower(procedimento_nome) LIKE '%histeroscopia%'
                  OR lower(procedimento_nome) LIKE '%bi[oó]psia%'
                THEN 1
                ELSE 0
            END AS is_treatment
        FROM gold.extrato_atendimento_central
        WHERE chegou = 'Atendido'
          AND prontuario IS NOT NULL
          AND data IS NOT NULL
    ),
    
    first_consultations_2026 AS (
        SELECT 
            agendamento_id,
            prontuario,
            proc_date AS consulta_date,
            proc_timestamp AS consulta_timestamp,
            medico,
            medico_completo,
            centro_custos_nome,
            procedimento_nome AS consulta_procedimento,
            CASE 
                WHEN lower(procedimento_nome) LIKE '%reprodu%' THEN 'Reprodução Humana'
                WHEN lower(procedimento_nome) LIKE '%preserva%' THEN 'Preservação da Fertilidade'
                ELSE 'Outra'
            END AS categoria_consulta,
            ROW_NUMBER() OVER (
                PARTITION BY prontuario, proc_date, medico
                ORDER BY proc_timestamp, agendamento_id
            ) AS rn_same_day_dedup
        FROM all_executed
        WHERE EXTRACT(year FROM proc_date) = 2026
          AND is_ultrasound = 0
          AND (
              lower(procedimento_nome) LIKE '%1%consulta%reprodu%'
              OR lower(procedimento_nome) LIKE '%primeira consulta%reprodu%'
              OR lower(procedimento_nome) LIKE '%1%consulta%preserva%'
              OR lower(procedimento_nome) LIKE '%primeira consulta%preserva%'
          )
    ),
    
    first_consultations_unique AS (
        SELECT *
        FROM first_consultations_2026
        WHERE rn_same_day_dedup = 1
    ),
    
    consultations_with_conversion AS (
        SELECT 
            c.agendamento_id,
            c.prontuario,
            c.consulta_date,
            c.medico,
            c.medico_completo,
            c.centro_custos_nome,
            c.consulta_procedimento,
            c.categoria_consulta,
            
            -- Subsequent non-US procedures on dates > consultation_date
            COUNT(CASE 
                WHEN sub.proc_date > c.consulta_date 
                 AND sub.is_ultrasound = 0 
                THEN 1 
            END) AS qtd_pos_nao_us,
            
            -- Subsequent treatments on dates > consultation_date
            COUNT(CASE 
                WHEN sub.proc_date > c.consulta_date 
                 AND sub.is_treatment = 1 
                THEN 1 
            END) AS qtd_pos_tratamentos,
            
            -- Subsequent US only on dates > consultation_date
            COUNT(CASE 
                WHEN sub.proc_date > c.consulta_date 
                 AND sub.is_ultrasound = 1 
                THEN 1 
            END) AS qtd_pos_us,
            
            -- Same-day non-US procedures
            COUNT(CASE 
                WHEN sub.proc_date = c.consulta_date 
                 AND sub.agendamento_id != c.agendamento_id 
                 AND sub.is_ultrasound = 0 
                THEN 1 
            END) AS qtd_mesmo_dia_nao_us,
            
            -- Conversion Flag (User Rule: any subsequent non-US procedure strictly after first consultation date)
            CASE 
                WHEN COUNT(CASE WHEN sub.proc_date > c.consulta_date AND sub.is_ultrasound = 0 THEN 1 END) > 0 
                THEN 1 
                ELSE 0 
            END AS is_converted,
            
            -- Conversion Flag (Treatment: proceeded to FIV/FET/Aspiração/Transferência/etc.)
            CASE 
                WHEN COUNT(CASE WHEN sub.proc_date > c.consulta_date AND sub.is_treatment = 1 THEN 1 END) > 0 
                THEN 1 
                ELSE 0 
            END AS is_converted_tratamento
            
        FROM first_consultations_unique c
        LEFT JOIN all_executed sub
          ON c.prontuario = sub.prontuario
         AND sub.proc_date >= c.consulta_date
        GROUP BY 
            c.agendamento_id,
            c.prontuario,
            c.consulta_date,
            c.medico,
            c.medico_completo,
            c.centro_custos_nome,
            c.consulta_procedimento,
            c.categoria_consulta
    )
    
    SELECT * FROM consultations_with_conversion
    ORDER BY consulta_date ASC, medico_completo ASC
    """

    df_details = con.execute(query).df()
    print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Extraídas {len(df_details):,} primeiras consultas de Reprodução/Preservação em 2026.", flush=True)
    
    return df_details

def generate_reports(df_details):
    """Generate summary tables ordered strictly by Total de Conversões descending and save CSV / Markdown reports."""
    
    # 1. Summary by Doctor - ORDERED BY TOTAL DE CONVERSÕES DESC
    df_by_doctor = df_details.groupby(['medico_completo']).agg(
        total_primeiras_consultas=('agendamento_id', 'count'),
        total_conversoes=('is_converted', 'sum'),
        nao_convertidas=('is_converted', lambda x: (x == 0).sum()),
        conversoes_tratamento=('is_converted_tratamento', 'sum'),
    ).reset_index()
    
    df_by_doctor['taxa_conversao_pct'] = (
        (df_by_doctor['total_conversoes'] / df_by_doctor['total_primeiras_consultas']) * 100
    ).round(2)
    df_by_doctor['taxa_conversao_tratamento_pct'] = (
        (df_by_doctor['conversoes_tratamento'] / df_by_doctor['total_primeiras_consultas']) * 100
    ).round(2)
    
    # SORT STRICTLY BY TOTAL DE CONVERSÕES DESCENDING!
    df_by_doctor = df_by_doctor.sort_values(
        by=['total_conversoes', 'total_primeiras_consultas'], 
        ascending=[False, False]
    )
    df_by_doctor['medico_completo'] = df_by_doctor['medico_completo'].replace('', 'Não Informado')

    # 2. Summary by Unit / Centro de Custos - ORDERED BY TOTAL DE CONVERSÕES DESC
    df_by_unit = df_details.groupby(['centro_custos_nome']).agg(
        total_primeiras_consultas=('agendamento_id', 'count'),
        total_conversoes=('is_converted', 'sum'),
        nao_convertidas=('is_converted', lambda x: (x == 0).sum()),
        conversoes_tratamento=('is_converted_tratamento', 'sum'),
    ).reset_index()
    df_by_unit['taxa_conversao_pct'] = (
        (df_by_unit['total_conversoes'] / df_by_unit['total_primeiras_consultas']) * 100
    ).round(2)
    df_by_unit['taxa_conversao_tratamento_pct'] = (
        (df_by_unit['conversoes_tratamento'] / df_by_unit['total_primeiras_consultas']) * 100
    ).round(2)
    df_by_unit = df_by_unit.sort_values(
        by=['total_conversoes', 'total_primeiras_consultas'], 
        ascending=[False, False]
    )

    # 3. Summary by Procedure Type - ORDERED BY TOTAL DE CONVERSÕES DESC
    df_by_proc = df_details.groupby(['consulta_procedimento', 'categoria_consulta']).agg(
        total_primeiras_consultas=('agendamento_id', 'count'),
        total_conversoes=('is_converted', 'sum'),
        nao_convertidas=('is_converted', lambda x: (x == 0).sum()),
        conversoes_tratamento=('is_converted_tratamento', 'sum'),
    ).reset_index()
    df_by_proc['taxa_conversao_pct'] = (
        (df_by_proc['total_conversoes'] / df_by_proc['total_primeiras_consultas']) * 100
    ).round(2)
    df_by_proc['taxa_conversao_tratamento_pct'] = (
        (df_by_proc['conversoes_tratamento'] / df_by_proc['total_primeiras_consultas']) * 100
    ).round(2)
    df_by_proc = df_by_proc.sort_values(
        by=['total_conversoes', 'total_primeiras_consultas'], 
        ascending=[False, False]
    )

    # 4. Overall Totals
    tot_geral = len(df_details)
    tot_conv = int(df_details['is_converted'].sum())
    tot_nao_conv = tot_geral - tot_conv
    taxa_conv_geral = round((tot_conv / tot_geral) * 100, 2) if tot_geral > 0 else 0.0
    tot_trat = int(df_details['is_converted_tratamento'].sum())
    taxa_trat_geral = round((tot_trat / tot_geral) * 100, 2) if tot_geral > 0 else 0.0

    print("\n=======================================================================", flush=True)
    print("RESUMO DE CONVERSÃO (ORDENADO POR TOTAL DE CONVERSÕES DESC) - 2026", flush=True)
    print("=======================================================================", flush=True)
    print(f"Total de Primeiras Consultas Executadas: {tot_geral:,}", flush=True)
    print(f"Total Convertidas (Regra Geral: Procedimentos Não-US Posteriores): {tot_conv:,} ({taxa_conv_geral:.2f}%)", flush=True)
    print(f"Total Convertidas para Tratamento Direto (FIV/FET/Punção/Transferência): {tot_trat:,} ({taxa_trat_geral:.2f}%)", flush=True)
    print("=======================================================================\n", flush=True)
    
    print("--- TOP 20 MÉDICOS ORDENADOS POR TOTAL DE CONVERSÕES ---", flush=True)
    print(df_by_doctor.head(20).to_string(index=False), flush=True)
    print("\n--- RESUMO POR UNIDADE (ORDENADO POR TOTAL DE CONVERSÕES) ---", flush=True)
    print(df_by_unit.to_string(index=False), flush=True)

    # Save to CSV
    df_by_doctor.to_csv(OUTPUT_CSV_MEDICOS, index=False, encoding='utf-8-sig')
    df_details.to_csv(OUTPUT_CSV_DETALHES, index=False, encoding='utf-8-sig')
    print(f"\n[OK] Relatório por médico exportado para: {OUTPUT_CSV_MEDICOS}", flush=True)
    print(f"[OK] Base detalhada exportada para: {OUTPUT_CSV_DETALHES}", flush=True)

    # Build Markdown Report
    md_content = f"""# Relatório de Conversão de Primeiras Consultas (2026)
### Foco: Consulta de Reprodução Humana e Consulta de Preservação da Fertilidade
### Ordenação: Por Total de Conversões Decrescente

**Tabela Analisada:** `gold.extrato_atendimento_central`  
**Data de Extração:** {datetime.datetime.now().strftime('%d/%m/%Y %H:%M:%S')}  

---

## 1. Regras de Negócio e Metodologia
1. **Foco Temporal:** Ano de 2026 (`EXTRACT(year FROM data) = 2026`).
2. **Procedimentos Executados:** Apenas agendamentos com atendimento confirmado e realizado (`chegou = 'Atendido'`).
3. **Escopo de Especialidades:** Foco exclusivo em **Consulta de Reprodução Humana** e **Consulta de Preservação da Fertilidade** (`1ª Consulta de Reprodução Humana` e `1ª Consulta Preservação da Fertilidade`).
4. **Critério de Conversão:**
   - **Convertido (Regra Geral):** O paciente (`prontuario`) realizou ao menos um procedimento executado em **data estritamente posterior** à primeira consulta (`data > data_primeira_consulta`).
   - **Não é Conversão:** Se o procedimento ocorreu no **mesmo dia** da consulta ou se os únicos procedimentos posteriores foram **ultrassons (US / USG / ecografia)**.
   - **Conversão para Tratamento (Métrica Adicional):** Paciente que avançou para procedimentos de ciclo (FIV, FET, aspiração folicular/punção, transferência embrionária, congelamento, biópsia, etc.).
5. **Ordenação:** Todas as tabelas estão ordenadas estritamente pelo **Total de Conversões** em ordem decrescente.
6. **Diferença entre as Taxas de Conversão:**
   - **Taxa de Conversão Geral ({taxa_conv_geral:.2f}%):** Mede o engajamento e a retenção clínica. Inclui qualquer paciente que retornou para consultas subsequentes de planejamento/reavaliação, exames pré-tratamento ou procedimentos clínicos.
   - **Taxa de Conversão para Tratamento ({taxa_trat_geral:.2f}%):** Mede a conversão estrita em procedimentos de ciclo de alta complexidade (FIV, FET, punção, transferência embrionária).

---

## 2. Indicadores Gerais (2026)

| Indicador | Volume | Taxa (%) |
| :--- | :---: | :---: |
| **Total de Primeiras Consultas Executadas** | **{tot_geral:,}** | 100,00% |
| **Total de Conversões (Procedimentos Posteriores Não-US)** | **{tot_conv:,}** | **{taxa_conv_geral:.2f}%** |
| **Consultas Não Convertidas** | **{tot_nao_conv:,}** | **{100 - taxa_conv_geral:.2f}%** |
| *Conversões em Tratamentos Diretos (FIV/FET/Punção)* | *{tot_trat:,}* | *{taxa_trat_geral:.2f}%* |

---

## 3. Conversão por Profissional / Médico (Ordenado por Total de Conversões Decrescente)

| Médico | Total 1ªs Consultas | Total de Conversões | Não Convertidas | Taxa de Conversão Geral (%) | Conv. Tratamento (Qtd) | Taxa Tratamento (%) |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
"""
    for _, row in df_by_doctor.iterrows():
        md_content += f"| {row['medico_completo']} | {row['total_primeiras_consultas']:,} | **{row['total_conversoes']:,}** | {row['nao_convertidas']:,} | {row['taxa_conversao_pct']:.2f}% | {row['conversoes_tratamento']:,} | {row['taxa_conversao_tratamento_pct']:.2f}% |\n"

    md_content += f"""
---

## 4. Conversão por Unidade / Centro de Custos (Ordenado por Total de Conversões Decrescente)

| Unidade | Total 1ªs Consultas | Total de Conversões | Não Convertidas | Taxa Conversão Geral (%) | Conv. Tratamento (Qtd) | Taxa Tratamento (%) |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
"""
    for _, row in df_by_unit.iterrows():
        md_content += f"| {row['centro_custos_nome']} | {row['total_primeiras_consultas']:,} | **{row['total_conversoes']:,}** | {row['nao_convertidas']:,} | {row['taxa_conversao_pct']:.2f}% | {row['conversoes_tratamento']:,} | {row['taxa_conversao_tratamento_pct']:.2f}% |\n"

    md_content += f"""
---

## 5. Conversão por Tipo de Procedimento de 1ª Consulta

| Procedimento | Categoria | Total 1ªs Consultas | Total de Conversões | Taxa Conversão Geral (%) | Conv. Tratamento (Qtd) | Taxa Tratamento (%) |
| :--- | :--- | :---: | :---: | :---: | :---: | :---: |
"""
    for _, row in df_by_proc.iterrows():
        md_content += f"| {row['consulta_procedimento']} | {row['categoria_consulta']} | {row['total_primeiras_consultas']:,} | **{row['total_conversoes']:,}** | {row['taxa_conversao_pct']:.2f}% | {row['conversoes_tratamento']:,} | {row['taxa_conversao_tratamento_pct']:.2f}% |\n"

    with open(OUTPUT_MD, 'w', encoding='utf-8') as f:
        f.write(md_content)
    print(f"[OK] Relatório markdown exportado para: {OUTPUT_MD}", flush=True)

    return df_by_doctor, df_by_unit, df_by_proc

def main():
    parser = argparse.ArgumentParser(description="Relatório de Conversão de Primeira Consulta 2026 (Reprodução & Preservação)")
    parser.add_argument("--no-wait", action="store_true", help="Do not wait if database is locked, exit immediately")
    parser.add_argument("--retry-interval", type=int, default=10, help="Interval in seconds to retry if database is locked")
    args = parser.parse_args()

    con = connect_with_lock_handling(DB_PATH, wait_if_locked=not args.no_wait, retry_interval=args.retry_interval)
    try:
        df_details = run_conversion_analysis(con)
        generate_reports(df_details)
    finally:
        con.close()
        print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Conexão DuckDB encerrada.", flush=True)

if __name__ == "__main__":
    main()
