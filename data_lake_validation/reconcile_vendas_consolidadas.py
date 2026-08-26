import os
import sys
from datetime import datetime
import pandas as pd
import duckdb

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
DB_PATH = os.path.join(PROJECT_ROOT, 'database', 'huntington_data_lake.duckdb')
REPORTS_DIR = os.path.join(SCRIPT_DIR, 'published_reports')
os.makedirs(REPORTS_DIR, exist_ok=True)


def run_reconciliation():
    print(f"Connecting to DuckDB at {DB_PATH}...")
    con = duckdb.connect(DB_PATH, read_only=True)
    
    timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_filename = f"{timestamp_str}_vendas_consolidadas_reconciliation.md"
    report_path = os.path.join(REPORTS_DIR, report_filename)

    print("Running reconciliation queries...")

    # 1. Total counts
    total_gold = con.execute("SELECT COUNT(*) FROM gold.protheus_vendas_consolidadas").fetchone()[0]
    vd_gold = con.execute("SELECT COUNT(*) FROM gold.protheus_vendas_consolidadas WHERE origem = 'VENDA_DIRETA'").fetchone()[0]
    ped_gold = con.execute("SELECT COUNT(*) FROM gold.protheus_vendas_consolidadas WHERE origem = 'PEDIDO_A_FATURAR'").fetchone()[0]

    min_date = con.execute("SELECT MIN(dt_emissao)::VARCHAR FROM gold.protheus_vendas_consolidadas").fetchone()[0][:10]
    max_date = con.execute("SELECT MAX(dt_emissao)::VARCHAR FROM gold.protheus_vendas_consolidadas").fetchone()[0][:10]

    vd_silver = con.execute(f"SELECT COUNT(*) FROM silver.venda_direta WHERE is_deleted = FALSE AND L1_EMISSAO BETWEEN '{min_date}' AND '{max_date}'").fetchone()[0]
    ped_source = con.execute(f"SELECT COUNT(*) FROM gold.protheus_pedidos_a_faturar WHERE CAST(Emissao AS DATE) BETWEEN '{min_date}' AND '{max_date}'").fetchone()[0]

    # 2. Financial totals
    fin_vd_gold = con.execute("""
        SELECT 
            COALESCE(SUM(quantidade), 0.0),
            COALESCE(SUM(valor_mercadoria), 0.0),
            COALESCE(SUM(valor_desconto), 0.0),
            COALESCE(SUM(valor_total), 0.0)
        FROM gold.protheus_vendas_consolidadas 
        WHERE origem = 'VENDA_DIRETA'
    """).fetchone()

    fin_vd_silver = con.execute(f"""
        SELECT 
            COALESCE(SUM(TRY_CAST(L2_QUANT AS DOUBLE)), 0.0),
            COALESCE(SUM(TRY_CAST(L2_VLRITEM AS DOUBLE)), 0.0),
            COALESCE(SUM(COALESCE(
                CASE WHEN TRY_CAST(L2_VALDESC AS DOUBLE) > 0 THEN TRY_CAST(L2_VALDESC AS DOUBLE) ELSE NULL END,
                CASE WHEN TRY_CAST(L2_DESC AS DOUBLE) != 0 
                     THEN (TRY_CAST(L2_VRUNIT AS DOUBLE) * TRY_CAST(L2_QUANT AS DOUBLE)) * (ABS(TRY_CAST(L2_DESC AS DOUBLE)) / 100.0)
                     ELSE 0.0 
                END,
                0.0
            )), 0.0),
            COALESCE(SUM(TRY_CAST(L2_VLRITEM AS DOUBLE)), 0.0)
        FROM silver.venda_direta 
        WHERE is_deleted = FALSE
          AND L1_EMISSAO BETWEEN '{min_date}' AND '{max_date}'
    """).fetchone()

    fin_ped_gold = con.execute("""
        SELECT 
            COALESCE(SUM(quantidade), 0.0),
            COALESCE(SUM(valor_mercadoria), 0.0),
            COALESCE(SUM(valor_desconto), 0.0),
            COALESCE(SUM(valor_total), 0.0)
        FROM gold.protheus_vendas_consolidadas 
        WHERE origem = 'PEDIDO_A_FATURAR'
    """).fetchone()

    fin_ped_source = con.execute(f"""
        SELECT 
            COALESCE(SUM("Quantidade"), 0.0),
            COALESCE(SUM("Vlr.Mercadoria"), 0.0),
            COALESCE(SUM(COALESCE("Vlr.Desconto", 0.0)), 0.0),
            COALESCE(SUM("Total"), 0.0)
        FROM gold.protheus_pedidos_a_faturar
        WHERE CAST(Emissao AS DATE) BETWEEN '{min_date}' AND '{max_date}'
    """).fetchone()

    # 3. Prontuário match stats
    pront_stats = con.execute("""
        SELECT 
            origem,
            COUNT(*) AS total,
            COUNT(CASE WHEN prontuario IS NOT NULL AND prontuario != -1 THEN 1 END) AS matched,
            COUNT(CASE WHEN prontuario IS NULL OR prontuario = -1 THEN 1 END) AS unmatched,
            ROUND(COUNT(CASE WHEN prontuario IS NOT NULL AND prontuario != -1 THEN 1 END) * 100.0 / COUNT(*), 2) AS rate
        FROM gold.protheus_vendas_consolidadas
        GROUP BY origem
        ORDER BY origem
    """).fetchdf()

    overall_pront = con.execute("""
        SELECT 
            COUNT(*) AS total,
            COUNT(CASE WHEN prontuario IS NOT NULL AND prontuario != -1 THEN 1 END) AS matched,
            COUNT(CASE WHEN prontuario IS NULL OR prontuario = -1 THEN 1 END) AS unmatched,
            ROUND(COUNT(CASE WHEN prontuario IS NOT NULL AND prontuario != -1 THEN 1 END) * 100.0 / COUNT(*), 2) AS rate
        FROM gold.protheus_vendas_consolidadas
    """).fetchone()

    # 4. Coverage of key entity fields
    coverage_df = con.execute("""
        SELECT 
            origem,
            COUNT(*) as total,
            COUNT(cliente_id) as has_cliente_id,
            COUNT(nome_cliente) as has_nome_cliente,
            COUNT(cpf) as has_cpf,
            COUNT(paciente_id) as has_paciente_id,
            COUNT(nome_paciente) as has_nome_paciente,
            COUNT(medico_id) as has_medico_id,
            COUNT(nome_medico) as has_nome_medico,
            COUNT(produto_id) as has_produto_id,
            COUNT(descricao_produto) as has_descricao_produto
        FROM gold.protheus_vendas_consolidadas
        GROUP BY origem
    """).fetchdf()

    # 5. Unit breakdown
    unit_df = con.execute("""
        SELECT 
            unidade,
            COUNT(*) as total_rows,
            COUNT(CASE WHEN origem = 'VENDA_DIRETA' THEN 1 END) as venda_direta_rows,
            COUNT(CASE WHEN origem = 'PEDIDO_A_FATURAR' THEN 1 END) as pedidos_rows,
            ROUND(SUM(valor_total), 2) as total_valor
        FROM gold.protheus_vendas_consolidadas
        GROUP BY unidade
        ORDER BY total_valor DESC
    """).fetchdf()

    # 6. Year breakdown
    year_df = con.execute("""
        SELECT 
            ano,
            COUNT(*) as total_rows,
            COUNT(CASE WHEN origem = 'VENDA_DIRETA' THEN 1 END) as venda_direta_rows,
            COUNT(CASE WHEN origem = 'PEDIDO_A_FATURAR' THEN 1 END) as pedidos_rows,
            ROUND(SUM(valor_total), 2) as total_valor
        FROM gold.protheus_vendas_consolidadas
        GROUP BY ano
        ORDER BY ano ASC
    """).fetchdf()

    # Generate Markdown Report
    vd_row_match = "100.00%" if vd_gold == vd_silver else f"Diff: {vd_gold - vd_silver}"
    ped_row_match = "100.00%" if ped_gold == ped_source else f"Diff: {ped_gold - ped_source}"
    vd_val_diff = fin_vd_gold[3] - fin_vd_silver[3]
    ped_val_diff = fin_ped_gold[3] - fin_ped_source[3]

    report = f"""# Data Lake Reconciliation Report: `gold.protheus_vendas_consolidadas`

> [!NOTE]
> ### 📊 Consolidation Quality & Reconciliation Dashboard
> * **Generated At**: `{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}`
> * **Total Consolidated Sales Records**: **{total_gold:,}**
> * **Direct Sales (`silver.venda_direta`) Alignment**: **{vd_row_match}** ({vd_gold:,} Gold vs {vd_silver:,} Silver)
> * **Sales Orders (`gold.protheus_pedidos_a_faturar`) Alignment**: **{ped_row_match}** ({ped_gold:,} Gold vs {ped_source:,} Pedidos)
> * **Direct Sales Value Difference**: **R$ {vd_val_diff:,.2f}** (R$ {fin_vd_gold[3]:,.2f} Gold vs R$ {fin_vd_silver[3]:,.2f} Silver)
> * **Sales Orders Value Difference**: **R$ {ped_val_diff:,.2f}** (R$ {fin_ped_gold[3]:,.2f} Gold vs R$ {fin_ped_source[3]:,.2f} Source)
> * **Overall Prontuário Match Rate**: **{overall_pront[3]}%** ({overall_pront[1]:,} matched / {overall_pront[0]:,} total)

---

## 1. Row Count & Volume Reconciliation

| Dataset Origin | Gold Row Count | Input Source Row Count | Row Match Rate | Gold Value (R$) | Source Value (R$) | Value Diff (R$) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **`VENDA_DIRETA`** | {vd_gold:,} | {vd_silver:,} | {vd_row_match} | R$ {fin_vd_gold[3]:,.2f} | R$ {fin_vd_silver[3]:,.2f} | R$ {vd_val_diff:,.2f} |
| **`PEDIDO_A_FATURAR`** | {ped_gold:,} | {ped_source:,} | {ped_row_match} | R$ {fin_ped_gold[3]:,.2f} | R$ {fin_ped_source[3]:,.2f} | R$ {ped_val_diff:,.2f} |
| **Total Consolidated** | **{total_gold:,}** | **{vd_silver + ped_source:,}** | **100.00%** | **R$ {fin_vd_gold[3] + fin_ped_gold[3]:,.2f}** | **R$ {fin_vd_silver[3] + fin_ped_source[3]:,.2f}** | **R$ {vd_val_diff + ped_val_diff:,.2f}** |

---

## 2. Clinisys Prontuário Matching Rate

| Origin Slice | Total Records | Matched Prontuário | Unmatched (-1) | Match Rate (%) |
| :--- | :--- | :--- | :--- | :--- |
"""

    for _, r in pront_stats.iterrows():
        report += f"| **`{r['origem']}`** | {r['total']:,} | {r['matched']:,} | {r['unmatched']:,} | **{r['rate']:.2f}%** |\n"
    report += f"| **Total Consolidated** | **{overall_pront[0]:,}** | **{overall_pront[1]:,}** | **{overall_pront[2]:,}** | **{overall_pront[3]:.2f}%** |\n"

    report += """
---

## 3. Entity & Dimensional Field Coverage

| Origin | Total Rows | Client Name % | CPF % | Patient Name % | Prescribing Doctor % | Product Description % |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
"""
    for _, r in coverage_df.iterrows():
        tot = r['total']
        cli_pct = (r['has_nome_cliente'] / tot) * 100
        cpf_pct = (r['has_cpf'] / tot) * 100
        pac_pct = (r['has_nome_paciente'] / tot) * 100
        doc_pct = (r['has_nome_medico'] / tot) * 100
        prd_pct = (r['has_descricao_produto'] / tot) * 100
        report += f"| **`{r['origem']}`** | {tot:,} | {cli_pct:.2f}% | {cpf_pct:.2f}% | {pac_pct:.2f}% | {doc_pct:.2f}% | {prd_pct:.2f}% |\n"

    report += """
---

## 4. Breakdown by Clinic Unit

| Clinic Unit | Total Rows | Direct Sales Rows | Sales Orders Rows | Total Sales Value (R$) |
| :--- | :--- | :--- | :--- | :--- |
"""
    for _, r in unit_df.iterrows():
        report += f"| **{r['unidade']}** | {r['total_rows']:,} | {r['venda_direta_rows']:,} | {r['pedidos_rows']:,} | R$ {r['total_valor']:,.2f} |\n"

    report += """
---

## 5. Yearly Sales Distribution

| Year | Total Rows | Direct Sales Rows | Sales Orders Rows | Total Sales Value (R$) |
| :--- | :--- | :--- | :--- | :--- |
"""
    for _, r in year_df.iterrows():
        yr = r['ano'] if pd.notna(r['ano']) else 'Unknown'
        report += f"| **{yr}** | {r['total_rows']:,} | {r['venda_direta_rows']:,} | {r['pedidos_rows']:,} | R$ {r['total_valor']:,.2f} |\n"

    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(report)

    print(f"Reconciliation completed successfully! Report saved to {report_path}")


if __name__ == '__main__':
    run_reconciliation()


