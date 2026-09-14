#!/usr/bin/env python3
"""
generate_venda_direta_dashboard.py
==================================
Generates an executive, interactive financial dashboard for Venda Direta
mirroring the exact layout, typography, KPI cards, and unit table from the
executive specification.

Includes:
- Summary table by Clinic / Brasil
- Dynamic month and unit selection
- Card 1: VS. Meta with LY MTD comparisons
- Card 2: Composição do Realizado
- Granular Case Drilldown drawer:
  - Faturado Direto
  - Faturado c/ Pedido
  - Pedidos Abertos (a faturar)
  - Instant live search & filtering
  - Paginated high-performance DOM
  - 1-click CSV Export for operational billing teams

Data Source:
- DuckDB: database/huntington_data_lake.duckdb -> gold.protheus_vendas_consolidadas
- Targets: protheus/dashboard/targets.json
"""

import os
import sys
import json
import logging
import duckdb
from datetime import datetime
import time

# -----------------------------------------------------------------------------
# 1. Setup Logging (Rule 1: Always log the script)
# -----------------------------------------------------------------------------
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LOGS_DIR = os.path.join(SCRIPT_DIR, 'logs')
os.makedirs(LOGS_DIR, exist_ok=True)
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
LOG_PATH = os.path.join(LOGS_DIR, f'generate_dashboard_{timestamp}.log')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s [%(filename)s:%(lineno)d] %(message)s',
    handlers=[
        logging.FileHandler(LOG_PATH, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Paths
ROOT_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, '..', '..'))
DUCKDB_PATH = os.path.join(ROOT_DIR, 'database', 'huntington_data_lake.duckdb')
TARGETS_PATH = os.path.join(SCRIPT_DIR, 'targets.json')
OUTPUT_HTML_PATH = os.path.join(SCRIPT_DIR, 'dashboard_venda_direta.html')

MONTH_NAMES_PT = {
    1: 'Janeiro', 2: 'Fevereiro', 3: 'Março', 4: 'Abril',
    5: 'Maio', 6: 'Junho', 7: 'Julho', 8: 'Agosto',
    9: 'Setembro', 10: 'Outubro', 11: 'Novembro', 12: 'Dezembro'
}


def get_duckdb_connection(db_path, read_only=True, max_retries=10, retry_delay=1.5):
    """Safely connects to DuckDB with retries to handle transient cloud sync file locks."""
    for attempt in range(1, max_retries + 1):
        try:
            return duckdb.connect(db_path, read_only=read_only)
        except Exception as e:
            if attempt == max_retries:
                logger.error(f"Failed to connect to DuckDB after {max_retries} attempts: {e}")
                raise
            logger.warning(f"DuckDB locked or busy, retrying {attempt}/{max_retries} in {retry_delay}s... ({e})")
            time.sleep(retry_delay)


def load_targets():
    """Loads target configurations from targets.json."""
    if not os.path.exists(TARGETS_PATH):
        logger.warning(f"targets.json not found at {TARGETS_PATH}. Using empty targets.")
        return {'receita': {}, 'ciclos': {}}
    
    with open(TARGETS_PATH, 'r', encoding='utf-8') as f:
        targets = json.load(f)
    logger.info(f"Loaded targets for months: {list(targets.get('receita', {}).keys())}")
    return targets


def fetch_and_aggregate_data():
    """Connects to DuckDB, queries gold.protheus_vendas_consolidadas, and computes aggregates and case details."""
    logger.info(f"Connecting to DuckDB at: {DUCKDB_PATH}")
    con = get_duckdb_connection(DUCKDB_PATH, read_only=True)
    
    # Check table existence
    check = con.execute("""
        SELECT COUNT(*) FROM information_schema.tables 
        WHERE table_schema = 'gold' AND table_name = 'protheus_vendas_consolidadas'
    """).fetchone()[0]
    if check == 0:
        con.close()
        raise RuntimeError("Table gold.protheus_vendas_consolidadas does not exist in DuckDB.")

    # Get available months
    months_query = """
        SELECT DISTINCT ano, mes
        FROM gold.protheus_vendas_consolidadas
        WHERE ano >= 2024
        ORDER BY ano DESC, mes DESC
    """
    available_months = con.execute(months_query).fetchall()
    logger.info(f"Found {len(available_months)} available monthly periods in data lake.")

    dataset_by_month = {}

    for ano, mes in available_months:
        month_key = f"{ano}-{mes:02d}"
        month_label = f"{MONTH_NAMES_PT.get(mes, '')} {ano}"
        
        # Max day for MTD comparison
        max_day = con.execute(f"""
            SELECT MAX(DAY(dt_emissao))
            FROM gold.protheus_vendas_consolidadas
            WHERE ano = {ano} AND mes = {mes}
        """).fetchone()[0]
        max_day = max_day if max_day is not None else 31

        # Current period aggregation
        cur_sql = f"""
            SELECT 
                CASE 
                    WHEN unidade = 'Pro Fiv' THEN 'ProFIV'
                    ELSE unidade 
                END AS unidade,
                SUM(CASE WHEN status_fluxo = 'FATURADO_DIRETO' THEN valor_total ELSE 0 END) as faturado_direto,
                SUM(CASE WHEN status_fluxo = 'FATURADO_VIA_PEDIDO' THEN valor_total ELSE 0 END) as faturado_pedido,
                SUM(CASE WHEN status_fluxo = 'PEDIDO_A_FATURAR' THEN valor_total ELSE 0 END) as pedidos_abertos,
                SUM(CASE WHEN status_fluxo IN ('FATURADO_DIRETO', 'FATURADO_VIA_PEDIDO', 'PEDIDO_A_FATURAR') THEN valor_total ELSE 0 END) as realizado,
                COUNT(*) as rows_count
            FROM gold.protheus_vendas_consolidadas
            WHERE ano = {ano} AND mes = {mes}
            GROUP BY 1
            ORDER BY realizado DESC
        """
        df_cur = con.execute(cur_sql).df()

        # LY MTD aggregation (Same month last year, day <= max_day)
        ano_ly = ano - 1
        ly_sql = f"""
            SELECT 
                CASE 
                    WHEN unidade = 'Pro Fiv' THEN 'ProFIV'
                    ELSE unidade 
                END AS unidade,
                SUM(CASE WHEN status_fluxo IN ('FATURADO_DIRETO', 'FATURADO_VIA_PEDIDO', 'PEDIDO_A_FATURAR') THEN valor_total ELSE 0 END) as mtd_ly
            FROM gold.protheus_vendas_consolidadas
            WHERE ano = {ano_ly} AND mes = {mes} AND DAY(dt_emissao) <= {max_day}
            GROUP BY 1
        """
        df_ly = con.execute(ly_sql).df()
        ly_map = dict(zip(df_ly['unidade'], df_ly['mtd_ly']))

        units_list = []
        tot_faturado_direto = 0.0
        tot_faturado_pedido = 0.0
        tot_pedidos_abertos = 0.0
        tot_realizado = 0.0
        tot_mtd_ly = 0.0
        has_any_ly_data = False

        for _, row in df_cur.iterrows():
            u_name = row['unidade']
            fat_dir = float(row['faturado_direto'])
            fat_ped = float(row['faturado_pedido'])
            ped_ab = float(row['pedidos_abertos'])
            realizado = float(row['realizado'])
            mtd_ly_val = ly_map.get(u_name, None)
            
            if mtd_ly_val is not None and mtd_ly_val > 0:
                has_any_ly_data = True
                tot_mtd_ly += float(mtd_ly_val)
                pct_vs_ly = round(((realizado - float(mtd_ly_val)) / float(mtd_ly_val)) * 100, 1)
            else:
                mtd_ly_val = None
                pct_vs_ly = None

            tot_faturado_direto += fat_dir
            tot_faturado_pedido += fat_ped
            tot_pedidos_abertos += ped_ab
            tot_realizado += realizado

            units_list.append({
                'unidade': u_name,
                'faturado_direto': round(fat_dir, 2),
                'faturado_pedido': round(fat_ped, 2),
                'pedidos_abertos': round(ped_ab, 2),
                'realizado': round(realizado, 2),
                'mtd_ly': round(float(mtd_ly_val), 2) if mtd_ly_val is not None else None,
                'pct_vs_ly': pct_vs_ly
            })

        # Summary for Brasil
        pct_vs_ly_total = None
        if has_any_ly_data and tot_mtd_ly > 0:
            pct_vs_ly_total = round(((tot_realizado - tot_mtd_ly) / tot_mtd_ly) * 100, 1)

        total_brasil = {
            'unidade': 'Brasil',
            'faturado_direto': round(tot_faturado_direto, 2),
            'faturado_pedido': round(tot_faturado_pedido, 2),
            'pedidos_abertos': round(tot_pedidos_abertos, 2),
            'realizado': round(tot_realizado, 2),
            'mtd_ly': round(tot_mtd_ly, 2) if has_any_ly_data else None,
            'pct_vs_ly': pct_vs_ly_total
        }

        dataset_by_month[month_key] = {
            'month_key': month_key,
            'month_label': month_label,
            'short_name': MONTH_NAMES_PT.get(mes, ''),
            'ano': ano,
            'mes': mes,
            'max_day': int(max_day),
            'total_brasil': total_brasil,
            'units': units_list
        }

    # Extract granular case items with distinct orcamento, pedido, nota, and reference dates
    logger.info("Extracting granular case items for operational drilldown (all 2026 + all open orders)...")
    cases_query = """
        SELECT 
            strftime(dt_emissao, '%Y-%m') as mes_key,
            CASE 
                WHEN unidade = 'Pro Fiv' THEN 'ProFIV'
                ELSE unidade 
            END AS unidade,
            status_fluxo,
            strftime(dt_emissao, '%d/%m/%Y') as dt_ref,
            CASE 
                WHEN dt_emissao = dt_pedido THEN 'Pedido'
                WHEN dt_emissao = dt_nota THEN 'Nota'
                WHEN dt_emissao = dt_orcamento THEN 'Orçamento'
                WHEN status_fluxo IN ('PEDIDO_A_FATURAR', 'FATURADO_VIA_PEDIDO') THEN 'Pedido'
                WHEN status_fluxo = 'FATURADO_DIRETO' THEN 'Nota'
                ELSE 'Emissão'
            END as tipo_dt,
            COALESCE(orcamento, '') as orcamento,
            COALESCE(strftime(dt_orcamento, '%d/%m/%Y'), '') as dt_orc,
            COALESCE(pedido, '') as pedido,
            COALESCE(strftime(dt_pedido, '%d/%m/%Y'), '') as dt_ped,
            COALESCE(num_nota, '') as num_nota,
            COALESCE(strftime(dt_nota, '%d/%m/%Y'), '') as dt_nota,
            COALESCE(prontuario, 0) as prontuario,
            COALESCE(nome_paciente, nome_cliente, 'Não informado') as paciente,
            COALESCE(nome_medico, '-') as medico,
            COALESCE(descricao_produto, '-') as produto,
            ROUND(valor_total, 2) as valor
        FROM gold.protheus_vendas_consolidadas
        WHERE status_fluxo IN ('FATURADO_DIRETO', 'FATURADO_VIA_PEDIDO', 'PEDIDO_A_FATURAR')
          AND (
              ano = 2026
              OR status_fluxo = 'PEDIDO_A_FATURAR'
              OR (status_fluxo = 'FATURADO_VIA_PEDIDO' AND ano >= 2024)
          )
        ORDER BY dt_emissao DESC, valor_total DESC
    """
    cases_rows = con.execute(cases_query).fetchall()
    logger.info(f"Extracted {len(cases_rows):,} granular case rows.")

    cases_dataset = {}
    for r in cases_rows:
        m, u, st, dt_ref, tipo_dt, orc, dt_orc, ped, dt_ped, nota, dt_nota, pront, pac, med, prod, val = r
        if m not in cases_dataset:
            cases_dataset[m] = {}
        if u not in cases_dataset[m]:
            cases_dataset[m][u] = {
                'FATURADO_DIRETO': [],
                'FATURADO_VIA_PEDIDO': [],
                'PEDIDO_A_FATURAR': []
            }
        pront_str = str(int(pront)) if (pront and pront > 0) else ''
        cases_dataset[m][u][st].append([
            dt_ref, tipo_dt, orc, dt_orc, ped, dt_ped, nota, dt_nota, pront_str, pac, med, prod, float(val)
        ])

    con.close()
    logger.info("DuckDB query extraction completed successfully.")
    return dataset_by_month, cases_dataset


def build_mockup_dataset():
    """Provides the exact mockup figures from the user's uploaded reference image."""
    mock_units = [
        {'unidade': 'Campinas', 'meta': None, 'realizado': 310000.0, 'mtd_ly': 285000.0, 'pct_vs_ly': 9.0, 'faturado_direto': 130000.0, 'faturado_pedido': 95000.0, 'pedidos_abertos': 85000.0},
        {'unidade': 'ProFIV', 'meta': None, 'realizado': 225000.0, 'mtd_ly': 210000.0, 'pct_vs_ly': 7.0, 'faturado_direto': 95000.0, 'faturado_pedido': 70000.0, 'pedidos_abertos': 60000.0},
        {'unidade': 'Vila Mariana', 'meta': None, 'realizado': 405000.0, 'mtd_ly': 380000.0, 'pct_vs_ly': 7.0, 'faturado_direto': 175000.0, 'faturado_pedido': 120000.0, 'pedidos_abertos': 110000.0},
        {'unidade': 'Ibirapuera', 'meta': None, 'realizado': 350000.0, 'mtd_ly': 330000.0, 'pct_vs_ly': 6.0, 'faturado_direto': 150000.0, 'faturado_pedido': 105000.0, 'pedidos_abertos': 95000.0},
        {'unidade': 'FIV Brasilia', 'meta': None, 'realizado': 275000.0, 'mtd_ly': 250000.0, 'pct_vs_ly': 10.0, 'faturado_direto': 120000.0, 'faturado_pedido': 85000.0, 'pedidos_abertos': 70000.0},
        {'unidade': 'Salvador - Cenafert', 'meta': None, 'realizado': 190000.0, 'mtd_ly': 170000.0, 'pct_vs_ly': 12.0, 'faturado_direto': 80000.0, 'faturado_pedido': 60000.0, 'pedidos_abertos': 50000.0},
        {'unidade': 'Belo Horizonte', 'meta': None, 'realizado': 210000.0, 'mtd_ly': 190000.0, 'pct_vs_ly': 11.0, 'faturado_direto': 90000.0, 'faturado_pedido': 65000.0, 'pedidos_abertos': 55000.0}
    ]
    mock_brasil = {
        'unidade': 'Brasil', 'meta': None, 'realizado': 1965000.0, 'mtd_ly': 1815000.0, 'pct_vs_ly': 8.0,
        'faturado_direto': 840000.0, 'faturado_pedido': 600000.0, 'pedidos_abertos': 525000.0
    }
    return {
        'mockup': {
            'month_key': 'mockup',
            'month_label': 'Mockup Original',
            'short_name': 'Setembro',
            'ano': 2026,
            'mes': 9,
            'max_day': 30,
            'total_brasil': mock_brasil,
            'units': mock_units
        }
    }


def generate_html(data_lake_dataset, cases_dataset, targets):
    """Generates the modern, interactive HTML matching the user design specification."""
    logger.info("Generating standalone HTML dashboard...")

    mockup_dataset = build_mockup_dataset()

    html_template = """<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Dashboard Financeiro — Venda Direta</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap" rel="stylesheet">
    <style>
        * {
            box-sizing: border-box;
            margin: 0;
            padding: 0;
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
        }

        body {
            background-color: #f8f9fa;
            color: #1f2937;
            padding: 32px 48px;
            min-height: 100vh;
        }

        .container {
            max-width: 1400px;
            margin: 0 auto;
        }

        /* Top Header */
        .header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 24px;
        }

        .header-title {
            font-size: 20px;
            font-weight: 700;
            color: #111827;
            letter-spacing: -0.02em;
        }

        .header-controls {
            display: flex;
            align-items: center;
            gap: 12px;
        }

        .toggle-btn {
            font-size: 12px;
            padding: 6px 12px;
            border-radius: 4px;
            border: 1px solid #d1d5db;
            background: #ffffff;
            cursor: pointer;
            color: #4b5563;
            transition: all 0.15s ease;
        }
        .toggle-btn.active {
            background: #111827;
            color: #ffffff;
            border-color: #111827;
        }

        .unit-select {
            padding: 6px 36px 6px 12px;
            font-size: 13px;
            font-weight: 500;
            color: #1f2937;
            background-color: #ffffff;
            border: 1px solid #9ca3af;
            border-radius: 4px;
            cursor: pointer;
            outline: none;
            appearance: none;
            background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' fill='none' viewBox='0 0 24 24' stroke='%234b5563'%3E%3Cpath stroke-linecap='round' stroke-linejoin='round' stroke-width='2' d='M19 9l-7 7-7-7'%3E%3C/path%3E%3C/svg%3E");
            background-repeat: no-repeat;
            background-position: right 8px center;
            background-size: 16px;
        }

        .unit-select:focus {
            border-color: #2563eb;
            box-shadow: 0 0 0 1px #2563eb;
        }

        .month-select {
            padding: 5px 10px;
            font-size: 12px;
            font-weight: 500;
            color: #e05252;
            background-color: #ffffff;
            border: 1px solid #e05252;
            border-radius: 4px;
            cursor: pointer;
            outline: none;
        }

        /* Top Metric Cards */
        .cards-grid {
            display: grid;
            grid-template-columns: 420px 1fr;
            gap: 24px;
            margin-bottom: 32px;
        }

        .card {
            background: #ffffff;
            border-radius: 6px;
            padding: 22px 24px;
            box-shadow: 0 1px 3px rgba(0, 0, 0, 0.04), 0 1px 2px rgba(0, 0, 0, 0.02);
            border: 1px solid #f1f5f9;
        }

        .card-label {
            font-size: 11px;
            font-weight: 700;
            letter-spacing: 0.06em;
            text-transform: uppercase;
            color: #e05252;
            margin-bottom: 12px;
        }

        /* Card 1: VS META */
        .main-kpi-row {
            display: flex;
            align-items: center;
            gap: 12px;
            margin-bottom: 8px;
        }

        .big-kpi {
            font-size: 32px;
            font-weight: 700;
            color: #111827;
            letter-spacing: -0.02em;
        }

        .pct-meta-tag {
            font-size: 13px;
            font-weight: 600;
            padding: 2px 8px;
            border-radius: 4px;
            background: #f1f5f9;
            color: #64748b;
        }

        .pct-meta-tag.has-target {
            background: #eff6ff;
            color: #1d4ed8;
            border: 1px solid #bfdbfe;
        }

        .meta-subtext {
            font-size: 13px;
            color: #64748b;
            margin-bottom: 8px;
        }

        .mtd-ly-row {
            display: flex;
            align-items: center;
            gap: 8px;
            font-size: 12px;
            color: #64748b;
        }

        .badge-ly {
            font-size: 11px;
            font-weight: 700;
            padding: 2px 6px;
            border-radius: 4px;
        }

        .badge-ly.positive {
            background-color: #d1fae5;
            color: #065f46;
        }

        .badge-ly.negative {
            background-color: #fee2e2;
            color: #b91c1c;
        }

        .badge-ly.neutral {
            background-color: #f1f5f9;
            color: #94a3b8;
        }

        /* Card 2: Composição */
        .comp-grid {
            display: grid;
            grid-template-columns: repeat(3, 1fr);
            gap: 20px;
            align-items: center;
            height: calc(100% - 24px);
        }

        .comp-col {
            padding-left: 20px;
            border-left: 1px solid #e2e8f0;
            cursor: pointer;
            transition: transform 0.15s ease, background-color 0.15s ease;
            padding-top: 6px;
            padding-bottom: 6px;
            border-radius: 4px;
        }

        .comp-col:hover {
            background-color: #f8fafc;
        }

        .comp-col:first-child {
            padding-left: 8px;
            border-left: none;
        }

        .comp-sublabel {
            font-size: 11px;
            font-weight: 600;
            color: #94a3b8;
            letter-spacing: 0.05em;
            text-transform: uppercase;
            margin-bottom: 6px;
        }

        .comp-val {
            font-size: 22px;
            font-weight: 700;
            color: #111827;
            margin-bottom: 4px;
        }

        .comp-share {
            font-size: 12px;
            color: #94a3b8;
        }

        /* Section Table */
        .section-header {
            margin-bottom: 12px;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }

        .section-title {
            font-size: 11px;
            font-weight: 700;
            letter-spacing: 0.06em;
            text-transform: uppercase;
            color: #e05252;
        }

        .table-hint {
            font-size: 11px;
            color: #6b7280;
            font-style: italic;
        }

        .table-container {
            background: #ffffff;
            border-radius: 6px;
            box-shadow: 0 1px 3px rgba(0, 0, 0, 0.04);
            border: 1px solid #f1f5f9;
            overflow-x: auto;
        }

        table {
            width: 100%;
            border-collapse: collapse;
            text-align: left;
        }

        th {
            font-size: 11px;
            font-weight: 700;
            letter-spacing: 0.05em;
            text-transform: uppercase;
            color: #4b5563;
            padding: 14px 18px;
            border-bottom: 1px solid #e2e8f0;
            white-space: nowrap;
        }

        td {
            font-size: 13px;
            color: #1f2937;
            padding: 14px 18px;
            border-bottom: 1px solid #f1f5f9;
            white-space: nowrap;
        }

        tr:hover td {
            background-color: #fafbfc;
        }

        tr.highlighted td {
            background-color: #fef2f2 !important;
            border-color: #fecaca;
        }

        .text-right {
            text-align: right;
        }

        .text-center {
            text-align: center;
        }

        .font-medium {
            font-weight: 500;
        }

        .font-semibold {
            font-weight: 600;
        }

        .font-bold {
            font-weight: 700;
        }

        .total-row td {
            font-weight: 700;
            color: #111827;
            border-top: 2px solid #cbd5e1;
            border-bottom: none;
            background-color: #ffffff;
        }

        .pointer-cell {
            cursor: pointer;
            transition: background-color 0.15s ease;
        }
        .pointer-cell:hover {
            text-decoration: underline;
            color: #1d4ed8 !important;
        }

        .muted {
            color: #94a3b8;
        }

        /* ------------------------------------------------------------- */
        /* Cases Drilldown Section                                       */
        /* ------------------------------------------------------------- */
        .cases-section {
            margin-top: 36px;
            background: #ffffff;
            border-radius: 6px;
            box-shadow: 0 1px 3px rgba(0, 0, 0, 0.04), 0 1px 2px rgba(0, 0, 0, 0.02);
            border: 1px solid #f1f5f9;
            padding: 24px;
        }

        .cases-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 20px;
            flex-wrap: wrap;
            gap: 16px;
        }

        .cases-title-group {
            display: flex;
            flex-direction: column;
            gap: 4px;
        }

        .cases-subtitle {
            font-size: 11px;
            font-weight: 700;
            letter-spacing: 0.06em;
            text-transform: uppercase;
            color: #e05252;
        }

        .cases-title {
            font-size: 18px;
            font-weight: 700;
            color: #111827;
            display: flex;
            align-items: center;
            gap: 8px;
        }

        .cases-controls {
            display: flex;
            align-items: center;
            gap: 12px;
            flex-wrap: wrap;
        }

        .cases-search-box {
            position: relative;
            width: 320px;
        }

        .cases-search-input {
            width: 100%;
            padding: 7px 12px 7px 34px;
            font-size: 12px;
            border: 1px solid #d1d5db;
            border-radius: 4px;
            outline: none;
            color: #1f2937;
            background: #ffffff;
            transition: all 0.15s ease;
        }

        .cases-search-input:focus {
            border-color: #2563eb;
            box-shadow: 0 0 0 1px #2563eb;
        }

        .cases-search-icon {
            position: absolute;
            left: 10px;
            top: 50%;
            transform: translateY(-50%);
            width: 15px;
            height: 15px;
            color: #9ca3af;
            pointer-events: none;
        }

        .btn-export-csv {
            display: inline-flex;
            align-items: center;
            gap: 6px;
            padding: 7px 14px;
            background-color: #ffffff;
            border: 1px solid #d1d5db;
            border-radius: 4px;
            font-size: 12px;
            font-weight: 600;
            color: #374151;
            cursor: pointer;
            transition: all 0.15s ease;
        }

        .btn-export-csv:hover {
            background-color: #f3f4f6;
            border-color: #9ca3af;
            color: #111827;
        }

        /* Category Tabs */
        .category-tabs {
            display: flex;
            gap: 10px;
            margin-bottom: 16px;
            border-bottom: 1px solid #e5e7eb;
            padding-bottom: 12px;
            overflow-x: auto;
        }

        .cat-tab-btn {
            display: inline-flex;
            align-items: center;
            gap: 8px;
            padding: 8px 16px;
            border-radius: 6px;
            border: 1px solid #e5e7eb;
            background: #f8fafc;
            font-size: 13px;
            font-weight: 600;
            color: #475569;
            cursor: pointer;
            transition: all 0.15s ease;
            white-space: nowrap;
        }

        .cat-tab-btn:hover {
            background: #f1f5f9;
            color: #1e293b;
        }

        .cat-tab-btn.active {
            background: #ffffff;
            border-color: #2563eb;
            color: #1d4ed8;
            box-shadow: 0 1px 3px rgba(37, 99, 235, 0.12);
        }

        .cat-tab-btn.tab-pending.active {
            border-color: #d97706;
            color: #b45309;
            box-shadow: 0 1px 3px rgba(217, 119, 6, 0.15);
        }

        .cat-tab-tag {
            font-size: 11px;
            padding: 2px 8px;
            border-radius: 12px;
            background: #e2e8f0;
            color: #475569;
            font-weight: 700;
        }

        .cat-tab-btn.active .cat-tab-tag {
            background: #dbeafe;
            color: #1e40af;
        }

        .cat-tab-btn.tab-pending.active .cat-tab-tag {
            background: #fef3c7;
            color: #92400e;
        }

        /* Cases Table */
        .cases-table-wrap {
            overflow-x: auto;
            border: 1px solid #f1f5f9;
            border-radius: 4px;
        }

        .cases-table {
            width: 100%;
            border-collapse: collapse;
            font-size: 12px;
        }

        .cases-table th {
            padding: 10px 14px;
            font-size: 11px;
            font-weight: 700;
            letter-spacing: 0.05em;
            text-transform: uppercase;
            color: #4b5563;
            background: #f8fafc;
            border-bottom: 1px solid #e2e8f0;
            white-space: nowrap;
        }

        .cases-table td {
            padding: 10px 14px;
            font-size: 12px;
            color: #1f2937;
            border-bottom: 1px solid #f1f5f9;
            white-space: nowrap;
        }

        .cases-table tr:hover td {
            background-color: #f8fafc;
        }

        /* Pagination & Stats */
        .cases-pagination-bar {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-top: 14px;
            font-size: 12px;
            color: #64748b;
            flex-wrap: wrap;
            gap: 12px;
        }

        .pagination-controls {
            display: flex;
            align-items: center;
            gap: 8px;
        }

        .btn-page {
            padding: 4px 10px;
            font-size: 12px;
            border: 1px solid #d1d5db;
            border-radius: 4px;
            background: #ffffff;
            cursor: pointer;
            color: #374151;
            transition: all 0.1s ease;
        }

        .btn-page:hover:not(:disabled) {
            background: #f3f4f6;
            border-color: #9ca3af;
        }

        .btn-page:disabled {
            opacity: 0.5;
            cursor: not-allowed;
        }

        .tag-doc {
            font-family: monospace;
            font-size: 11px;
            background: #f1f5f9;
            padding: 2px 6px;
            border-radius: 3px;
            color: #334155;
        }

        .tag-unit-badge {
            font-size: 10px;
            font-weight: 600;
            padding: 2px 6px;
            border-radius: 3px;
            background: #e0e7ff;
            color: #3730a3;
            margin-right: 4px;
        }

        .badge-date-type {
            display: inline-block;
            font-size: 9px;
            font-weight: 700;
            letter-spacing: 0.04em;
            text-transform: uppercase;
            padding: 1px 5px;
            border-radius: 3px;
            background: #f1f5f9;
            color: #64748b;
            margin-top: 2px;
            white-space: nowrap;
        }

        .badge-date-type.pedido {
            background: #fef3c7;
            color: #92400e;
        }

        .badge-date-type.nota {
            background: #dbeafe;
            color: #1e40af;
        }

        .badge-date-type.orcamento {
            background: #f3e8ff;
            color: #6b21a8;
        }

        .sub-date {
            font-size: 10px;
            color: #94a3b8;
            margin-top: 1px;
            white-space: nowrap;
        }

        .tag-pending-nf {
            display: inline-block;
            font-size: 10px;
            font-weight: 600;
            padding: 2px 6px;
            border-radius: 3px;
            background: #fffbeb;
            color: #b45309;
            border: 1px dashed #f59e0b;
            white-space: nowrap;
        }

        .doc-cell {
            display: flex;
            flex-direction: column;
            gap: 1px;
        }

        .cases-empty-state {
            padding: 48px 24px;
            text-align: center;
            color: #94a3b8;
            font-size: 13px;
        }

        /* Footer & Info */
        .footer-note {
            margin-top: 32px;
            display: flex;
            justify-content: space-between;
            align-items: center;
            font-size: 11px;
            color: #94a3b8;
        }

        .footer-badges {
            display: flex;
            gap: 8px;
        }

        .pill-badge {
            background: #e2e8f0;
            color: #475569;
            padding: 2px 8px;
            border-radius: 4px;
            font-size: 10px;
            font-weight: 600;
        }
    </style>
</head>
<body>
    <div class="container">
        <!-- Header -->
        <header class="header">
            <h1 class="header-title">Dashboard Financeiro — Venda Direta</h1>
            <div class="header-controls">
                <button id="toggleDataBtn" class="toggle-btn active" onclick="toggleDataSource('lake')" title="Exibir dados do Lake ou Mockup">
                    ● Dados Reais (Data Lake)
                </button>
                <select id="monthSelect" class="month-select" onchange="onMonthChange()">
                    <!-- Populated dynamically -->
                </select>
                <select id="unitSelect" class="unit-select" onchange="onUnitChange()">
                    <option value="Todas">Todas as Unidades</option>
                    <option value="Campinas">Campinas</option>
                    <option value="ProFIV">ProFIV</option>
                    <option value="Vila Mariana">Vila Mariana</option>
                    <option value="Ibirapuera">Ibirapuera</option>
                    <option value="FIV Brasilia">FIV Brasilia</option>
                    <option value="Salvador - Cenafert">Salvador - Cenafert</option>
                    <option value="Belo Horizonte">Belo Horizonte</option>
                    <option value="Rio de Janeiro">Rio de Janeiro</option>
                </select>
            </div>
        </header>

        <!-- KPI Cards Grid -->
        <div class="cards-grid">
            <!-- Card 1: Unidade / Brasil vs Meta -->
            <div class="card">
                <div class="card-label" id="card1Label">BRASIL VS. META</div>
                <div class="main-kpi-row">
                    <span class="big-kpi" id="kpiRealizado">R$ 0</span>
                    <span class="pct-meta-tag" id="kpiPctMeta">—</span>
                </div>
                <div class="meta-subtext" id="kpiMeta">Meta: A definir</div>
                <div class="mtd-ly-row">
                    <span id="kpiMtdLy">MTD LY: R$ 0</span>
                    <span class="badge-ly" id="kpiPctVsLy">—</span>
                </div>
            </div>

            <!-- Card 2: Composição do Realizado -->
            <div class="card">
                <div class="card-label">COMPOSIÇÃO DO REALIZADO (CLIQUE PARA FILTRAR)</div>
                <div class="comp-grid">
                    <div class="comp-col" onclick="selectActiveCategoryAndScroll('FATURADO_DIRETO')" title="Filtrar casos Faturado Direto">
                        <div class="comp-sublabel">FATURADO DIRETO</div>
                        <div class="comp-val" id="compFatDireto">R$ 0</div>
                        <div class="comp-share" id="compFatDiretoPct">0% do total</div>
                    </div>
                    <div class="comp-col" onclick="selectActiveCategoryAndScroll('FATURADO_VIA_PEDIDO')" title="Filtrar casos Faturado c/ Pedido">
                        <div class="comp-sublabel">FATURADO C/ PEDIDO</div>
                        <div class="comp-val" id="compFatPedido">R$ 0</div>
                        <div class="comp-share" id="compFatPedidoPct">0% do total</div>
                    </div>
                    <div class="comp-col" onclick="selectActiveCategoryAndScroll('PEDIDO_A_FATURAR')" title="Filtrar Pedidos Abertos (a faturar)">
                        <div class="comp-sublabel">PEDIDOS ABERTOS</div>
                        <div class="comp-val" id="compPedAbertos" style="color: #b45309;">R$ 0</div>
                        <div class="comp-share" id="compPedAbertosPct">0% do total</div>
                    </div>
                </div>
            </div>
        </div>

        <!-- Section: Por Unidade -->
        <div class="section-header">
            <h2 class="section-title">POR UNIDADE</h2>
            <span class="table-hint">💡 Clique na linha da unidade ou na coluna desejada para abrir os casos detalhados abaixo</span>
        </div>

        <!-- Table -->
        <div class="table-container">
            <table>
                <thead>
                    <tr>
                        <th style="width: 20%;">UNIDADE</th>
                        <th class="text-right" style="width: 10%;">META</th>
                        <th class="text-right" style="width: 12%;">REALIZADO</th>
                        <th class="text-center" style="width: 8%;">% META</th>
                        <th class="text-right" style="width: 11%;">MTD LY</th>
                        <th class="text-center" style="width: 8%;">% VS LY</th>
                        <th class="text-right" style="width: 11%;" title="Clique no valor de uma unidade para abrir seus casos diretos">FATURADO DIRETO ↗</th>
                        <th class="text-right" style="width: 11%;" title="Clique no valor de uma unidade para abrir seus pedidos faturados">FATURADO C/ PEDIDO ↗</th>
                        <th class="text-right" style="width: 11%;" title="Clique no valor de uma unidade para abrir seus pedidos pendentes de faturamento">PEDIDOS ABERTOS ↗</th>
                    </tr>
                </thead>
                <tbody id="tableBody">
                    <!-- Populated dynamically -->
                </tbody>
                <tfoot id="tableFoot">
                    <!-- Total row populated dynamically -->
                </tfoot>
            </table>
        </div>

        <!-- Section: Detalhamento de Casos (Operational Drilldown) -->
        <div id="casesDrilldownSection" class="cases-section">
            <div class="cases-header">
                <div class="cases-title-group">
                    <span class="cases-subtitle">DETALHAMENTO OPERACIONAL</span>
                    <h3 class="cases-title" id="casesSectionTitle">
                        Casos — Todas as Unidades
                    </h3>
                </div>
                <div class="cases-controls">
                    <div class="cases-search-box">
                        <svg class="cases-search-icon" viewBox="0 0 20 20" fill="currentColor">
                            <path fill-rule="evenodd" d="M8 4a4 4 0 100 8 4 4 0 000-8zM2 8a6 6 0 1110.89 3.476l4.817 4.817a1 1 0 01-1.414 1.414l-4.816-4.816A6 6 0 012 8z" clip-rule="evenodd" />
                        </svg>
                        <input type="text" id="caseSearchInput" class="cases-search-input" placeholder="Buscar por paciente, doc, pedido, médico..." oninput="onCaseSearchChange()">
                    </div>
                    <button class="btn-export-csv" onclick="exportCasesCSV()" title="Exportar casos visíveis para planilha CSV">
                        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                            <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"></path>
                            <polyline points="7 10 12 15 17 10"></polyline>
                            <line x1="12" y1="15" x2="12" y2="3"></line>
                        </svg>
                        Exportar CSV
                    </button>
                </div>
            </div>

            <!-- Category Tabs Switcher -->
            <div class="category-tabs">
                <button id="tabFatDireto" class="cat-tab-btn" onclick="setCasesCategory('FATURADO_DIRETO')">
                    📋 Faturado Direto
                    <span id="tagCountFatDireto" class="cat-tab-tag">0</span>
                </button>
                <button id="tabFatPedido" class="cat-tab-btn" onclick="setCasesCategory('FATURADO_VIA_PEDIDO')">
                    📦 Faturado c/ Pedido
                    <span id="tagCountFatPedido" class="cat-tab-tag">0</span>
                </button>
                <button id="tabPedAbertos" class="cat-tab-btn tab-pending active" onclick="setCasesCategory('PEDIDO_A_FATURAR')">
                    ⏳ Pedidos Abertos (A Faturar)
                    <span id="tagCountPedAbertos" class="cat-tab-tag">0</span>
                </button>
            </div>

            <!-- Cases Table -->
            <div class="cases-table-wrap">
                <table class="cases-table">
                    <thead>
                        <tr>
                            <th style="width: 95px;" title="Data de competência que posiciona a venda/pedido no mês">COMPETÊNCIA ℹ️</th>
                            <th id="thUnit" style="width: 110px;">UNIDADE</th>
                            <th style="width: 105px;">ORÇAMENTO</th>
                            <th style="width: 105px;">PEDIDO</th>
                            <th style="width: 110px;">NOTA FISCAL</th>
                            <th style="width: 80px;">PRONT.</th>
                            <th style="width: 190px;">PACIENTE</th>
                            <th style="width: 150px;">MÉDICO</th>
                            <th>PROCEDIMENTO / PRODUTO</th>
                            <th class="text-right" style="width: 105px;">VALOR</th>
                        </tr>
                    </thead>
                    <tbody id="casesTableBody">
                        <!-- Populated dynamically -->
                    </tbody>
                </table>
            </div>

            <!-- Pagination Bar -->
            <div class="cases-pagination-bar">
                <div id="casesSummaryText">
                    Carregando casos...
                </div>
                <div class="pagination-controls">
                    <button id="btnPrevPage" class="btn-page" onclick="changeCasePage(-1)" disabled>❮ Anterior</button>
                    <span id="casesPageIndicator" style="font-weight: 500;">Página 1 de 1</span>
                    <button id="btnNextPage" class="btn-page" onclick="changeCasePage(1)" disabled>Próxima ❯</button>
                </div>
            </div>
        </div>

        <!-- Footer -->
        <div class="footer-note">
            <div>Fonte: huntington_data_lake.gold.protheus_vendas_consolidadas | Pipeline: 03_silver_to_gold.py</div>
            <div class="footer-badges">
                <span class="pill-badge" id="dataStatusBadge">LIVE LAKE</span>
                <span class="pill-badge" id="dataCutoffBadge">MTD Cutoff: Dia 11</span>
            </div>
        </div>
    </div>

    <!-- Script Payload -->
    <script>
        const LAKE_DATA = """ + json.dumps(data_lake_dataset, ensure_ascii=False) + """;
        const CASES_DATA = """ + json.dumps(cases_dataset, ensure_ascii=False) + """;
        const MOCKUP_DATA = """ + json.dumps(mockup_dataset, ensure_ascii=False) + """;
        const TARGETS = """ + json.dumps(targets, ensure_ascii=False) + """;

        let currentSource = 'lake'; // 'lake' or 'mockup'
        let currentMonthKey = '';
        let selectedUnit = 'Todas';

        // Drilldown state
        let currentCasesCategory = 'PEDIDO_A_FATURAR'; // default to pending orders to follow up
        let currentCasePage = 1;
        const CASES_PAGE_SIZE = 20;
        let caseSearchTerm = '';

        // Category labels mapping
        const CATEGORY_NAMES = {
            'FATURADO_DIRETO': 'Faturado Direto',
            'FATURADO_VIA_PEDIDO': 'Faturado c/ Pedido',
            'PEDIDO_A_FATURAR': 'Pedidos Abertos (A Faturar)'
        };

        // Currency formatter for BRL
        function formatBRL(val) {
            if (val === null || val === undefined || isNaN(val)) return '—';
            const num = Number(val);
            return 'R$ ' + Math.round(num).toLocaleString('pt-BR');
        }

        function formatBRLPrecise(val) {
            if (val === null || val === undefined || isNaN(val)) return '—';
            const num = Number(val);
            return 'R$ ' + num.toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
        }

        function formatPct(val) {
            if (val === null || val === undefined || isNaN(val)) return '—';
            const num = Number(val);
            const sign = num > 0 ? '+' : '';
            return `${sign}${Math.round(num)}%`;
        }

        function getTarget(monthKey, unitName) {
            if (currentSource === 'mockup') return null;
            const recTargets = TARGETS?.receita?.[monthKey] || {};
            
            if (recTargets[unitName] !== undefined) return recTargets[unitName];
            if (unitName === 'FIV Brasilia' && recTargets['Brasília'] !== undefined) return recTargets['Brasília'];
            if (unitName === 'Salvador - Cenafert' && recTargets['Salvador'] !== undefined) return recTargets['Salvador'];
            if (unitName === 'Rio de Janeiro' && recTargets['Fertipraxis'] !== undefined) return recTargets['Fertipraxis'];
            if ((unitName === 'Pro Fiv' || unitName === 'ProFIV') && (recTargets['ProFIV'] !== undefined || recTargets['Pro Fiv'] !== undefined)) {
                return recTargets['ProFIV'] || recTargets['Pro Fiv'];
            }
            return null;
        }

        function getActiveDataset() {
            return currentSource === 'lake' ? LAKE_DATA : MOCKUP_DATA;
        }

        function init() {
            const activeData = getActiveDataset();
            const monthSelect = document.getElementById('monthSelect');
            monthSelect.innerHTML = '';

            const keys = Object.keys(activeData);
            if (keys.length === 0) return;

            keys.forEach(k => {
                const opt = document.createElement('option');
                opt.value = k;
                opt.textContent = activeData[k].month_label;
                monthSelect.appendChild(opt);
            });

            currentMonthKey = keys[0];
            monthSelect.value = currentMonthKey;

            renderDashboard();
        }

        function toggleDataSource(src) {
            if (src) {
                currentSource = src;
            } else {
                currentSource = currentSource === 'lake' ? 'mockup' : 'lake';
            }

            const btn = document.getElementById('toggleDataBtn');
            const statusBadge = document.getElementById('dataStatusBadge');

            if (currentSource === 'lake') {
                btn.classList.add('active');
                btn.textContent = '● Dados Reais (Data Lake)';
                statusBadge.textContent = 'LIVE LAKE';
            } else {
                btn.classList.remove('active');
                btn.textContent = '○ Mockup (Design Original)';
                statusBadge.textContent = 'MOCKUP DESIGN';
            }

            init();
        }

        function onMonthChange() {
            currentMonthKey = document.getElementById('monthSelect').value;
            currentCasePage = 1;
            renderDashboard();
        }

        function onUnitChange() {
            selectedUnit = document.getElementById('unitSelect').value;
            currentCasePage = 1;
            renderDashboard();
        }

        function selectUnitFromTable(unitName) {
            selectedUnit = unitName;
            document.getElementById('unitSelect').value = (unitName === 'Brasil') ? 'Todas' : unitName;
            currentCasePage = 1;
            renderDashboard();
            scrollToCases();
        }

        function selectUnitAndCategory(unitName, category) {
            selectedUnit = unitName;
            document.getElementById('unitSelect').value = (unitName === 'Brasil') ? 'Todas' : unitName;
            currentCasesCategory = category;
            currentCasePage = 1;
            renderDashboard();
            scrollToCases();
        }

        function selectActiveCategoryAndScroll(category) {
            currentCasesCategory = category;
            currentCasePage = 1;
            renderCasesDrilldown();
            scrollToCases();
        }

        function setCasesCategory(category) {
            currentCasesCategory = category;
            currentCasePage = 1;
            renderCasesDrilldown();
        }

        function scrollToCases() {
            const el = document.getElementById('casesDrilldownSection');
            if (el) {
                el.scrollIntoView({ behavior: 'smooth', block: 'start' });
            }
        }

        function onCaseSearchChange() {
            caseSearchTerm = document.getElementById('caseSearchInput').value.trim().toLowerCase();
            currentCasePage = 1;
            renderCasesDrilldown();
        }

        function changeCasePage(delta) {
            currentCasePage += delta;
            renderCasesDrilldown();
        }

        // Retrieve raw cases list for current month, unit and category
        function getRawCases(monthKey, unitName, category) {
            if (currentSource === 'mockup') return [];
            const mData = CASES_DATA?.[monthKey];
            if (!mData) return [];

            if (unitName !== 'Todas' && unitName !== 'Brasil') {
                const uData = mData[unitName];
                if (!uData) return [];
                const items = uData[category] || [];
                return items.map(r => ({
                    dt_ref: r[0], tipo_dt: r[1], orc: r[2], dt_orc: r[3],
                    ped: r[4], dt_ped: r[5], nota: r[6], dt_nota: r[7],
                    pront: r[8], pac: r[9], med: r[10], prod: r[11], val: r[12], unidade: unitName
                }));
            } else {
                const all = [];
                for (const u in mData) {
                    const list = mData[u]?.[category] || [];
                    for (let i = 0; i < list.length; i++) {
                        const r = list[i];
                        all.push({
                            dt_ref: r[0], tipo_dt: r[1], orc: r[2], dt_orc: r[3],
                            ped: r[4], dt_ped: r[5], nota: r[6], dt_nota: r[7],
                            pront: r[8], pac: r[9], med: r[10], prod: r[11], val: r[12], unidade: u
                        });
                    }
                }
                return all;
            }
        }

        function renderDashboard() {
            const activeData = getActiveDataset();
            const periodData = activeData[currentMonthKey];
            if (!periodData) return;

            const cutoffBadge = document.getElementById('dataCutoffBadge');
            cutoffBadge.textContent = `MTD Cutoff: Dia ${periodData.max_day}`;

            // 1. Determine active entity for Top Cards
            let entityData = null;
            let entityTarget = null;
            let card1Title = 'BRASIL VS. META';

            if (selectedUnit === 'Todas' || selectedUnit === 'Brasil') {
                entityData = periodData.total_brasil;
                entityTarget = getTarget(currentMonthKey, 'Brasil');
                card1Title = 'BRASIL VS. META';
            } else {
                entityData = periodData.units.find(u => u.unidade === selectedUnit);
                if (!entityData) {
                    entityData = {
                        unidade: selectedUnit,
                        realizado: 0,
                        faturado_direto: 0,
                        faturado_pedido: 0,
                        pedidos_abertos: 0,
                        mtd_ly: null,
                        pct_vs_ly: null
                    };
                }
                entityTarget = getTarget(currentMonthKey, selectedUnit);
                card1Title = `${selectedUnit.toUpperCase()} VS. META`;
            }

            // 2. Render Card 1
            document.getElementById('card1Label').textContent = card1Title;
            document.getElementById('kpiRealizado').textContent = formatBRL(entityData.realizado);

            const pctMetaElem = document.getElementById('kpiPctMeta');
            const metaSubElem = document.getElementById('kpiMeta');

            if (entityTarget !== null && entityTarget > 0) {
                const pct = Math.round((entityData.realizado / entityTarget) * 100);
                pctMetaElem.textContent = `${pct}%`;
                pctMetaElem.className = 'pct-meta-tag has-target';
                metaSubElem.textContent = `Meta: ${formatBRL(entityTarget)}`;
            } else {
                pctMetaElem.textContent = '—';
                pctMetaElem.className = 'pct-meta-tag';
                metaSubElem.textContent = 'Meta: A definir';
            }

            const kpiMtdLyElem = document.getElementById('kpiMtdLy');
            const kpiPctVsLyElem = document.getElementById('kpiPctVsLy');

            if (entityData.mtd_ly !== null && entityData.mtd_ly !== undefined && entityData.mtd_ly > 0) {
                kpiMtdLyElem.textContent = `MTD LY: ${formatBRL(entityData.mtd_ly)}`;
                kpiPctVsLyElem.textContent = formatPct(entityData.pct_vs_ly);
                kpiPctVsLyElem.className = `badge-ly ${entityData.pct_vs_ly >= 0 ? 'positive' : 'negative'}`;
                kpiPctVsLyElem.style.display = 'inline-block';
            } else {
                kpiMtdLyElem.textContent = 'MTD LY: —';
                kpiPctVsLyElem.textContent = '—';
                kpiPctVsLyElem.className = 'badge-ly neutral';
            }

            // 3. Render Card 2: Composition
            const totalRealizado = entityData.realizado > 0 ? entityData.realizado : 1;
            const pctFatDir = Math.round((entityData.faturado_direto / totalRealizado) * 100);
            const pctFatPed = Math.round((entityData.faturado_pedido / totalRealizado) * 100);
            const pctPedAb = Math.round((entityData.pedidos_abertos / totalRealizado) * 100);

            document.getElementById('compFatDireto').textContent = formatBRL(entityData.faturado_direto);
            document.getElementById('compFatDiretoPct').textContent = `${pctFatDir}% do total`;

            document.getElementById('compFatPedido').textContent = formatBRL(entityData.faturado_pedido);
            document.getElementById('compFatPedidoPct').textContent = `${pctFatPed}% do total`;

            document.getElementById('compPedAbertos').textContent = formatBRL(entityData.pedidos_abertos);
            document.getElementById('compPedAbertosPct').textContent = `${pctPedAb}% do total`;

            // 4. Render Table
            const tbody = document.getElementById('tableBody');
            tbody.innerHTML = '';

            periodData.units.forEach(u => {
                const tr = document.createElement('tr');
                const isSelected = (selectedUnit === u.unidade);
                if (isSelected) tr.classList.add('highlighted');
                tr.style.cursor = 'pointer';
                tr.onclick = () => selectUnitFromTable(u.unidade);

                const uTarget = getTarget(currentMonthKey, u.unidade);
                const targetText = uTarget ? formatBRL(uTarget) : 'A definir';
                const pctMetaText = uTarget ? `${Math.round((u.realizado / uTarget) * 100)}%` : '—';
                const mtdLyText = (u.mtd_ly !== null && u.mtd_ly !== undefined && u.mtd_ly > 0) ? formatBRL(u.mtd_ly) : '—';
                
                let pctVsLyBadge = '<span class="muted">—</span>';
                if (u.pct_vs_ly !== null && u.pct_vs_ly !== undefined) {
                    const badgeClass = u.pct_vs_ly >= 0 ? 'positive' : 'negative';
                    pctVsLyBadge = `<span class="badge-ly ${badgeClass}">${formatPct(u.pct_vs_ly)}</span>`;
                }

                tr.innerHTML = `
                    <td class="font-medium">${u.unidade}</td>
                    <td class="text-right ${uTarget ? '' : 'muted'}">${targetText}</td>
                    <td class="text-right font-semibold">${formatBRL(u.realizado)}</td>
                    <td class="text-center font-medium ${uTarget ? 'text-blue-600' : 'muted'}">${pctMetaText}</td>
                    <td class="text-right ${u.mtd_ly ? '' : 'muted'}">${mtdLyText}</td>
                    <td class="text-center">${pctVsLyBadge}</td>
                    <td class="text-right pointer-cell" onclick="event.stopPropagation(); selectUnitAndCategory('${u.unidade}', 'FATURADO_DIRETO');">${formatBRL(u.faturado_direto)}</td>
                    <td class="text-right pointer-cell" onclick="event.stopPropagation(); selectUnitAndCategory('${u.unidade}', 'FATURADO_VIA_PEDIDO');">${formatBRL(u.faturado_pedido)}</td>
                    <td class="text-right pointer-cell" style="font-weight: 600; color: #b45309;" onclick="event.stopPropagation(); selectUnitAndCategory('${u.unidade}', 'PEDIDO_A_FATURAR');">${formatBRL(u.pedidos_abertos)}</td>
                `;
                tbody.appendChild(tr);
            });

            // 5. Render Table Foot (Brasil)
            const tfoot = document.getElementById('tableFoot');
            const b = periodData.total_brasil;
            const bTarget = getTarget(currentMonthKey, 'Brasil');
            const bTargetText = bTarget ? formatBRL(bTarget) : 'A definir';
            const bPctMetaText = bTarget ? `${Math.round((b.realizado / bTarget) * 100)}%` : '—';
            const bMtdLyText = (b.mtd_ly !== null && b.mtd_ly !== undefined && b.mtd_ly > 0) ? formatBRL(b.mtd_ly) : '—';
            
            let bPctVsLyBadge = '<span class="muted">—</span>';
            if (b.pct_vs_ly !== null && b.pct_vs_ly !== undefined) {
                const bBadgeClass = b.pct_vs_ly >= 0 ? 'positive' : 'negative';
                bPctVsLyBadge = `<span class="badge-ly ${bBadgeClass}">${formatPct(b.pct_vs_ly)}</span>`;
            }

            tfoot.innerHTML = `
                <tr class="total-row" style="cursor: pointer;" onclick="selectUnitFromTable('Todas')">
                    <td>Brasil (Consolidado)</td>
                    <td class="text-right ${bTarget ? '' : 'muted'}">${bTargetText}</td>
                    <td class="text-right">${formatBRL(b.realizado)}</td>
                    <td class="text-center ${bTarget ? 'text-blue-600' : 'muted'}">${bPctMetaText}</td>
                    <td class="text-right ${b.mtd_ly ? '' : 'muted'}">${bMtdLyText}</td>
                    <td class="text-center">${bPctVsLyBadge}</td>
                    <td class="text-right pointer-cell" onclick="event.stopPropagation(); selectUnitAndCategory('Todas', 'FATURADO_DIRETO');">${formatBRL(b.faturado_direto)}</td>
                    <td class="text-right pointer-cell" onclick="event.stopPropagation(); selectUnitAndCategory('Todas', 'FATURADO_VIA_PEDIDO');">${formatBRL(b.faturado_pedido)}</td>
                    <td class="text-right pointer-cell" style="font-weight: 700; color: #b45309;" onclick="event.stopPropagation(); selectUnitAndCategory('Todas', 'PEDIDO_A_FATURAR');">${formatBRL(b.pedidos_abertos)}</td>
                </tr>
            `;

            // 6. Render Cases Drilldown
            renderCasesDrilldown();
        }

        function renderCasesDrilldown() {
            const sectionTitle = document.getElementById('casesSectionTitle');
            const unitLabel = (selectedUnit === 'Todas' || selectedUnit === 'Brasil') ? 'Todas as Unidades (Brasil)' : selectedUnit;
            const activeMonthObj = getActiveDataset()[currentMonthKey];
            const monthLabel = activeMonthObj ? activeMonthObj.month_label : currentMonthKey;
            
            sectionTitle.innerHTML = `Casos — <strong>${unitLabel}</strong> <span style="font-size: 14px; font-weight: normal; color: #6b7280; margin-left: 8px;">(${monthLabel})</span>`;

            // Update Tab Badges (Counts & Amounts)
            const countDir = getRawCases(currentMonthKey, selectedUnit, 'FATURADO_DIRETO').length;
            const countPed = getRawCases(currentMonthKey, selectedUnit, 'FATURADO_VIA_PEDIDO').length;
            const countAbe = getRawCases(currentMonthKey, selectedUnit, 'PEDIDO_A_FATURAR').length;

            document.getElementById('tagCountFatDireto').textContent = countDir.toLocaleString('pt-BR');
            document.getElementById('tagCountFatPedido').textContent = countPed.toLocaleString('pt-BR');
            document.getElementById('tagCountPedAbertos').textContent = countAbe.toLocaleString('pt-BR');

            // Highlight Active Tab
            document.querySelectorAll('.cat-tab-btn').forEach(btn => btn.classList.remove('active'));
            if (currentCasesCategory === 'FATURADO_DIRETO') {
                document.getElementById('tabFatDireto').classList.add('active');
            } else if (currentCasesCategory === 'FATURADO_VIA_PEDIDO') {
                document.getElementById('tabFatPedido').classList.add('active');
            } else {
                document.getElementById('tabPedAbertos').classList.add('active');
            }

            // Mockup Notice
            const tbody = document.getElementById('casesTableBody');
            if (currentSource === 'mockup') {
                tbody.innerHTML = `
                    <tr>
                        <td colspan="9" class="cases-empty-state">
                            ℹ️ O detalhamento caso a caso requer a base de dados do Data Lake. Alternar para <strong>"Dados Reais (Data Lake)"</strong> no topo para auditar os pedidos e notas individuais.
                        </td>
                    </tr>
                `;
                document.getElementById('casesSummaryText').textContent = 'Nenhum caso detalhado disponível no modo Mockup.';
                document.getElementById('btnPrevPage').disabled = true;
                document.getElementById('btnNextPage').disabled = true;
                document.getElementById('casesPageIndicator').textContent = 'Página 1 de 1';
                return;
            }

            // Fetch and filter raw cases
            let cases = getRawCases(currentMonthKey, selectedUnit, currentCasesCategory);

            if (caseSearchTerm) {
                cases = cases.filter(c => {
                    return (
                        c.pac.toLowerCase().includes(caseSearchTerm) ||
                        c.orc.toLowerCase().includes(caseSearchTerm) ||
                        c.ped.toLowerCase().includes(caseSearchTerm) ||
                        c.nota.toLowerCase().includes(caseSearchTerm) ||
                        c.med.toLowerCase().includes(caseSearchTerm) ||
                        c.prod.toLowerCase().includes(caseSearchTerm) ||
                        c.unidade.toLowerCase().includes(caseSearchTerm) ||
                        String(c.pront).includes(caseSearchTerm)
                    );
                });
            }

            const totalCount = cases.length;
            const totalSum = cases.reduce((acc, c) => acc + c.val, 0);

            if (totalCount === 0) {
                tbody.innerHTML = `
                    <tr>
                        <td colspan="10" class="cases-empty-state">
                            Nenhum caso encontrado para <strong>${unitLabel}</strong> na categoria <strong>${CATEGORY_NAMES[currentCasesCategory]}</strong>${caseSearchTerm ? ' com o filtro digitado' : ''}.
                        </td>
                    </tr>
                `;
                document.getElementById('casesSummaryText').textContent = '0 casos encontrados';
                document.getElementById('btnPrevPage').disabled = true;
                document.getElementById('btnNextPage').disabled = true;
                document.getElementById('casesPageIndicator').textContent = 'Página 1 de 1';
                return;
            }

            // Pagination calculation
            const totalPages = Math.max(1, Math.ceil(totalCount / CASES_PAGE_SIZE));
            if (currentCasePage > totalPages) currentCasePage = totalPages;
            if (currentCasePage < 1) currentCasePage = 1;

            const startIndex = (currentCasePage - 1) * CASES_PAGE_SIZE;
            const pageItems = cases.slice(startIndex, startIndex + CASES_PAGE_SIZE);

            tbody.innerHTML = '';
            pageItems.forEach(c => {
                const tr = document.createElement('tr');

                let notaContent = '<span class="muted">—</span>';
                if (c.nota) {
                    notaContent = `<div class="doc-cell"><span class="tag-doc">${c.nota}</span>${c.dt_nota ? `<span class="sub-date">${c.dt_nota}</span>` : ''}</div>`;
                } else if (currentCasesCategory === 'PEDIDO_A_FATURAR') {
                    notaContent = `<span class="tag-pending-nf">A Faturar</span>`;
                }

                let pedContent = c.ped ? `<div class="doc-cell"><span class="tag-doc">${c.ped}</span>${c.dt_ped ? `<span class="sub-date">${c.dt_ped}</span>` : ''}</div>` : '<span class="muted">—</span>';
                let orcContent = c.orc ? `<div class="doc-cell"><span class="tag-doc">${c.orc}</span>${c.dt_orc ? `<span class="sub-date">${c.dt_orc}</span>` : ''}</div>` : '<span class="muted">—</span>';

                let badgeClass = 'badge-date-type';
                if (c.tipo_dt === 'Pedido') badgeClass += ' pedido';
                else if (c.tipo_dt === 'Nota') badgeClass += ' nota';
                else if (c.tipo_dt === 'Orçamento') badgeClass += ' orcamento';

                tr.innerHTML = `
                    <td>
                        <div><strong>${c.dt_ref}</strong></div>
                        <span class="${badgeClass}">Dt. ${c.tipo_dt}</span>
                    </td>
                    <td><span class="tag-unit-badge">${c.unidade}</span></td>
                    <td>${orcContent}</td>
                    <td>${pedContent}</td>
                    <td>${notaContent}</td>
                    <td>${c.pront ? c.pront : '—'}</td>
                    <td class="font-medium" style="max-width: 190px; overflow: hidden; text-overflow: ellipsis;" title="${c.pac}">${c.pac}</td>
                    <td style="max-width: 150px; overflow: hidden; text-overflow: ellipsis;" title="${c.med}">${c.med}</td>
                    <td style="max-width: 240px; overflow: hidden; text-overflow: ellipsis;" title="${c.prod}">${c.prod}</td>
                    <td class="text-right font-semibold" style="${currentCasesCategory === 'PEDIDO_A_FATURAR' ? 'color: #b45309;' : ''}">${formatBRLPrecise(c.val)}</td>
                `;
                tbody.appendChild(tr);
            });

            // Update Summary and Page Controls
            const startDisplay = startIndex + 1;
            const endDisplay = Math.min(startIndex + CASES_PAGE_SIZE, totalCount);

            const distinctOrc = new Set(cases.filter(c => c.orc).map(c => c.orc));
            const distinctPed = new Set(cases.filter(c => c.ped).map(c => c.ped));
            const distinctNota = new Set(cases.filter(c => c.nota).map(c => c.nota));

            let metricsSummary = [];
            if (distinctOrc.size > 0) metricsSummary.push(`<strong>${distinctOrc.size.toLocaleString('pt-BR')}</strong> orçamentos`);
            if (distinctPed.size > 0) metricsSummary.push(`<strong>${distinctPed.size.toLocaleString('pt-BR')}</strong> pedidos`);
            if (distinctNota.size > 0) metricsSummary.push(`<strong>${distinctNota.size.toLocaleString('pt-BR')}</strong> notas`);
            if (currentCasesCategory === 'PEDIDO_A_FATURAR') metricsSummary.push(`<span style="color: #b45309; font-weight: 600;">0 notas (Aguardando faturamento)</span>`);

            const metricsText = metricsSummary.length > 0 ? ` · ${metricsSummary.join(' · ')}` : '';

            document.getElementById('casesSummaryText').innerHTML = `
                Exibindo <strong>${startDisplay}</strong> a <strong>${endDisplay}</strong> de <strong>${totalCount.toLocaleString('pt-BR')}</strong> itens · Total: <strong>${formatBRLPrecise(totalSum)}</strong>${metricsText}
            `;
            document.getElementById('casesPageIndicator').textContent = `Página ${currentCasePage} de ${totalPages}`;
            document.getElementById('btnPrevPage').disabled = (currentCasePage <= 1);
            document.getElementById('btnNextPage').disabled = (currentCasePage >= totalPages);
        }

        // Export displayed/filtered cases to CSV
        function exportCasesCSV() {
            if (currentSource === 'mockup') {
                alert('Exportação disponível no modo "Dados Reais (Data Lake)".');
                return;
            }

            let cases = getRawCases(currentMonthKey, selectedUnit, currentCasesCategory);
            if (caseSearchTerm) {
                cases = cases.filter(c => {
                    return (
                        c.pac.toLowerCase().includes(caseSearchTerm) ||
                        c.orc.toLowerCase().includes(caseSearchTerm) ||
                        c.ped.toLowerCase().includes(caseSearchTerm) ||
                        c.nota.toLowerCase().includes(caseSearchTerm) ||
                        c.med.toLowerCase().includes(caseSearchTerm) ||
                        c.prod.toLowerCase().includes(caseSearchTerm) ||
                        c.unidade.toLowerCase().includes(caseSearchTerm) ||
                        String(c.pront).includes(caseSearchTerm)
                    );
                });
            }

            if (cases.length === 0) {
                alert('Nenhum registro encontrado para exportar.');
                return;
            }

            // Build CSV content with UTF-8 BOM
            const headers = [
                'Competência', 'Origem Data Competência', 'Unidade', 
                'Orçamento', 'Data Orçamento', 
                'Pedido', 'Data Pedido', 
                'Nota Fiscal', 'Data Nota', 
                'Prontuário', 'Paciente', 'Médico', 'Procedimento / Produto', 'Valor (R$)'
            ];
            let csvRows = [];
            csvRows.push(headers.join(';'));

            cases.forEach(c => {
                const row = [
                    `"${c.dt_ref}"`,
                    `"${c.tipo_dt}"`,
                    `"${c.unidade}"`,
                    `"${c.orc}"`,
                    `"${c.dt_orc}"`,
                    `"${c.ped}"`,
                    `"${c.dt_ped}"`,
                    `"${c.nota}"`,
                    `"${c.dt_nota}"`,
                    `"${c.pront}"`,
                    `"${c.pac.replace(/"/g, '""')}"`,
                    `"${c.med.replace(/"/g, '""')}"`,
                    `"${c.prod.replace(/"/g, '""')}"`,
                    `"${c.val.toFixed(2).replace('.', ',')}"`
                ];
                csvRows.push(row.join(';'));
            });

            const csvString = String.fromCharCode(0xFEFF) + csvRows.join(String.fromCharCode(13, 10));
            const blob = new Blob([csvString], { type: 'text/csv;charset=utf-8;' });
            const url = URL.createObjectURL(blob);
            const link = document.createElement('a');
            
            const cleanUnit = selectedUnit.replace(/[^a-zA-Z0-9_-]/g, '_');
            link.setAttribute('href', url);
            link.setAttribute('download', `casos_${cleanUnit}_${currentCasesCategory}_${currentMonthKey}.csv`);
            document.body.appendChild(link);
            link.click();
            document.body.removeChild(link);
            URL.revokeObjectURL(url);
        }

        window.addEventListener('DOMContentLoaded', init);
    </script>
</body>
</html>
"""

    with open(OUTPUT_HTML_PATH, 'w', encoding='utf-8') as f:
        f.write(html_template)
    logger.info(f"Dashboard successfully generated and written to: {OUTPUT_HTML_PATH}")


def main():
    logger.info("=== GENERATE PROTHEUS VENDA DIRETA DASHBOARD STARTED ===")
    t0 = time.time()
    try:
        targets = load_targets()
        data_lake_dataset, cases_dataset = fetch_and_aggregate_data()
        generate_html(data_lake_dataset, cases_dataset, targets)
        logger.info(f"=== COMPLETED IN {time.time()-t0:.2f}s ===")
    except Exception as e:
        logger.error(f"Error generating dashboard: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
