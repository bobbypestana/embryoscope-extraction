#!/usr/bin/env python3
"""
Executive & Digital Marketing BI Dashboard Generator — v3
Queries gold.leads_funil and compiles an interactive standalone HTML dashboard.

Tabs:
  1. 👔 Visão Gerencial (CEO / Executive Overview)
  2. 🎯 Funil & Conversão Detalhado
  3. 📈 Performance por Canal & Campanha
  4. 📋 Registros Individuais & Exportação
"""

import os
import yaml
import json
import logging
import duckdb
from datetime import datetime
import time

# Setup logging
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LOGS_DIR = os.path.join(SCRIPT_DIR, 'logs')
os.makedirs(LOGS_DIR, exist_ok=True)
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
script_name = os.path.splitext(os.path.basename(__file__))[0]
LOG_PATH = os.path.join(LOGS_DIR, f'{script_name}_{timestamp}.log')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.FileHandler(LOG_PATH),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

PARAMS_PATH = os.path.join(SCRIPT_DIR, 'params.yml')
with open(PARAMS_PATH, 'r') as f:
    config = yaml.safe_load(f)

DUCKDB_PATH = config['duckdb_path']
OUTPUT_HTML = os.path.join(SCRIPT_DIR, 'dashboard.html')


def fetch_data():
    logger.info("Connecting to database and fetching enriched gold.leads_funil data...")
    con = duckdb.connect(DUCKDB_PATH, read_only=True)

    df = con.execute("""
        SELECT
            lead_id,
            COALESCE(deal_name, '')                                   AS deal_name,
            COALESCE(deal_status, '')                                 AS deal_status,
            COALESCE(fonte, 'Não informada')                          AS fonte,
            COALESCE(campanha_id, 'Orgânico / Direto')                AS campanha_id,
            COALESCE(unidade, 'Não informada')                        AS unidade,
            COALESCE(funil, '')                                       AS funil,
            strftime(lead_from, '%Y-%m-%d')                          AS lead_from,
            COALESCE(strftime(lead_to, '%Y-%m-%d'), '')               AS lead_to,
            COALESCE(lead_email, '')                                  AS lead_email,
            prontuario,
            COALESCE(matched_email, '')                               AS matched_email,
            matched_flag,
            match_category,
            COALESCE(consulta_type, '')                               AS consulta_type,
            COALESCE(strftime(consulta_date, '%Y-%m-%d'), '')         AS consulta_date,
            COALESCE(consulta_status, '')                             AS consulta_status,
            dias_ate_primeira_consulta,
            ROUND(total_gasto_pos_lead, 2)                            AS total_gasto_pos_lead,
            qtd_itens_vendidos_pos_lead,
            qtd_pedidos_pos_lead,
            COALESCE(strftime(primeira_venda_data, '%Y-%m-%d'), '')   AS primeira_venda_data,
            dias_ate_primeira_venda
        FROM gold.leads_funil
        ORDER BY lead_from DESC
    """).df()

    con.close()
    
    # Load channel classification from Excel
    excel_path = os.path.join(SCRIPT_DIR, '..', 'data_input', 'canais_rd_station_.xlsx')
    if os.path.exists(excel_path):
        import pandas as pd
        excel_df = pd.read_excel(excel_path)
        keep_channels = set(excel_df[excel_df['flag_excluir_da_dash'] == 0]['name'].astype(str).str.strip())
        logger.info(f"Loaded {len(keep_channels)} valid marketing channels from {excel_path}")
    else:
        logger.warning("canais_rd_station_.xlsx not found, defaulting all channels to valid.")
        keep_channels = None

    records = df.to_dict(orient='records')
    for r in records:
        if keep_channels is not None:
            r['is_canal_valido'] = 1 if str(r['fonte']).strip() in keep_channels else 0
        else:
            r['is_canal_valido'] = 1

    logger.info(f"Loaded {len(records):,} leads. Valid marketing leads: {sum(r['is_canal_valido'] for r in records):,}")
    return records


def generate_html(data):
    logger.info("Compiling Executive BI HTML Dashboard template...")
    json_data = json.dumps(data)
    update_time = datetime.now().strftime('%d/%m/%Y %H:%M:%S')

    html = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Executive BI & Marketing Funnel | Huntington</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap" rel="stylesheet">
    <style>
        * {{ font-family: 'Inter', sans-serif; }}
        body {{ background: radial-gradient(ellipse at top, #001f3f 0%, #030712 70%); color: #f3f4f6; min-height: 100vh; }}
        .glass {{ background: rgba(10, 25, 47, 0.70); backdrop-filter: blur(16px); border: 1px solid rgba(0, 175, 217, 0.15); }}
        .glass-card {{ background: rgba(0, 31, 63, 0.65); border: 1px solid rgba(255, 255, 255, 0.08); }}
        ::-webkit-scrollbar {{ width: 6px; height: 6px; }}
        ::-webkit-scrollbar-track {{ background: #030712; }}
        ::-webkit-scrollbar-thumb {{ background: #00508c; border-radius: 3px; }}
        ::-webkit-scrollbar-thumb:hover {{ background: #00afd9; }}
        select, input {{ color-scheme: dark; }}
        .tab-btn {{ background: rgba(0, 50, 90, 0.5); color: #94a3b8; border: 1px solid rgba(0, 175, 217, 0.2); }}
        .tab-btn:hover {{ background: rgba(0, 80, 140, 0.7); color: #f8fafc; }}
        .active-tab {{ background: #00508c !important; color: #ffffff !important; border-color: #00afd9 !important; box-shadow: 0 0 16px rgba(0, 175, 217, 0.35); }}
    </style>
</head>
<body class="p-4 md:p-6 lg:p-8">
<div class="max-w-[1550px] mx-auto space-y-6">

    <!-- ═══════════════════════════════ HEADER ═══════════════════════════════ -->
    <header class="glass rounded-2xl px-6 py-5 flex flex-col lg:flex-row lg:items-center justify-between gap-4">
        <div>
            <div class="flex items-center gap-3">
                <div class="p-2.5 bg-[#00508c]/40 border border-[#00afd9]/40 rounded-xl text-[#00afd9]">
                    <svg class="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 7h8m0 0v8m0-8l-8 8-4-4-6 6"/>
                    </svg>
                </div>
                <div>
                    <h1 class="text-xl md:text-2xl font-extrabold text-white tracking-tight flex items-center gap-2">
                        Executive BI &amp; Marketing Funnel
                        <span class="text-xs font-semibold px-2.5 py-0.5 rounded-full bg-[#00afd9]/20 text-[#00afd9] border border-[#00afd9]/30">End-to-End</span>
                    </h1>
                    <p class="text-slate-400 text-xs mt-0.5">RD Station CRM · Clinisys Agendamentos · Protheus ERP Faturamento</p>
                </div>
            </div>
        </div>
        <div class="flex flex-wrap items-center gap-3">
            <button id="btn-export-csv" class="flex items-center gap-2 px-3.5 py-2 rounded-xl bg-slate-800 hover:bg-slate-700 text-slate-200 text-xs font-semibold border border-slate-700 transition-all shadow-sm">
                <svg class="w-4 h-4 text-[#00b9ad]" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-4l-4 4m0 0l-4-4m4 4V4"/>
                </svg>
                Exportar CSV
            </button>
            <div class="flex items-center gap-2 text-xs bg-slate-900/80 border border-slate-800 text-slate-400 px-3.5 py-2 rounded-xl">
                <span class="w-2 h-2 rounded-full bg-[#00b9ad] animate-pulse"></span>
                <span>Atualizado: <strong class="text-slate-200">{update_time}</strong></span>
            </div>
        </div>
    </header>

    <!-- ═══════════════════════════════ GLOBAL FILTERS ═══════════════════════════════ -->
    <section class="glass rounded-2xl px-6 py-4">
        <div class="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-3">
            <div class="flex flex-col gap-1">
                <label class="text-[10px] font-bold uppercase tracking-wider text-[#00afd9] flex items-center gap-1">
                    <span class="w-1.5 h-1.5 rounded-full bg-[#00afd9]"></span>
                    Escopo de Canais
                </label>
                <select id="f-canal-valido" class="bg-slate-900 border border-[#00afd9]/50 text-[#00afd9] text-xs rounded-xl px-3 py-2 font-semibold focus:outline-none focus:border-[#00afd9]">
                    <option value="valid_only" selected>Canais da Campanha (Padrão)</option>
                    <option value="all">Todos os Canais (Sem Filtro)</option>
                    <option value="excluded_only">Canais Desconsiderados</option>
                </select>
            </div>
            <div class="flex flex-col gap-1">
                <label class="text-[10px] font-bold uppercase tracking-wider text-slate-400">Canal Específico</label>
                <select id="f-source" class="bg-slate-900 border border-slate-700/80 text-slate-200 text-xs rounded-xl px-3 py-2 focus:outline-none">
                    <option value="all">Todas as Fontes</option>
                </select>
            </div>
            <div class="flex flex-col gap-1">
                <label class="text-[10px] font-bold uppercase tracking-wider text-slate-400">Unidade Clínica</label>
                <select id="f-unidade" class="bg-slate-900 border border-slate-700/80 text-slate-200 text-xs rounded-xl px-3 py-2 focus:outline-none">
                    <option value="all">Todas as Unidades</option>
                </select>
            </div>
            <div class="flex flex-col gap-1">
                <label class="text-[10px] font-bold uppercase tracking-wider text-slate-400">1ª Consulta</label>
                <select id="f-consulta" class="bg-slate-900 border border-slate-700/80 text-slate-200 text-xs rounded-xl px-3 py-2 focus:outline-none">
                    <option value="all">Todas</option>
                    <option value="yes">Com 1ª Consulta</option>
                    <option value="no">Sem 1ª Consulta</option>
                </select>
            </div>
            <div class="flex flex-col gap-1">
                <label class="text-[10px] font-bold uppercase tracking-wider text-slate-400">Faturamento / Venda</label>
                <select id="f-sales" class="bg-slate-900 border border-slate-700/80 text-slate-200 text-xs rounded-xl px-3 py-2 focus:outline-none">
                    <option value="all">Todos</option>
                    <option value="with_sales">Com Venda (&gt; R$ 0)</option>
                    <option value="no_sales">Sem Venda</option>
                </select>
            </div>
            <div class="flex flex-col gap-1">
                <label class="text-[10px] font-bold uppercase tracking-wider text-slate-400">Período Inbound</label>
                <select id="f-period" class="bg-slate-900 border border-slate-700/80 text-slate-200 text-xs rounded-xl px-3 py-2 focus:outline-none">
                    <option value="all">Todo o Histórico</option>
                    <option value="2026">Ano 2026</option>
                    <option value="2025">Ano 2025</option>
                    <option value="last90">Últimos 90 dias</option>
                    <option value="last30">Últimos 30 dias</option>
                    <option value="custom">Personalizado...</option>
                </select>
            </div>
            <div id="custom-dates" class="hidden flex-col gap-1 col-span-2 lg:col-span-1">
                <label class="text-[10px] font-bold uppercase tracking-wider text-slate-400">Intervalo Custom</label>
                <div class="flex gap-1.5">
                    <input type="date" id="f-start" class="w-1/2 bg-slate-900 border border-slate-700 text-slate-200 text-[11px] rounded-lg px-2 py-1.5 focus:outline-none">
                    <input type="date" id="f-end" class="w-1/2 bg-slate-900 border border-slate-700 text-slate-200 text-[11px] rounded-lg px-2 py-1.5 focus:outline-none">
                </div>
            </div>
        </div>
    </section>

    <!-- ═══════════════════════════════ NAVIGATION TABS ═══════════════════════════════ -->
    <nav class="flex flex-wrap gap-2">
        <button id="btn-tab-ceo" class="tab-btn active-tab px-5 py-2.5 text-xs font-bold rounded-xl transition-all flex items-center gap-2">
            <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M16 8v8m-4-5v5m-4-2v2m-2 4h12a2 2 0 002-2V6a2 2 0 00-2-2H6a2 2 0 00-2 2v12a2 2 0 002 2z"/>
            </svg>
            1. Visão Gerencial (CEO View)
        </button>
        <button id="btn-tab-funnel" class="tab-btn px-5 py-2.5 text-xs font-bold rounded-xl transition-all flex items-center gap-2">
            <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3 4a1 1 0 011-1h16a1 1 0 011 1v2.586a1 1 0 01-.293.707l-6.414 6.414a1 1 0 00-.293.707V17l-4 4v-6.586a1 1 0 00-.293-.707L3.293 7.293A1 1 0 013 6.586V4z"/>
            </svg>
            2. Funil &amp; Conversão Detalhado
        </button>
        <button id="btn-tab-channels" class="tab-btn px-5 py-2.5 text-xs font-bold rounded-xl transition-all flex items-center gap-2">
            <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M11 3.055A9.001 9.001 0 1020.945 13H11V3.055z"/>
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M20.488 9H15V3.512A9.025 9.025 0 0120.488 9z"/>
            </svg>
            3. Canais &amp; Campanhas
        </button>
        <button id="btn-tab-records" class="tab-btn px-5 py-2.5 text-xs font-bold rounded-xl transition-all flex items-center gap-2">
            <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 6h16M4 10h16M4 14h16M4 18h16"/>
            </svg>
            4. Registros &amp; Auditoria
        </button>
    </nav>

    <!-- ═══════════════════════════════ TAB 1: CEO OVERVIEW ═══════════════════════════════ -->
    <div id="tab-ceo" class="space-y-6">

        <!-- Executive KPI Cards -->
        <div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
            <div class="glass-card rounded-2xl p-5 border-l-4 border-[#00afd9]">
                <div class="text-[11px] font-bold uppercase tracking-wider text-slate-400">Total Leads Gerados</div>
                <div id="kpi-total-leads" class="text-3xl font-extrabold text-white mt-1">—</div>
                <div class="text-xs text-[#00afd9] mt-1 flex items-center gap-1">
                    <span id="kpi-matched-leads">—</span> acoplados a prontuários (<span id="kpi-matched-pct">—</span>)
                </div>
            </div>

            <div class="glass-card rounded-2xl p-5 border-l-4 border-[#4e358c]">
                <div class="text-[11px] font-bold uppercase tracking-wider text-slate-400">1ª Consultas Agendadas</div>
                <div id="kpi-total-consultas" class="text-3xl font-extrabold text-[#c4b5fd] mt-1">—</div>
                <div class="text-xs text-slate-400 mt-1">
                    Taxa de Agendamento: <strong id="kpi-conv-lead-cons" class="text-[#a78bfa]">—</strong>
                </div>
            </div>

            <div class="glass-card rounded-2xl p-5 border-l-4 border-[#00b9ad]">
                <div class="text-[11px] font-bold uppercase tracking-wider text-slate-400">Pacientes Convertidos (Vendas)</div>
                <div id="kpi-total-sales-leads" class="text-3xl font-extrabold text-[#00b9ad] mt-1">—</div>
                <div class="text-xs text-slate-400 mt-1">
                    Conversão de Consultas: <strong id="kpi-conv-cons-sale" class="text-[#00b9ad]">—</strong>
                </div>
            </div>

            <div class="glass-card rounded-2xl p-5 border-l-4 border-[#f7a600]">
                <div class="text-[11px] font-bold uppercase tracking-wider text-slate-400">Receita Total Atribuída</div>
                <div id="kpi-total-revenue" class="text-3xl font-extrabold text-[#f7a600] mt-1">—</div>
                <div class="text-xs text-slate-400 mt-1">
                    Ticket Médio / Paciente: <strong id="kpi-ticket-medio" class="text-[#f7a600]">—</strong>
                </div>
            </div>
        </div>

        <!-- Velocity & Executive Metrics Strip -->
        <div class="grid grid-cols-2 sm:grid-cols-4 gap-4">
            <div class="glass rounded-xl p-4 text-center">
                <div class="text-[10px] font-bold uppercase tracking-wider text-slate-400">Taxa de Presença (Show Rate)</div>
                <div id="kpi-show-rate" class="text-xl font-bold text-[#00b9ad] mt-1">—</div>
                <div class="text-[10px] text-slate-500 mt-0.5">Consultas c/ status 'Atendido'</div>
            </div>
            <div class="glass rounded-xl p-4 text-center">
                <div class="text-[10px] font-bold uppercase tracking-wider text-slate-400">Conversão Global Inbound</div>
                <div id="kpi-global-conv" class="text-xl font-bold text-[#00afd9] mt-1">—</div>
                <div class="text-[10px] text-slate-500 mt-0.5">Lead $\rightarrow$ Tratamento Pago</div>
            </div>
            <div class="glass rounded-xl p-4 text-center">
                <div class="text-[10px] font-bold uppercase tracking-wider text-slate-400">Tempo Médio até 1ª Consulta</div>
                <div id="kpi-avg-days-cons" class="text-xl font-bold text-[#c4b5fd] mt-1">—</div>
                <div class="text-[10px] text-slate-500 mt-0.5">Média em dias pós-lead</div>
            </div>
            <div class="glass rounded-xl p-4 text-center">
                <div class="text-[10px] font-bold uppercase tracking-wider text-slate-400">Tempo Médio até 1ª Venda</div>
                <div id="kpi-avg-days-sale" class="text-xl font-bold text-[#f7a600] mt-1">—</div>
                <div class="text-[10px] text-slate-500 mt-0.5">Ciclo Comercial Completo</div>
            </div>
        </div>

        <!-- Main Charts Grid -->
        <div class="grid grid-cols-1 lg:grid-cols-3 gap-6">
            <div class="lg:col-span-2 glass rounded-2xl p-6">
                <h3 class="text-sm font-bold text-white uppercase tracking-wider mb-4 flex items-center gap-2">
                    <span class="w-2 h-2 rounded-full bg-[#00afd9]"></span>
                    Evolução Mensal: Volume de Leads vs. Receita Faturada (R$)
                </h3>
                <div class="h-80">
                    <canvas id="chart-monthly-trends"></canvas>
                </div>
            </div>

            <div class="glass rounded-2xl p-6">
                <h3 class="text-sm font-bold text-white uppercase tracking-wider mb-4 flex items-center gap-2">
                    <span class="w-2 h-2 rounded-full bg-[#00b9ad]"></span>
                    Top 5 Fontes Geradoras de Receita
                </h3>
                <div class="h-80">
                    <canvas id="chart-top-sources-revenue"></canvas>
                </div>
            </div>
        </div>
    </div>

    <!-- ═══════════════════════════════ TAB 2: DETAILED FUNNEL ═══════════════════════════════ -->
    <div id="tab-funnel" class="hidden space-y-6">
        <div class="glass rounded-2xl p-6">
            <h3 class="text-sm font-bold text-white uppercase tracking-wider mb-6 flex items-center gap-2">
                <span class="w-2 h-2 rounded-full bg-[#00afd9]"></span>
                Macro Funil de Conversão Hospitalar (Leads $\rightarrow$ Prontuários $\rightarrow$ Consultas $\rightarrow$ Procedimentos)
            </h3>

            <div class="grid grid-cols-1 md:grid-cols-4 gap-4 mb-6">
                <div class="bg-slate-900/80 border border-slate-800 rounded-xl p-4 text-center">
                    <div class="text-xs text-slate-400 font-semibold">1. Leads Inbound</div>
                    <div id="funnel-step-1" class="text-2xl font-bold text-white my-1">—</div>
                    <div class="text-[11px] text-slate-500">Base CRM 100%</div>
                </div>
                <div class="bg-slate-900/80 border border-slate-800 rounded-xl p-4 text-center">
                    <div class="text-xs text-[#00afd9] font-semibold">2. Pacientes Acoplados</div>
                    <div id="funnel-step-2" class="text-2xl font-bold text-[#00afd9] my-1">—</div>
                    <div id="funnel-pct-1-2" class="text-[11px] text-[#00afd9] font-semibold">— dos leads</div>
                </div>
                <div class="bg-slate-900/80 border border-slate-800 rounded-xl p-4 text-center">
                    <div class="text-xs text-[#c4b5fd] font-semibold">3. 1ª Consulta Agendada</div>
                    <div id="funnel-step-3" class="text-2xl font-bold text-[#c4b5fd] my-1">—</div>
                    <div id="funnel-pct-2-3" class="text-[11px] text-[#c4b5fd] font-semibold">— dos pacientes</div>
                </div>
                <div class="bg-slate-900/80 border border-slate-800 rounded-xl p-4 text-center">
                    <div class="text-xs text-[#00b9ad] font-semibold">4. Procedimento / Venda</div>
                    <div id="funnel-step-4" class="text-2xl font-bold text-[#00b9ad] my-1">—</div>
                    <div id="funnel-pct-3-4" class="text-[11px] text-[#00b9ad] font-semibold">— das consultas</div>
                </div>
            </div>

            <!-- Funnel progress bars -->
            <div class="space-y-3">
                <div class="w-full bg-slate-900 h-4 rounded-full overflow-hidden flex">
                    <div id="pbar-leads" class="bg-[#00508c] h-full" style="width: 100%"></div>
                </div>
                <div class="w-full bg-slate-900 h-4 rounded-full overflow-hidden flex">
                    <div id="pbar-matched" class="bg-[#00afd9] h-full" style="width: 50%"></div>
                </div>
                <div class="w-full bg-slate-900 h-4 rounded-full overflow-hidden flex">
                    <div id="pbar-consultas" class="bg-[#4e358c] h-full" style="width: 35%"></div>
                </div>
                <div class="w-full bg-slate-900 h-4 rounded-full overflow-hidden flex">
                    <div id="pbar-sales" class="bg-[#00b9ad] h-full" style="width: 15%"></div>
                </div>
            </div>
        </div>

        <div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
            <!-- Consulta Status Breakdown -->
            <div class="glass rounded-2xl p-6">
                <h3 class="text-sm font-bold text-white uppercase tracking-wider mb-4">Status da 1ª Consulta</h3>
                <div class="h-64">
                    <canvas id="chart-consulta-status"></canvas>
                </div>
            </div>

            <!-- Match Channel Distribution -->
            <div class="glass rounded-2xl p-6">
                <h3 class="text-sm font-bold text-white uppercase tracking-wider mb-4">Canais de Acoplamento (Identificação do Paciente)</h3>
                <div class="h-64">
                    <canvas id="chart-match-categories"></canvas>
                </div>
            </div>
        </div>
    </div>

    <!-- ═══════════════════════════════ TAB 3: CHANNELS & CAMPAIGNS ═══════════════════════════════ -->
    <div id="tab-channels" class="hidden space-y-6">
        <div class="glass rounded-2xl p-6">
            <h3 class="text-sm font-bold text-white uppercase tracking-wider mb-4 flex items-center justify-between">
                <span>Matriz de Performance por Canal de Aquisição (Fonte)</span>
                <span class="text-xs text-slate-400 font-normal">Ordenado por Receita Total</span>
            </h3>
            <div class="overflow-x-auto">
                <table class="w-full text-xs text-left">
                    <thead class="bg-slate-900/90 uppercase font-bold text-slate-400 border-b border-slate-700">
                        <tr>
                            <th class="px-4 py-3">Canal / Fonte</th>
                            <th class="px-4 py-3 text-right">Leads</th>
                            <th class="px-4 py-3 text-right">Acoplados</th>
                            <th class="px-4 py-3 text-right">1ª Consultas</th>
                            <th class="px-4 py-3 text-right">Conv. Consulta</th>
                            <th class="px-4 py-3 text-right">Vendas (&gt;0)</th>
                            <th class="px-4 py-3 text-right">Conv. Venda</th>
                            <th class="px-4 py-3 text-right">Receita Total (R$)</th>
                            <th class="px-4 py-3 text-right">Ticket Médio</th>
                        </tr>
                    </thead>
                    <tbody id="table-channels-body" class="divide-y divide-slate-800 text-slate-300">
                        <!-- Populated by JavaScript -->
                    </tbody>
                </table>
            </div>
        </div>

        <div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
            <div class="glass rounded-2xl p-6">
                <h3 class="text-sm font-bold text-white uppercase tracking-wider mb-4">Volume de Leads por Unidade Clínica</h3>
                <div class="h-72">
                    <canvas id="chart-unidade-volume"></canvas>
                </div>
            </div>
            <div class="glass rounded-2xl p-6">
                <h3 class="text-sm font-bold text-white uppercase tracking-wider mb-4">Receita Atribuída por Unidade Clínica</h3>
                <div class="h-72">
                    <canvas id="chart-unidade-revenue"></canvas>
                </div>
            </div>
        </div>
    </div>

    <!-- ═══════════════════════════════ TAB 4: RECORDS & AUDIT ═══════════════════════════════ -->
    <div id="tab-records" class="hidden space-y-6">
        <div class="glass rounded-2xl p-6">
            <div class="flex flex-col md:flex-row items-start md:items-center justify-between gap-4 mb-4">
                <div>
                    <h3 class="text-sm font-bold text-white uppercase tracking-wider">Explorador de Registros Individuais</h3>
                    <p class="text-xs text-slate-400 mt-0.5">Mostrando <span id="rec-count-showing">0</span> de <span id="rec-count-total">0</span> leads filtrados</p>
                </div>
                <div class="flex items-center gap-3 w-full md:w-auto">
                    <input type="text" id="table-search" placeholder="Buscar por Nome, E-mail, Prontuário..." class="bg-slate-900 border border-slate-700 text-slate-200 text-xs rounded-xl px-3.5 py-2 w-full md:w-80 focus:outline-none focus:border-sky-500">
                </div>
            </div>

            <div class="overflow-x-auto rounded-xl border border-slate-800">
                <table class="w-full text-xs text-left">
                    <thead class="bg-slate-900 uppercase font-bold text-slate-400 border-b border-slate-800">
                        <tr>
                            <th class="px-4 py-3">Lead ID / Nome</th>
                            <th class="px-4 py-3">Data Lead</th>
                            <th class="px-4 py-3">Canal / Fonte</th>
                            <th class="px-4 py-3">Unidade</th>
                            <th class="px-4 py-3">Prontuário</th>
                            <th class="px-4 py-3">1ª Consulta</th>
                            <th class="px-4 py-3">Status Consulta</th>
                            <th class="px-4 py-3 text-right">Pedidos</th>
                            <th class="px-4 py-3 text-right">Receita Total</th>
                        </tr>
                    </thead>
                    <tbody id="records-table-body" class="divide-y divide-slate-800 text-slate-300 font-mono text-[11px]">
                        <!-- Paginated records -->
                    </tbody>
                </table>
            </div>

            <!-- Pagination -->
            <div class="flex items-center justify-between mt-4">
                <div class="text-xs text-slate-400">
                    Página <span id="page-current">1</span> de <span id="page-total">1</span>
                </div>
                <div class="flex gap-2">
                    <button id="btn-prev-page" class="px-3 py-1.5 bg-slate-800 hover:bg-slate-700 text-slate-200 rounded-lg text-xs font-semibold disabled:opacity-40">Anterior</button>
                    <button id="btn-next-page" class="px-3 py-1.5 bg-slate-800 hover:bg-slate-700 text-slate-200 rounded-lg text-xs font-semibold disabled:opacity-40">Próxima</button>
                </div>
            </div>
        </div>
    </div>

</div>

<!-- ═══════════════════════════════ APPLICATION JAVASCRIPT ═══════════════════════════════ -->
<script>
const rawData = {json_data};
let filteredData = [...rawData];
let currentPage = 1;
const pageSize = 50;

// Chart instances
let chartMonthlyTrends, chartTopSourcesRev, chartConsultaStatus, chartMatchCategories, chartUnidadeVol, chartUnidadeRev;

function init() {{
    populateFilterOptions();
    bindEvents();
    applyFilters();
}}

function populateFilterOptions() {{
    const sources = [...new Set(rawData.map(r => r.fonte).filter(Boolean))].sort();
    const sourceSelect = document.getElementById('f-source');
    sources.forEach(s => {{
        const opt = document.createElement('option');
        opt.value = s;
        opt.textContent = s;
        sourceSelect.appendChild(opt);
    }});

    const unidades = [...new Set(rawData.map(r => r.unidade).filter(Boolean))].sort();
    const unidadeSelect = document.getElementById('f-unidade');
    unidades.forEach(u => {{
        const opt = document.createElement('option');
        opt.value = u;
        opt.textContent = u;
        unidadeSelect.appendChild(opt);
    }});
}}

function bindEvents() {{
    ['f-canal-valido', 'f-source', 'f-unidade', 'f-consulta', 'f-sales', 'f-period', 'f-start', 'f-end'].forEach(id => {{
        document.getElementById(id).addEventListener('change', applyFilters);
    }});

    document.getElementById('f-period').addEventListener('change', (e) => {{
        document.getElementById('custom-dates').classList.toggle('hidden', e.target.value !== 'custom');
    }});

    document.getElementById('table-search').addEventListener('input', () => {{
        currentPage = 1;
        renderRecordsTable();
    }});

    document.getElementById('btn-prev-page').addEventListener('click', () => {{
        if (currentPage > 1) {{ currentPage--; renderRecordsTable(); }}
    }});

    document.getElementById('btn-next-page').addEventListener('click', () => {{
        const totalPages = Math.ceil(getSearchedData().length / pageSize) || 1;
        if (currentPage < totalPages) {{ currentPage++; renderRecordsTable(); }}
    }});

    document.getElementById('btn-export-csv').addEventListener('click', exportCSV);

    // Tab Navigation
    const tabs = [
        {{ btn: 'btn-tab-ceo', view: 'tab-ceo' }},
        {{ btn: 'btn-tab-funnel', view: 'tab-funnel' }},
        {{ btn: 'btn-tab-channels', view: 'tab-channels' }},
        {{ btn: 'btn-tab-records', view: 'tab-records' }}
    ];

    tabs.forEach(t => {{
        document.getElementById(t.btn).addEventListener('click', () => {{
            tabs.forEach(o => {{
                document.getElementById(o.btn).classList.remove('active-tab');
                document.getElementById(o.view).classList.add('hidden');
            }});
            document.getElementById(t.btn).classList.add('active-tab');
            document.getElementById(t.view).classList.remove('hidden');
        }});
    }});
}}

function applyFilters() {{
    const sCanalScope = document.getElementById('f-canal-valido').value;
    const sSource = document.getElementById('f-source').value;
    const sUnidade = document.getElementById('f-unidade').value;
    const sConsulta = document.getElementById('f-consulta').value;
    const sSales = document.getElementById('f-sales').value;
    const sPeriod = document.getElementById('f-period').value;
    const startVal = document.getElementById('f-start').value;
    const endVal = document.getElementById('f-end').value;

    filteredData = rawData.filter(r => {{
        if (sCanalScope === 'valid_only' && r.is_canal_valido !== 1) return false;
        if (sCanalScope === 'excluded_only' && r.is_canal_valido !== 0) return false;

        if (sSource !== 'all' && r.fonte !== sSource) return false;
        if (sUnidade !== 'all' && r.unidade !== sUnidade) return false;
        if (sConsulta === 'yes' && !r.consulta_date) return false;
        if (sConsulta === 'no' && r.consulta_date) return false;
        if (sSales === 'with_sales' && r.total_gasto_pos_lead <= 0) return false;
        if (sSales === 'no_sales' && r.total_gasto_pos_lead > 0) return false;

        if (sPeriod === '2026' && !r.lead_from.startsWith('2026')) return false;
        if (sPeriod === '2025' && !r.lead_from.startsWith('2025')) return false;
        if (sPeriod === 'last90') {{
            const d = new Date(r.lead_from);
            const cutoff = new Date(); cutoff.setDate(cutoff.getDate() - 90);
            if (d < cutoff) return false;
        }}
        if (sPeriod === 'last30') {{
            const d = new Date(r.lead_from);
            const cutoff = new Date(); cutoff.setDate(cutoff.getDate() - 30);
            if (d < cutoff) return false;
        }}
        if (sPeriod === 'custom') {{
            if (startVal && r.lead_from < startVal) return false;
            if (endVal && r.lead_from > endVal) return false;
        }}
        return true;
    }});

    currentPage = 1;
    updateAllViews();
}}

function updateAllViews() {{
    updateKPIs();
    updateFunnelView();
    updateCharts();
    updateChannelsTable();
    renderRecordsTable();
}}

function updateKPIs() {{
    const totalLeads = filteredData.length;
    const matchedLeads = filteredData.filter(r => r.prontuario != null).length;
    const withConsulta = filteredData.filter(r => r.consulta_date !== '').length;
    const attended = filteredData.filter(r => r.consulta_status === 'Atendido').length;
    const withSales = filteredData.filter(r => r.total_gasto_pos_lead > 0).length;
    const totalRevenue = filteredData.reduce((acc, r) => acc + (r.total_gasto_pos_lead || 0), 0);

    const ticketMedio = withSales > 0 ? (totalRevenue / withSales) : 0;
    const matchedPct = totalLeads > 0 ? (matchedLeads / totalLeads * 100) : 0;
    const convLeadCons = matchedLeads > 0 ? (withConsulta / matchedLeads * 100) : 0;
    const convConsSale = withConsulta > 0 ? (withSales / withConsulta * 100) : 0;
    const showRate = withConsulta > 0 ? (attended / withConsulta * 100) : 0;
    const globalConv = totalLeads > 0 ? (withSales / totalLeads * 100) : 0;

    // Velocity averages
    const daysConsArr = filteredData.map(r => r.dias_ate_primeira_consulta).filter(d => d != null && d >= 0);
    const avgDaysCons = daysConsArr.length > 0 ? (daysConsArr.reduce((a, b) => a + b, 0) / daysConsArr.length).toFixed(1) : '—';

    const daysSaleArr = filteredData.map(r => r.dias_ate_primeira_venda).filter(d => d != null && d >= 0);
    const avgDaysSale = daysSaleArr.length > 0 ? (daysSaleArr.reduce((a, b) => a + b, 0) / daysSaleArr.length).toFixed(1) : '—';

    document.getElementById('kpi-total-leads').textContent = totalLeads.toLocaleString('pt-BR');
    document.getElementById('kpi-matched-leads').textContent = matchedLeads.toLocaleString('pt-BR');
    document.getElementById('kpi-matched-pct').textContent = matchedPct.toFixed(1) + '%';
    document.getElementById('kpi-total-consultas').textContent = withConsulta.toLocaleString('pt-BR');
    document.getElementById('kpi-conv-lead-cons').textContent = convLeadCons.toFixed(1) + '%';
    document.getElementById('kpi-total-sales-leads').textContent = withSales.toLocaleString('pt-BR');
    document.getElementById('kpi-conv-cons-sale').textContent = convConsSale.toFixed(1) + '%';
    document.getElementById('kpi-total-revenue').textContent = 'R$ ' + totalRevenue.toLocaleString('pt-BR', {{ minimumFractionDigits: 2, maximumFractionDigits: 2 }});
    document.getElementById('kpi-ticket-medio').textContent = 'R$ ' + ticketMedio.toLocaleString('pt-BR', {{ minimumFractionDigits: 2, maximumFractionDigits: 2 }});

    document.getElementById('kpi-show-rate').textContent = showRate.toFixed(1) + '%';
    document.getElementById('kpi-global-conv').textContent = globalConv.toFixed(1) + '%';
    document.getElementById('kpi-avg-days-cons').textContent = avgDaysCons !== '—' ? avgDaysCons + ' dias' : '—';
    document.getElementById('kpi-avg-days-sale').textContent = avgDaysSale !== '—' ? avgDaysSale + ' dias' : '—';
}}

function updateFunnelView() {{
    const totalLeads = filteredData.length;
    const matchedLeads = filteredData.filter(r => r.prontuario != null).length;
    const withConsulta = filteredData.filter(r => r.consulta_date !== '').length;
    const withSales = filteredData.filter(r => r.total_gasto_pos_lead > 0).length;

    document.getElementById('funnel-step-1').textContent = totalLeads.toLocaleString('pt-BR');
    document.getElementById('funnel-step-2').textContent = matchedLeads.toLocaleString('pt-BR');
    document.getElementById('funnel-step-3').textContent = withConsulta.toLocaleString('pt-BR');
    document.getElementById('funnel-step-4').textContent = withSales.toLocaleString('pt-BR');

    const pct12 = totalLeads > 0 ? (matchedLeads / totalLeads * 100).toFixed(1) : 0;
    const pct23 = matchedLeads > 0 ? (withConsulta / matchedLeads * 100).toFixed(1) : 0;
    const pct34 = withConsulta > 0 ? (withSales / withConsulta * 100).toFixed(1) : 0;

    document.getElementById('funnel-pct-1-2').textContent = pct12 + '% dos leads';
    document.getElementById('funnel-pct-2-3').textContent = pct23 + '% dos acoplados';
    document.getElementById('funnel-pct-3-4').textContent = pct34 + '% das consultas';

    document.getElementById('pbar-leads').style.width = '100%';
    document.getElementById('pbar-matched').style.width = (totalLeads > 0 ? (matchedLeads / totalLeads * 100) : 0) + '%';
    document.getElementById('pbar-consultas').style.width = (totalLeads > 0 ? (withConsulta / totalLeads * 100) : 0) + '%';
    document.getElementById('pbar-sales').style.width = (totalLeads > 0 ? (withSales / totalLeads * 100) : 0) + '%';
}}

function updateCharts() {{
    // Monthly Evolution
    const monthMap = {{}};
    filteredData.forEach(r => {{
        const m = r.lead_from.substring(0, 7); // YYYY-MM
        if (!monthMap[m]) monthMap[m] = {{ leads: 0, revenue: 0 }};
        monthMap[m].leads++;
        monthMap[m].revenue += (r.total_gasto_pos_lead || 0);
    }});

    const sortedMonths = Object.keys(monthMap).sort();
    const labels = sortedMonths;
    const leadsSeries = sortedMonths.map(m => monthMap[m].leads);
    const revenueSeries = sortedMonths.map(m => monthMap[m].revenue);

    if (chartMonthlyTrends) chartMonthlyTrends.destroy();
    chartMonthlyTrends = new Chart(document.getElementById('chart-monthly-trends'), {{
        type: 'bar',
        data: {{
            labels: labels,
            datasets: [
                {{
                    label: 'Volume de Leads',
                    data: leadsSeries,
                    backgroundColor: 'rgba(0, 175, 217, 0.45)',
                    borderColor: '#00AFD9',
                    borderWidth: 1.5,
                    yAxisID: 'y'
                }},
                {{
                    label: 'Receita Atribuída (R$)',
                    data: revenueSeries,
                    type: 'line',
                    borderColor: '#00B9AD',
                    backgroundColor: 'rgba(0, 185, 173, 0.15)',
                    borderWidth: 2.5,
                    tension: 0.3,
                    fill: false,
                    yAxisID: 'y1'
                }}
            ]
        }},
        options: {{
            responsive: true,
            maintainAspectRatio: false,
            interaction: {{ mode: 'index', intersect: false }},
            scales: {{
                x: {{ grid: {{ color: 'rgba(255,255,255,0.05)' }}, ticks: {{ color: '#94a3b8', font: {{ size: 10 }} }} }},
                y: {{ position: 'left', grid: {{ color: 'rgba(255,255,255,0.05)' }}, ticks: {{ color: '#00AFD9' }} }},
                y1: {{ position: 'right', grid: {{ drawOnChartArea: false }}, ticks: {{ color: '#00B9AD', callback: v => 'R$ ' + (v/1000).toFixed(0) + 'k' }} }}
            }},
            plugins: {{ legend: {{ labels: {{ color: '#cbd5e1' }} }} }}
        }}
    }});

    // Top Sources Revenue
    const sourceRev = {{}};
    filteredData.forEach(r => {{
        const s = r.fonte || 'Não informada';
        sourceRev[s] = (sourceRev[s] || 0) + (r.total_gasto_pos_lead || 0);
    }});
    const top5Sources = Object.entries(sourceRev).sort((a,b) => b[1] - a[1]).slice(0, 5);

    if (chartTopSourcesRev) chartTopSourcesRev.destroy();
    chartTopSourcesRev = new Chart(document.getElementById('chart-top-sources-revenue'), {{
        type: 'doughnut',
        data: {{
            labels: top5Sources.map(s => s[0]),
            datasets: [{{
                data: top5Sources.map(s => s[1]),
                backgroundColor: ['#00AFD9', '#4E358C', '#00B9AD', '#F7A600', '#EA516D'],
                borderWidth: 0
            }}]
        }},
        options: {{
            responsive: true,
            maintainAspectRatio: false,
            plugins: {{
                legend: {{ position: 'bottom', labels: {{ color: '#cbd5e1', font: {{ size: 10 }} }} }}
            }}
        }}
    }});

    // Consulta Status
    const statusMap = {{}};
    filteredData.filter(r => r.consulta_date !== '').forEach(r => {{
        const st = r.consulta_status || 'Outros / Não reg.';
        statusMap[st] = (statusMap[st] || 0) + 1;
    }});
    if (chartConsultaStatus) chartConsultaStatus.destroy();
    chartConsultaStatus = new Chart(document.getElementById('chart-consulta-status'), {{
        type: 'pie',
        data: {{
            labels: Object.keys(statusMap),
            datasets: [{{
                data: Object.values(statusMap),
                backgroundColor: ['#00B9AD', '#EA516D', '#F7A600', '#4E358C', '#706F6F'],
                borderWidth: 0
            }}]
        }},
        options: {{
            responsive: true,
            maintainAspectRatio: false,
            plugins: {{ legend: {{ position: 'right', labels: {{ color: '#cbd5e1', font: {{ size: 11 }} }} }} }}
        }}
    }});

    // Match Categories
    const matchMap = {{}};
    filteredData.forEach(r => {{
        const cat = r.match_category || 'unmatched';
        matchMap[cat] = (matchMap[cat] || 0) + 1;
    }});
    if (chartMatchCategories) chartMatchCategories.destroy();
    chartMatchCategories = new Chart(document.getElementById('chart-match-categories'), {{
        type: 'bar',
        data: {{
            labels: Object.keys(matchMap),
            datasets: [{{
                label: 'Leads',
                data: Object.values(matchMap),
                backgroundColor: '#00AFD9',
                borderRadius: 6
            }}]
        }},
        options: {{
            responsive: true,
            maintainAspectRatio: false,
            plugins: {{ legend: {{ display: false }} }},
            scales: {{
                x: {{ ticks: {{ color: '#94a3b8' }} }},
                y: {{ ticks: {{ color: '#94a3b8' }} }}
            }}
        }}
    }});

    // Unidades Volume & Revenue
    const unidMap = {{}};
    filteredData.forEach(r => {{
        const u = r.unidade || 'Não informada';
        if (!unidMap[u]) unidMap[u] = {{ leads: 0, revenue: 0 }};
        unidMap[u].leads++;
        unidMap[u].revenue += (r.total_gasto_pos_lead || 0);
    }});
    const sortedUnids = Object.keys(unidMap).sort((a,b) => unidMap[b].leads - unidMap[a].leads).slice(0, 6);

    if (chartUnidadeVol) chartUnidadeVol.destroy();
    chartUnidadeVol = new Chart(document.getElementById('chart-unidade-volume'), {{
        type: 'bar',
        data: {{
            labels: sortedUnids,
            datasets: [{{ label: 'Leads', data: sortedUnids.map(u => unidMap[u].leads), backgroundColor: '#4E358C', borderRadius: 6 }}]
        }},
        options: {{
            responsive: true,
            maintainAspectRatio: false,
            plugins: {{ legend: {{ display: false }} }},
            scales: {{ x: {{ ticks: {{ color: '#94a3b8' }} }}, y: {{ ticks: {{ color: '#94a3b8' }} }} }}
        }}
    }});

    if (chartUnidadeRev) chartUnidadeRev.destroy();
    chartUnidadeRev = new Chart(document.getElementById('chart-unidade-revenue'), {{
        type: 'bar',
        data: {{
            labels: sortedUnids,
            datasets: [{{ label: 'Receita (R$)', data: sortedUnids.map(u => unidMap[u].revenue), backgroundColor: '#A2C617', borderRadius: 6 }}]
        }},
        options: {{
            responsive: true,
            maintainAspectRatio: false,
            plugins: {{ legend: {{ display: false }} }},
            scales: {{
                x: {{ ticks: {{ color: '#94a3b8' }} }},
                y: {{ ticks: {{ color: '#94a3b8', callback: v => 'R$ ' + (v/1000).toFixed(0) + 'k' }} }}
            }}
        }}
    }});
}}

function updateChannelsTable() {{
    const chMap = {{}};
    filteredData.forEach(r => {{
        const s = r.fonte || 'Não informada';
        if (!chMap[s]) chMap[s] = {{ leads: 0, matched: 0, consultas: 0, sales: 0, revenue: 0 }};
        chMap[s].leads++;
        if (r.prontuario != null) chMap[s].matched++;
        if (r.consulta_date) chMap[s].consultas++;
        if (r.total_gasto_pos_lead > 0) {{
            chMap[s].sales++;
            chMap[s].revenue += r.total_gasto_pos_lead;
        }}
    }});

    const sortedChannels = Object.entries(chMap).sort((a,b) => b[1].revenue - a[1].revenue);
    const tbody = document.getElementById('table-channels-body');
    tbody.innerHTML = '';

    sortedChannels.forEach(([source, stat]) => {{
        const convCons = stat.matched > 0 ? (stat.consultas / stat.matched * 100).toFixed(1) + '%' : '0%';
        const convSale = stat.consultas > 0 ? (stat.sales / stat.consultas * 100).toFixed(1) + '%' : '0%';
        const ticket = stat.sales > 0 ? 'R$ ' + (stat.revenue / stat.sales).toLocaleString('pt-BR', {{ minimumFractionDigits: 2, maximumFractionDigits: 2 }}) : 'R$ 0,00';

        const tr = document.createElement('tr');
        tr.className = 'hover:bg-slate-800/40 transition-colors';
        tr.innerHTML = `
            <td class="px-4 py-2.5 font-medium text-white">${{source}}</td>
            <td class="px-4 py-2.5 text-right font-mono">${{stat.leads.toLocaleString('pt-BR')}}</td>
            <td class="px-4 py-2.5 text-right font-mono">${{stat.matched.toLocaleString('pt-BR')}}</td>
            <td class="px-4 py-2.5 text-right font-mono text-indigo-400">${{stat.consultas.toLocaleString('pt-BR')}}</td>
            <td class="px-4 py-2.5 text-right font-mono">${{convCons}}</td>
            <td class="px-4 py-2.5 text-right font-mono text-emerald-400">${{stat.sales.toLocaleString('pt-BR')}}</td>
            <td class="px-4 py-2.5 text-right font-mono">${{convSale}}</td>
            <td class="px-4 py-2.5 text-right font-mono font-bold text-amber-300">R$ ${{stat.revenue.toLocaleString('pt-BR', {{ minimumFractionDigits: 2, maximumFractionDigits: 2 }})}}</td>
            <td class="px-4 py-2.5 text-right font-mono text-slate-400">${{ticket}}</td>
        `;
        tbody.appendChild(tr);
    }});
}}

function getSearchedData() {{
    const q = document.getElementById('table-search').value.toLowerCase().trim();
    if (!q) return filteredData;
    return filteredData.filter(r => {{
        return (r.deal_name && r.deal_name.toLowerCase().includes(q)) ||
               (r.lead_email && r.lead_email.toLowerCase().includes(q)) ||
               (r.lead_id && r.lead_id.toLowerCase().includes(q)) ||
               (r.prontuario && String(r.prontuario).includes(q));
    }});
}}

function renderRecordsTable() {{
    const searched = getSearchedData();
    const totalRecords = searched.length;
    const totalPages = Math.ceil(totalRecords / pageSize) || 1;
    if (currentPage > totalPages) currentPage = totalPages;

    const startIdx = (currentPage - 1) * pageSize;
    const pageRecords = searched.slice(startIdx, startIdx + pageSize);

    document.getElementById('rec-count-showing').textContent = pageRecords.length;
    document.getElementById('rec-count-total').textContent = totalRecords.toLocaleString('pt-BR');
    document.getElementById('page-current').textContent = currentPage;
    document.getElementById('page-total').textContent = totalPages;

    document.getElementById('btn-prev-page').disabled = (currentPage <= 1);
    document.getElementById('btn-next-page').disabled = (currentPage >= totalPages);

    const tbody = document.getElementById('records-table-body');
    tbody.innerHTML = '';

    pageRecords.forEach(r => {{
        const tr = document.createElement('tr');
        tr.className = 'hover:bg-slate-800/40 transition-colors';
        const formattedRev = 'R$ ' + (r.total_gasto_pos_lead || 0).toLocaleString('pt-BR', {{ minimumFractionDigits: 2, maximumFractionDigits: 2 }});
        
        tr.innerHTML = `
            <td class="px-4 py-2 text-white">
                <div class="font-medium">${{r.deal_name || 'Sem Nome'}}</div>
                <div class="text-[10px] text-slate-500 font-mono">${{r.lead_id.substring(0, 12)}}...</div>
            </td>
            <td class="px-4 py-2">${{r.lead_from}}</td>
            <td class="px-4 py-2">${{r.fonte}}</td>
            <td class="px-4 py-2">${{r.unidade}}</td>
            <td class="px-4 py-2 font-bold text-sky-400">${{r.prontuario || '—'}}</td>
            <td class="px-4 py-2">${{r.consulta_date || '—'}}</td>
            <td class="px-4 py-2">
                <span class="px-2 py-0.5 rounded text-[10px] ${{r.consulta_status === 'Atendido' ? 'bg-emerald-500/20 text-emerald-300' : (r.consulta_status ? 'bg-slate-800 text-slate-300' : 'text-slate-600')}}">
                    ${{r.consulta_status || '—'}}
                </span>
            </td>
            <td class="px-4 py-2 text-right">${{r.qtd_pedidos_pos_lead}}</td>
            <td class="px-4 py-2 text-right font-bold ${{r.total_gasto_pos_lead > 0 ? 'text-amber-300' : 'text-slate-500'}}">${{formattedRev}}</td>
        `;
        tbody.appendChild(tr);
    }});
}}

function exportCSV() {{
    const headers = ['Lead_ID', 'Nome_Negocio', 'Data_Lead', 'Canal_Fonte', 'Campanha_ID', 'Unidade', 'Prontuario', 'Consulta_Data', 'Consulta_Status', 'Dias_Ate_Consulta', 'Qtd_Pedidos', 'Receita_Total_Pos_Lead', 'Dias_Ate_Venda'];
    const rows = filteredData.map(r => [
        `"${{r.lead_id}}"`,
        `"${{(r.deal_name || '').replace(/"/g, '""')}}"`,
        r.lead_from,
        `"${{r.fonte}}"`,
        `"${{r.campanha_id}}"`,
        `"${{r.unidade}}"`,
        r.prontuario || '',
        r.consulta_date || '',
        `"${{r.consulta_status || ''}}"`,
        r.dias_ate_primeira_consulta != null ? r.dias_ate_primeira_consulta : '',
        r.qtd_pedidos_pos_lead,
        r.total_gasto_pos_lead || 0,
        r.dias_ate_primeira_venda != null ? r.dias_ate_primeira_venda : ''
    ]);

    const csvContent = 'data:text/csv;charset=utf-8,' + [headers.join(','), ...rows.map(e => e.join(','))].join('\\n');
    const encodedUri = encodeURI(csvContent);
    const link = document.createElement('a');
    link.setAttribute('href', encodedUri);
    link.setAttribute('download', `huntington_leads_funil_${{new Date().toISOString().slice(0,10)}}.csv`);
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
}}

window.addEventListener('DOMContentLoaded', init);
</script>
</body>
</html>
"""

    with open(OUTPUT_HTML, 'w', encoding='utf-8') as f:
        f.write(html)

    logger.info(f"Executive BI Dashboard successfully saved to: {OUTPUT_HTML}")


def main():
    logger.info("=== LEADS FUNNEL DASHBOARD GENERATOR STARTED ===")
    logger.info(f"Database Path: {DUCKDB_PATH}")
    logger.info(f"Output File:   {OUTPUT_HTML}")
    t0 = time.time()
    try:
        data = fetch_data()
        generate_html(data)
        logger.info(f"Dashboard generated in {time.time()-t0:.2f}s")
        logger.info("=== LEADS FUNNEL DASHBOARD GENERATOR FINISHED SUCCESSFUL ===")
    except Exception as e:
        logger.error(f"Failed to generate dashboard: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    main()

