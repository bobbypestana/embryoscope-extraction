#!/usr/bin/env python3
"""
Leads Funnel Dashboard Generator — v2
Queries gold.leads_funil and compiles an interactive standalone HTML page.

Layout:
  Section A — Match Breakdown: how leads matched by channel
  Section B — Funnel: Leads → Matched → 1ª Consulta
  Section C — Status of 1st Consultation
  Tab: Individual Records table
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
    logger.info("Connecting to database and fetching leads_funil data...")
    con = duckdb.connect(DUCKDB_PATH, read_only=True)

    logger.info("Querying gold.leads_funil...")
    df = con.execute("""
        SELECT
            lead_id,
            strftime(lead_date, '%Y-%m-%d')                          AS lead_date,
            COALESCE(lead_email, '')                                  AS lead_email,
            prontuario,
            COALESCE(matched_email, '')                               AS matched_email,
            matched_flag,
            match_category,
            COALESCE(consulta_type, '')                               AS consulta_type,
            COALESCE(strftime(consulta_date, '%Y-%m-%d'), '')         AS consulta_date,
            COALESCE(consulta_status, '')                             AS consulta_status
        FROM gold.leads_funil
        ORDER BY lead_date DESC
    """).df()

    con.close()
    logger.info(f"Loaded {len(df):,} leads.")
    return df.to_dict(orient='records')


def generate_html(data):
    logger.info("Compiling HTML dashboard template...")

    total_leads   = len(data)
    matched_leads = sum(1 for r in data if r['prontuario'] is not None)
    with_consulta = sum(1 for r in data if r['consulta_date'] != '')
    match_pct     = matched_leads / total_leads * 100 if total_leads else 0
    cons_pct      = with_consulta / matched_leads * 100 if matched_leads else 0
    overall_pct   = with_consulta / total_leads * 100 if total_leads else 0

    json_data   = json.dumps(data)
    update_time = datetime.now().strftime('%d/%m/%Y %H:%M:%S')

    html = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Funil de Conversão — RD Station & Clinisys | Huntington</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap" rel="stylesheet">
    <style>
        * {{ font-family: 'Inter', sans-serif; }}
        body {{ background: radial-gradient(ellipse at top, #0d1629 0%, #020617 60%); color: #f1f5f9; min-height: 100vh; }}
        .glass {{ background: rgba(15,23,42,0.55); backdrop-filter: blur(20px); border: 1px solid rgba(255,255,255,0.06); }}
        .glow-sky {{ text-shadow: 0 0 16px rgba(14,165,233,0.4); }}
        ::-webkit-scrollbar {{ width: 5px; height: 5px; }}
        ::-webkit-scrollbar-track {{ background: #020617; }}
        ::-webkit-scrollbar-thumb {{ background: #1e3a5f; border-radius: 3px; }}
        ::-webkit-scrollbar-thumb:hover {{ background: #0ea5e9; }}
        .funnel-step {{ transition: all 0.4s cubic-bezier(.4,0,.2,1); }}
        .kpi-val {{ transition: all 0.3s ease; }}
        .bar-fill {{ transition: width 0.6s cubic-bezier(.4,0,.2,1); }}
        select, input {{ color-scheme: dark; }}
        select:focus, input:focus {{ outline: 2px solid #0ea5e9; outline-offset: 1px; }}
    </style>
</head>
<body class="p-4 md:p-6">
<div class="max-w-7xl mx-auto space-y-5">

    <!-- ═══════════════════════════════ HEADER ═══════════════════════════════ -->
    <header class="glass rounded-2xl px-6 py-5 flex flex-col lg:flex-row lg:items-center justify-between gap-4">
        <div>
            <h1 class="text-xl font-bold text-sky-400 glow-sky flex items-center gap-2">
                <svg class="w-6 h-6 flex-shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2"
                          d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z"/>
                </svg>
                Funil de Conversão — RD Station &amp; Clinisys
            </h1>
            <p class="text-slate-400 text-sm mt-1">Acoplamento de leads com prontuários · 1ª consulta agendada</p>
        </div>
        <div class="flex items-center gap-2 text-xs bg-slate-900/70 border border-slate-700/60 text-slate-400 px-3 py-2 rounded-lg">
            <span class="w-2 h-2 rounded-full bg-emerald-400 animate-pulse"></span>
            Atualizado em: <span class="font-semibold text-slate-200 ml-1">{update_time}</span>
        </div>
    </header>

    <!-- ═══════════════════════════════ FILTERS ═══════════════════════════════ -->
    <section class="glass rounded-2xl px-6 py-4">
        <div class="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-5 gap-3">
            <div class="flex flex-col gap-1">
                <label class="text-[10px] font-semibold uppercase tracking-wider text-slate-500">Canal Match</label>
                <select id="f-category" class="bg-slate-900 border border-slate-700/80 text-slate-200 text-xs rounded-lg px-3 py-2 focus:outline-none">
                    <option value="all">Todos</option>
                    <option value="email_esposa">E-mail Esposa</option>
                    <option value="email_marido">E-mail Marido</option>
                    <option value="celular_esposa">Celular Esposa</option>
                    <option value="celular_marido">Celular Marido</option>
                    <option value="celular_geral">Celular Geral</option>
                    <option value="unmatched">Sem Match</option>
                </select>
            </div>
            <div class="flex flex-col gap-1">
                <label class="text-[10px] font-semibold uppercase tracking-wider text-slate-500">Consulta</label>
                <select id="f-consulta" class="bg-slate-900 border border-slate-700/80 text-slate-200 text-xs rounded-lg px-3 py-2 focus:outline-none">
                    <option value="all">Todas</option>
                    <option value="yes">Com consulta</option>
                    <option value="no">Sem consulta</option>
                </select>
            </div>
            <div class="flex flex-col gap-1">
                <label class="text-[10px] font-semibold uppercase tracking-wider text-slate-500">Status Consulta</label>
                <select id="f-status" class="bg-slate-900 border border-slate-700/80 text-slate-200 text-xs rounded-lg px-3 py-2 focus:outline-none">
                    <option value="all">Todos</option>
                </select>
            </div>
            <div class="flex flex-col gap-1">
                <label class="text-[10px] font-semibold uppercase tracking-wider text-slate-500">Período</label>
                <select id="f-period" class="bg-slate-900 border border-slate-700/80 text-slate-200 text-xs rounded-lg px-3 py-2 focus:outline-none">
                    <option value="all">Todo o período</option>
                    <option value="last30">Últimos 30 dias</option>
                    <option value="last90">Últimos 90 dias</option>
                    <option value="ytd">Ano atual (YTD)</option>
                    <option value="custom">Personalizado</option>
                </select>
            </div>
            <div id="custom-dates" class="hidden flex-col gap-1 col-span-2 lg:col-span-1">
                <label class="text-[10px] font-semibold uppercase tracking-wider text-slate-500">Datas</label>
                <div class="flex gap-2">
                    <input type="date" id="f-start" class="flex-1 bg-slate-900 border border-slate-700/80 text-slate-200 text-xs rounded-lg px-2 py-2 focus:outline-none">
                    <input type="date" id="f-end"   class="flex-1 bg-slate-900 border border-slate-700/80 text-slate-200 text-xs rounded-lg px-2 py-2 focus:outline-none">
                </div>
            </div>
        </div>
    </section>

    <!-- ═══════════════════════════════ TABS ═══════════════════════════════ -->
    <div class="flex gap-2">
        <button id="btn-overview" class="tab-btn active-tab px-4 py-2 text-xs font-semibold rounded-lg transition-all flex items-center gap-1.5">
            <svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z"/>
            </svg>
            Visão Geral
        </button>
        <button id="btn-details" class="tab-btn px-4 py-2 text-xs font-semibold rounded-lg transition-all flex items-center gap-1.5">
            <svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3 10h18M3 14h18m-9-4v8m-7 0h14a2 2 0 002-2V8a2 2 0 00-2-2H5a2 2 0 00-2 2v8a2 2 0 002 2z"/>
            </svg>
            Registros
        </button>
    </div>
    <style>
        .tab-btn {{ background: rgba(30,41,59,.6); color: #94a3b8; border: 1px solid rgba(255,255,255,0.05); }}
        .tab-btn:hover {{ background: rgba(51,65,85,.7); color: #e2e8f0; }}
        .active-tab {{ background: #0284c7 !important; color: #fff !important; border-color: #0ea5e9 !important; }}
    </style>

    <!-- ═══════════════════════════════ OVERVIEW TAB ═══════════════════════════════ -->
    <div id="tab-overview" class="space-y-5">

        <!-- ── Section A: Match breakdown ── -->
        <section class="glass rounded-2xl p-6">
            <h2 class="text-xs font-bold uppercase tracking-widest text-slate-400 mb-5 flex items-center gap-2">
                <span class="w-1.5 h-4 rounded-full bg-sky-500 inline-block"></span>
                A · Acoplamento de Leads por Canal
            </h2>

            <!-- KPI strip -->
            <div class="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-3 mb-6">
                <div class="bg-sky-950/50 border border-sky-800/50 rounded-xl p-4 text-center">
                    <div class="text-[10px] font-semibold uppercase tracking-wider text-sky-400/70 mb-1">E-mail Esposa</div>
                    <div id="kpi-email-esposa" class="text-2xl font-bold text-sky-300 kpi-val">—</div>
                    <div id="kpi-email-esposa-pct" class="text-[10px] text-sky-400/60 mt-0.5">—</div>
                </div>
                <div class="bg-indigo-950/50 border border-indigo-800/50 rounded-xl p-4 text-center">
                    <div class="text-[10px] font-semibold uppercase tracking-wider text-indigo-400/70 mb-1">E-mail Marido</div>
                    <div id="kpi-email-marido" class="text-2xl font-bold text-indigo-300 kpi-val">—</div>
                    <div id="kpi-email-marido-pct" class="text-[10px] text-indigo-400/60 mt-0.5">—</div>
                </div>
                <div class="bg-teal-950/50 border border-teal-800/50 rounded-xl p-4 text-center">
                    <div class="text-[10px] font-semibold uppercase tracking-wider text-teal-400/70 mb-1">Celular Esposa</div>
                    <div id="kpi-cel-esposa" class="text-2xl font-bold text-teal-300 kpi-val">—</div>
                    <div id="kpi-cel-esposa-pct" class="text-[10px] text-teal-400/60 mt-0.5">—</div>
                </div>
                <div class="bg-emerald-950/50 border border-emerald-800/50 rounded-xl p-4 text-center">
                    <div class="text-[10px] font-semibold uppercase tracking-wider text-emerald-400/70 mb-1">Celular Marido</div>
                    <div id="kpi-cel-marido" class="text-2xl font-bold text-emerald-300 kpi-val">—</div>
                    <div id="kpi-cel-marido-pct" class="text-[10px] text-emerald-400/60 mt-0.5">—</div>
                </div>
                <div class="bg-violet-950/50 border border-violet-800/50 rounded-xl p-4 text-center">
                    <div class="text-[10px] font-semibold uppercase tracking-wider text-violet-400/70 mb-1">Celular Geral</div>
                    <div id="kpi-cel-geral" class="text-2xl font-bold text-violet-300 kpi-val">—</div>
                    <div id="kpi-cel-geral-pct" class="text-[10px] text-violet-400/60 mt-0.5">—</div>
                </div>
                <div class="bg-slate-900/60 border border-slate-700/40 rounded-xl p-4 text-center">
                    <div class="text-[10px] font-semibold uppercase tracking-wider text-slate-500 mb-1">Sem Match</div>
                    <div id="kpi-unmatched" class="text-2xl font-bold text-slate-400 kpi-val">—</div>
                    <div id="kpi-unmatched-pct" class="text-[10px] text-slate-500 mt-0.5">—</div>
                </div>
            </div>

            <!-- Horizontal stacked bar -->
            <div class="space-y-2">
                <div class="flex justify-between text-[10px] text-slate-500 font-semibold uppercase tracking-wider">
                    <span>Distribuição dos <span id="bar-total">—</span> leads</span>
                    <span>100%</span>
                </div>
                <div class="relative h-10 rounded-xl overflow-hidden bg-slate-900/60 flex">
                    <div id="bar-email-esposa" class="bar-fill h-full bg-sky-500 transition-all" style="width:0%"></div>
                    <div id="bar-email-marido" class="bar-fill h-full bg-indigo-500 transition-all" style="width:0%"></div>
                    <div id="bar-cel-esposa"   class="bar-fill h-full bg-teal-500 transition-all" style="width:0%"></div>
                    <div id="bar-cel-marido"   class="bar-fill h-full bg-emerald-500 transition-all" style="width:0%"></div>
                    <div id="bar-cel-geral"    class="bar-fill h-full bg-violet-500 transition-all" style="width:0%"></div>
                    <div id="bar-unmatched"    class="bar-fill h-full bg-slate-700/60 transition-all" style="width:0%"></div>
                </div>
                <div class="flex flex-wrap gap-3 text-[10px] text-slate-400">
                    <span class="flex items-center gap-1"><span class="w-2 h-2 rounded-sm bg-sky-500"></span>E-mail Esposa</span>
                    <span class="flex items-center gap-1"><span class="w-2 h-2 rounded-sm bg-indigo-500"></span>E-mail Marido</span>
                    <span class="flex items-center gap-1"><span class="w-2 h-2 rounded-sm bg-teal-500"></span>Celular Esposa</span>
                    <span class="flex items-center gap-1"><span class="w-2 h-2 rounded-sm bg-emerald-500"></span>Celular Marido</span>
                    <span class="flex items-center gap-1"><span class="w-2 h-2 rounded-sm bg-violet-500"></span>Celular Geral</span>
                    <span class="flex items-center gap-1"><span class="w-2 h-2 rounded-sm bg-slate-600"></span>Sem Match</span>
                </div>
            </div>
        </section>

        <!-- ── Section B + C grid ── -->
        <div class="grid grid-cols-1 lg:grid-cols-5 gap-5">

            <!-- Section B: Funnel -->
            <section class="glass rounded-2xl p-6 lg:col-span-2 flex flex-col">
                <h2 class="text-xs font-bold uppercase tracking-widest text-slate-400 mb-5 flex items-center gap-2">
                    <span class="w-1.5 h-4 rounded-full bg-emerald-500 inline-block"></span>
                    B · Funil de Conversão
                </h2>

                <div class="flex-1 flex flex-col justify-center space-y-4">
                    <!-- Step 1 -->
                    <div class="funnel-step space-y-1.5">
                        <div class="flex justify-between items-center text-xs">
                            <span class="text-slate-300 font-semibold">① Leads RD Station</span>
                            <span id="f1-val" class="font-bold text-slate-100">{total_leads:,}</span>
                        </div>
                        <div class="h-9 rounded-lg overflow-hidden bg-slate-800/50 relative flex items-center px-3">
                            <div class="absolute inset-0 bg-sky-500/30"></div>
                            <span class="relative text-xs font-bold text-sky-300">100%</span>
                        </div>
                    </div>

                    <div class="flex items-center gap-2 text-slate-600 text-xs pl-2">
                        <svg class="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7"/></svg>
                        Acoplamento por e-mail / celular
                    </div>

                    <!-- Step 2 -->
                    <div class="funnel-step space-y-1.5">
                        <div class="flex justify-between items-center text-xs">
                            <span class="text-slate-300 font-semibold">② Prontuários Identificados</span>
                            <span id="f2-val" class="font-bold text-slate-100">—</span>
                        </div>
                        <div class="h-9 rounded-lg overflow-hidden bg-slate-800/50 relative flex items-center px-3">
                            <div id="f2-bar" class="bar-fill absolute left-0 top-0 bottom-0 bg-sky-400/30" style="width:{match_pct:.1f}%"></div>
                            <span id="f2-pct" class="relative text-xs font-bold text-sky-400">{match_pct:.1f}%</span>
                        </div>
                    </div>

                    <div class="flex items-center gap-2 text-slate-600 text-xs pl-2">
                        <svg class="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7"/></svg>
                        Consulta de reprodução / preservação
                    </div>

                    <!-- Step 3 -->
                    <div class="funnel-step space-y-1.5">
                        <div class="flex justify-between items-center text-xs">
                            <span class="text-slate-300 font-semibold">③ 1ª Consulta Agendada</span>
                            <span id="f3-val" class="font-bold text-slate-100">—</span>
                        </div>
                        <div class="h-9 rounded-lg overflow-hidden bg-slate-800/50 relative flex items-center px-3">
                            <div id="f3-bar" class="bar-fill absolute left-0 top-0 bottom-0 bg-emerald-500/30" style="width:{overall_pct:.1f}%"></div>
                            <span id="f3-pct" class="relative text-xs font-bold text-emerald-400">{overall_pct:.1f}% dos leads</span>
                        </div>
                        <div class="text-right text-[10px] text-slate-500">
                            <span id="f3-of-matched">—</span> dos acoplados
                        </div>
                    </div>
                </div>
            </section>

            <!-- Section C: Consultation Status -->
            <section class="glass rounded-2xl p-6 lg:col-span-3 flex flex-col">
                <h2 class="text-xs font-bold uppercase tracking-widest text-slate-400 mb-5 flex items-center gap-2">
                    <span class="w-1.5 h-4 rounded-full bg-amber-500 inline-block"></span>
                    C · Status da 1ª Consulta
                </h2>

                <div class="flex-1 flex flex-col lg:flex-row gap-6">
                    <!-- Donut chart -->
                    <div class="flex-shrink-0 flex items-center justify-center">
                        <div class="relative w-48 h-48">
                            <canvas id="chart-status"></canvas>
                            <div class="absolute inset-0 flex flex-col items-center justify-center pointer-events-none">
                                <span id="donut-center-val" class="text-2xl font-bold text-slate-100">—</span>
                                <span class="text-[10px] text-slate-500 font-medium">consultas</span>
                            </div>
                        </div>
                    </div>
                    <!-- Status bars -->
                    <div id="status-bars" class="flex-1 space-y-2 overflow-y-auto max-h-60 pr-1"></div>
                </div>
            </section>
        </div>
    </div>

    <!-- ═══════════════════════════════ DETAILS TAB ═══════════════════════════════ -->
    <div id="tab-details" class="hidden space-y-4">
        <div class="glass rounded-xl px-5 py-3 flex flex-col sm:flex-row gap-3 justify-between items-center">
            <div class="relative w-full sm:max-w-sm">
                <input type="text" id="search-input" placeholder="Buscar por lead ID, e-mail ou prontuário..."
                       class="w-full bg-slate-900 border border-slate-700/80 text-slate-200 text-xs rounded-lg pl-9 pr-4 py-2.5 focus:outline-none">
                <svg class="w-4 h-4 text-slate-500 absolute left-3 top-2.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z"/>
                </svg>
            </div>
            <span class="text-xs text-slate-500">
                <span id="pag-start">0</span>–<span id="pag-end">0</span> de <span id="pag-total" class="text-slate-300 font-semibold">0</span>
            </span>
        </div>

        <div class="glass rounded-2xl overflow-hidden">
            <div class="overflow-x-auto">
                <table class="w-full text-xs text-left text-slate-300">
                    <thead class="bg-slate-900/80 text-[10px] font-bold uppercase tracking-wider text-slate-500">
                        <tr>
                            <th class="px-5 py-3.5">Lead / Data</th>
                            <th class="px-5 py-3.5">E-mail Lead</th>
                            <th class="px-5 py-3.5">Prontuário</th>
                            <th class="px-5 py-3.5">Canal Match</th>
                            <th class="px-5 py-3.5">Match Exato</th>
                            <th class="px-5 py-3.5">1ª Consulta</th>
                            <th class="px-5 py-3.5">Data Consulta</th>
                            <th class="px-5 py-3.5">Status</th>
                        </tr>
                    </thead>
                    <tbody id="tbl-body" class="divide-y divide-slate-800/50"></tbody>
                </table>
            </div>
            <div class="bg-slate-900/60 px-5 py-3 border-t border-slate-800/60 flex items-center justify-between">
                <button id="btn-prev" class="px-3 py-1.5 text-xs font-semibold bg-slate-800 rounded-lg hover:bg-slate-700 disabled:opacity-40 disabled:cursor-not-allowed transition-colors">← Anterior</button>
                <span class="text-xs text-slate-500">Pág. <span id="page-num">1</span> / <span id="page-max">1</span></span>
                <button id="btn-next" class="px-3 py-1.5 text-xs font-semibold bg-slate-800 rounded-lg hover:bg-slate-700 disabled:opacity-40 disabled:cursor-not-allowed transition-colors">Próximo →</button>
            </div>
        </div>
    </div>

</div>

<!-- DATA -->
<script>const leadsData = {json_data};</script>

<!-- APP -->
<script>
// ─── Constants ───────────────────────────────────────────────────────────────
const CATEGORY_META = {{
    email_esposa:  {{ label: 'E-mail Esposa',  color: '#38bdf8', barId: 'bar-email-esposa', kpiId: 'kpi-email-esposa', pctId: 'kpi-email-esposa-pct' }},
    email_marido:  {{ label: 'E-mail Marido',  color: '#818cf8', barId: 'bar-email-marido', kpiId: 'kpi-email-marido', pctId: 'kpi-email-marido-pct' }},
    celular_esposa:{{ label: 'Celular Esposa', color: '#2dd4bf', barId: 'bar-cel-esposa',   kpiId: 'kpi-cel-esposa',   pctId: 'kpi-cel-esposa-pct'   }},
    celular_marido:{{ label: 'Celular Marido', color: '#34d399', barId: 'bar-cel-marido',   kpiId: 'kpi-cel-marido',   pctId: 'kpi-cel-marido-pct'   }},
    celular_geral: {{ label: 'Celular Geral',  color: '#a78bfa', barId: 'bar-cel-geral',    kpiId: 'kpi-cel-geral',    pctId: 'kpi-cel-geral-pct'    }},
    unmatched:     {{ label: 'Sem Match',       color: '#475569', barId: 'bar-unmatched',    kpiId: 'kpi-unmatched',    pctId: 'kpi-unmatched-pct'    }},
}};

const STATUS_COLORS = {{
    'Agendada':               '#38bdf8',
    'Atendido':               '#34d399',
    'Sim':                    '#6ee7b7',
    'Em Atendimento':         '#67e8f9',
    'Remarcou':               '#fbbf24',
    'Remarcou (Profissional)':'#f59e0b',
    'Desmarcou':              '#fb923c',
    'Desmarcou (Profissional)':'#f97316',
    'Faltou':                 '#f87171',
    'Não':                    '#ef4444',
}};

const PAGE_SIZE = 20;
let filtered = [...leadsData];
let currentPage = 1;
let statusChart = null;

// ─── Filters DOM ────────────────────────────────────────────────────────────
const fCat    = document.getElementById('f-category');
const fCons   = document.getElementById('f-consulta');
const fStatus = document.getElementById('f-status');
const fPeriod = document.getElementById('f-period');
const fStart  = document.getElementById('f-start');
const fEnd    = document.getElementById('f-end');
const customDates = document.getElementById('custom-dates');
const searchEl = document.getElementById('search-input');

// ─── Populate status filter ──────────────────────────────────────────────────
function initFilters() {{
    const statuses = new Set();
    leadsData.forEach(r => {{ if (r.consulta_status) statuses.add(r.consulta_status); }});
    Array.from(statuses).sort().forEach(s => {{
        const o = document.createElement('option');
        o.value = s; o.textContent = s;
        fStatus.appendChild(o);
    }});
}}

// ─── Filter engine ──────────────────────────────────────────────────────────
function runFilters() {{
    const cat    = fCat.value;
    const cons   = fCons.value;
    const status = fStatus.value;
    const period = fPeriod.value;

    let start = null, end = null;
    const today = new Date();
    if (period === 'last30')  {{ start = new Date(); start.setDate(today.getDate()-30); end = today; }}
    else if (period === 'last90') {{ start = new Date(); start.setDate(today.getDate()-90); end = today; }}
    else if (period === 'ytd')    {{ start = new Date(today.getFullYear(),0,1); end = today; }}
    else if (period === 'custom') {{
        if (fStart.value) start = new Date(fStart.value+'T00:00:00');
        if (fEnd.value)   end   = new Date(fEnd.value+'T23:59:59');
    }}

    const q = (searchEl?.value || '').toLowerCase().trim();

    filtered = leadsData.filter(r => {{
        if (cat    !== 'all' && r.match_category   !== cat)    return false;
        if (cons   === 'yes' && r.consulta_date    === '')     return false;
        if (cons   === 'no'  && r.consulta_date    !== '')     return false;
        if (status !== 'all' && r.consulta_status  !== status) return false;
        if (start || end) {{
            const d = new Date(r.lead_date+'T00:00:00');
            if (start && d < start) return false;
            if (end   && d > end)   return false;
        }}
        if (q) {{
            const s = String(r.lead_id)+r.lead_email+String(r.prontuario ?? '');
            if (!s.toLowerCase().includes(q)) return false;
        }}
        return true;
    }});

    currentPage = 1;
    render();
}}

// ─── Render all ─────────────────────────────────────────────────────────────
function render() {{
    renderSectionA();
    renderSectionB();
    renderSectionC();
    renderTable();
}}

// ─── Section A ──────────────────────────────────────────────────────────────
function renderSectionA() {{
    const total = filtered.length;
    document.getElementById('bar-total').textContent = total.toLocaleString('pt-BR');

    const counts = {{}};
    Object.keys(CATEGORY_META).forEach(k => counts[k] = 0);
    filtered.forEach(r => {{ if (counts[r.match_category] !== undefined) counts[r.match_category]++; }});

    Object.entries(CATEGORY_META).forEach(([key, meta]) => {{
        const n   = counts[key] || 0;
        const pct = total > 0 ? (n/total*100) : 0;
        document.getElementById(meta.kpiId).textContent = n.toLocaleString('pt-BR');
        document.getElementById(meta.pctId).textContent = pct.toFixed(1)+'% dos leads';
        document.getElementById(meta.barId).style.width  = pct.toFixed(2)+'%';
        document.getElementById(meta.barId).title        = `${{meta.label}}: ${{n.toLocaleString('pt-BR')}} (${{pct.toFixed(1)}}%)`;
    }});
}}

// ─── Section B ──────────────────────────────────────────────────────────────
function renderSectionB() {{
    const total   = filtered.length;
    const matched = filtered.filter(r => r.prontuario !== null).length;
    const cons    = filtered.filter(r => r.consulta_date !== '').length;
    const mPct    = total   > 0 ? matched/total*100 : 0;
    const oPct    = total   > 0 ? cons/total*100    : 0;
    const cPct    = matched > 0 ? cons/matched*100  : 0;

    document.getElementById('f2-val').textContent  = matched.toLocaleString('pt-BR');
    document.getElementById('f2-pct').textContent  = mPct.toFixed(1)+'%';
    document.getElementById('f2-bar').style.width  = mPct.toFixed(1)+'%';
    document.getElementById('f3-val').textContent  = cons.toLocaleString('pt-BR');
    document.getElementById('f3-pct').textContent  = oPct.toFixed(1)+'% dos leads';
    document.getElementById('f3-bar').style.width  = oPct.toFixed(1)+'%';
    document.getElementById('f3-of-matched').textContent = cPct.toFixed(1)+'% dos acoplados';
}}

// ─── Section C ──────────────────────────────────────────────────────────────
function renderSectionC() {{
    const withCons = filtered.filter(r => r.consulta_date !== '');
    const counts   = {{}};
    withCons.forEach(r => {{ counts[r.consulta_status] = (counts[r.consulta_status]||0)+1; }});

    // Sort by count desc
    const sorted = Object.entries(counts).sort((a,b) => b[1]-a[1]);
    const labels = sorted.map(([k]) => k);
    const vals   = sorted.map(([,v]) => v);
    const colors = labels.map(l => STATUS_COLORS[l] || '#64748b');
    const total  = vals.reduce((a,b) => a+b, 0);

    document.getElementById('donut-center-val').textContent = total.toLocaleString('pt-BR');

    // Donut chart
    if (statusChart) statusChart.destroy();
    const ctx = document.getElementById('chart-status').getContext('2d');
    statusChart = new Chart(ctx, {{
        type: 'doughnut',
        data: {{ labels, datasets: [{{ data: vals, backgroundColor: colors, borderColor: '#0a0f1e', borderWidth: 3 }}] }},
        options: {{
            responsive: true, maintainAspectRatio: false, cutout: '68%',
            plugins: {{ legend: {{ display: false }}, tooltip: {{
                callbacks: {{
                    label: ctx => ` ${{ctx.label}}: ${{ctx.parsed.toLocaleString('pt-BR')}} (${{(ctx.parsed/total*100).toFixed(1)}}%)`
                }}
            }} }}
        }}
    }});

    // Horizontal bar legend
    const barsEl = document.getElementById('status-bars');
    barsEl.innerHTML = '';
    sorted.forEach(([status, n]) => {{
        const pct   = total > 0 ? (n/total*100) : 0;
        const color = STATUS_COLORS[status] || '#64748b';
        barsEl.innerHTML += `
            <div class="space-y-0.5">
                <div class="flex justify-between items-center text-[10px]">
                    <span class="font-semibold text-slate-300">${{status || '—'}}</span>
                    <span class="text-slate-400">${{n.toLocaleString('pt-BR')}} · ${{pct.toFixed(1)}}%</span>
                </div>
                <div class="h-2 rounded-full bg-slate-800/70 overflow-hidden">
                    <div class="bar-fill h-full rounded-full" style="width:${{pct.toFixed(1)}}%; background:${{color}};"></div>
                </div>
            </div>`;
    }});
}}

// ─── Table ──────────────────────────────────────────────────────────────────
function categoryBadge(cat) {{
    const meta = CATEGORY_META[cat];
    if (!meta) return `<span class="text-slate-500">—</span>`;
    return `<span class="px-2 py-0.5 rounded-full text-[10px] font-semibold border"
            style="color:${{meta.color}};border-color:${{meta.color}}40;background:${{meta.color}}15">${{meta.label}}</span>`;
}}

function statusBadge(s) {{
    if (!s) return '<span class="text-slate-600">—</span>';
    const c = STATUS_COLORS[s] || '#64748b';
    return `<span class="px-2 py-0.5 rounded text-[10px] font-semibold"
            style="color:${{c}};background:${{c}}18;border:1px solid ${{c}}35">${{s}}</span>`;
}}

function renderTable() {{
    const total = filtered.length;
    const pages = Math.ceil(total/PAGE_SIZE)||1;
    if (currentPage > pages) currentPage = pages;
    const s = (currentPage-1)*PAGE_SIZE, e = Math.min(s+PAGE_SIZE, total);

    document.getElementById('pag-start').textContent  = total > 0 ? s+1 : 0;
    document.getElementById('pag-end').textContent    = e;
    document.getElementById('pag-total').textContent  = total.toLocaleString('pt-BR');
    document.getElementById('page-num').textContent   = currentPage;
    document.getElementById('page-max').textContent   = pages;
    document.getElementById('btn-prev').disabled      = currentPage === 1;
    document.getElementById('btn-next').disabled      = currentPage === pages;

    const body = document.getElementById('tbl-body');
    body.innerHTML = '';
    if (total === 0) {{
        body.innerHTML = `<tr><td colspan="8" class="px-5 py-10 text-center text-slate-600">Nenhum resultado.</td></tr>`;
        return;
    }}
    filtered.slice(s,e).forEach(r => {{
        const tr = document.createElement('tr');
        tr.className = 'hover:bg-slate-800/25 transition-colors';
        tr.innerHTML = `
            <td class="px-5 py-3">
                <div class="font-bold text-sky-400">${{r.lead_id}}</div>
                <div class="text-[10px] text-slate-600 mt-0.5">${{r.lead_date}}</div>
            </td>
            <td class="px-5 py-3 text-slate-400 max-w-[180px] truncate" title="${{r.lead_email}}">${{r.lead_email||'—'}}</td>
            <td class="px-5 py-3 font-bold text-slate-200">${{r.prontuario ?? '<span class="text-slate-600 font-normal">—</span>'}}</td>
            <td class="px-5 py-3">${{categoryBadge(r.match_category)}}</td>
            <td class="px-5 py-3 text-slate-500 max-w-[180px] truncate text-[10px]" title="${{r.matched_email}}">${{r.matched_email||'—'}}</td>
            <td class="px-5 py-3 text-slate-400 max-w-[160px] truncate text-[10px]" title="${{r.consulta_type}}">${{r.consulta_type||'—'}}</td>
            <td class="px-5 py-3 text-slate-400 text-[10px]">${{r.consulta_date||'—'}}</td>
            <td class="px-5 py-3">${{statusBadge(r.consulta_status)}}</td>`;
        body.appendChild(tr);
    }});
}}

// ─── Tab switching ───────────────────────────────────────────────────────────
document.getElementById('btn-overview').addEventListener('click', () => {{
    document.getElementById('tab-overview').classList.remove('hidden');
    document.getElementById('tab-details').classList.add('hidden');
    document.getElementById('btn-overview').classList.add('active-tab');
    document.getElementById('btn-details').classList.remove('active-tab');
}});
document.getElementById('btn-details').addEventListener('click', () => {{
    document.getElementById('tab-details').classList.remove('hidden');
    document.getElementById('tab-overview').classList.add('hidden');
    document.getElementById('btn-details').classList.add('active-tab');
    document.getElementById('btn-overview').classList.remove('active-tab');
}});

// ─── Event listeners ─────────────────────────────────────────────────────────
[fCat, fCons, fStatus, fStart, fEnd].forEach(el => el.addEventListener('change', runFilters));
fPeriod.addEventListener('change', e => {{
    customDates.classList.toggle('hidden', e.target.value !== 'custom');
    customDates.classList.toggle('flex',   e.target.value === 'custom');
    runFilters();
}});
searchEl.addEventListener('input', runFilters);
document.getElementById('btn-prev').addEventListener('click', () => {{ currentPage--; renderTable(); }});
document.getElementById('btn-next').addEventListener('click', () => {{ currentPage++; renderTable(); }});

// ─── Boot ────────────────────────────────────────────────────────────────────
initFilters();
runFilters();
</script>
</body>
</html>"""

    with open(OUTPUT_HTML, 'w', encoding='utf-8') as f:
        f.write(html)
    logger.info(f"Dashboard written to {OUTPUT_HTML} ({os.path.getsize(OUTPUT_HTML)/1024/1024:.2f} MB)")


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
