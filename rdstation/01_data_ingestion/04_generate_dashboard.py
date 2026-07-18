#!/usr/bin/env python3
"""
RD Station CRM Dashboard Generator
Queries DuckDB and compiles an interactive standalone HTML page with client-side filtering.
Includes a centered sales funnel chart, tabbed sheets, and default 'Todos' filters.
"""

import os
import yaml
import json
import logging
import duckdb
from datetime import datetime

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

# Load parameters
PARAMS_PATH = os.path.join(SCRIPT_DIR, 'params.yml')
with open(PARAMS_PATH, 'r') as f:
    config = yaml.safe_load(f)

DUCKDB_PATH = config['duckdb_path']
OUTPUT_HTML = os.path.join(SCRIPT_DIR, 'dashboard.html')

def fetch_data():
    logger.info("Connecting to database and fetching raw records...")
    con = duckdb.connect(DUCKDB_PATH, read_only=True)
    
    # Load lost reasons mapping if exists
    lost_reasons_path = os.path.join(SCRIPT_DIR, 'lost_reasons.json')
    lost_reasons_map = {}
    if os.path.exists(lost_reasons_path):
        try:
            with open(lost_reasons_path, 'r', encoding='utf-8') as f:
                lost_reasons_map = json.load(f)
            logger.info(f"Loaded lost reasons mapping with {len(lost_reasons_map)} keys.")
        except Exception as e:
            logger.warning(f"Could not load lost reasons mapping: {e}")
            
    # 1. Fetch all deals for client-side aggregation
    logger.info("Querying deals...")
    deals_rows = con.execute("""
        SELECT 
            d.id,
            d.name,
            d.status,
            COALESCE(p.name, 'Sem Funil') AS pipeline,
            COALESCE(s.name, 'Sem Etapa') AS stage,
            COALESCE(u.name, 'Sem Responsável') AS agent,
            COALESCE(d.custom_unidade, 'Sem Unidade') AS clinic,
            COALESCE(d.custom_como_conheceu_a_huntington, 'Sem Canal') AS channel,
            COALESCE(d.custom_procurou_por, 'Não Informado') AS treatment,
            COALESCE(TRY_CAST(d.total_price AS DOUBLE), 0.0) AS revenue,
            strftime(d.created_at, '%Y-%m-%d') AS created_at,
            strftime(d.updated_at, '%Y-%m-%d') AS updated_at,
            strftime(d.closed_at, '%Y-%m-%d') AS closed_at,
            COALESCE(d.lost_reason_id, 'Sem Motivo') AS lost_reason
        FROM silver.deals d
        LEFT JOIN silver.pipelines p ON d.pipeline_id = p.id
        LEFT JOIN silver.stages s ON d.stage_id = s.id AND d.pipeline_id = s.pipeline_id
        LEFT JOIN silver.users u ON d.owner_id = u.id
    """).fetchall()
    
    deals = []
    for r in deals_rows:
        reason_id = r[13]
        reason_name = lost_reasons_map.get(reason_id, reason_id)
        deals.append({
            "id": r[0],
            "name": r[1],
            "status": r[2],
            "pipeline": r[3],
            "stage": r[4],
            "agent": r[5],
            "clinic": r[6],
            "channel": r[7],
            "treatment": r[8],
            "revenue": r[9],
            "created_at": r[10],
            "updated_at": r[11],
            "closed_at": r[12],
            "lost_reason": reason_name
        })
        
    # 2. Fetch pipelines and stages in order for funnel chart mapping
    logger.info("Querying pipelines and stages order...")
    stages_rows = con.execute("""
        SELECT 
            p.name AS pipeline_name,
            s.name AS stage_name,
            s.order AS stage_order
        FROM silver.stages s
        JOIN silver.pipelines p ON s.pipeline_id = p.id
        ORDER BY pipeline_name, stage_order ASC
    """).fetchall()
    
    stages = {}
    for pipe, stage, order in stages_rows:
        if pipe not in stages:
            stages[pipe] = []
        stages[pipe].append(stage)
        
    con.close()
    logger.info(f"Successfully loaded {len(deals)} deals and {len(stages)} pipelines.")
    return deals, stages

HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Huntington - RD Station CRM Sales Dashboard</title>
    
    <!-- Tailwind CSS & Google Fonts -->
    <script src="https://cdn.tailwindcss.com"></script>
    <link href="https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700&display=swap" rel="stylesheet">
    
    <!-- Chart.js -->
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    
    <style>
        body {
            font-family: 'Outfit', sans-serif;
            background-color: #0b0f19;
            color: #f8fafc;
        }
        .glass-panel {
            background: rgba(15, 23, 42, 0.45);
            backdrop-filter: blur(12px);
            -webkit-backdrop-filter: blur(12px);
            border: 1px solid rgba(255, 255, 255, 0.05);
            box-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.3);
        }
        .text-glow {
            text-shadow: 0 0 10px rgba(14, 165, 233, 0.3);
        }
        select, input {
            transition: all 0.2s ease;
        }
        select:focus, input:focus {
            border-color: #3b82f6;
            box-shadow: 0 0 0 2px rgba(59, 130, 246, 0.2);
        }
        /* Scrollbars styling */
        ::-webkit-scrollbar {
            width: 6px;
            height: 6px;
        }
        ::-webkit-scrollbar-track {
            background: #0b0f19;
        }
        ::-webkit-scrollbar-thumb {
            background: #1e293b;
            border-radius: 3px;
        }
        ::-webkit-scrollbar-thumb:hover {
            background: #0ea5e9;
        }
    </style>
</head>
<body class="p-4 md:p-8 min-h-screen">
    <div class="max-w-7xl mx-auto space-y-6">
        
        <!-- Header -->
        <header class="flex flex-col lg:flex-row lg:items-center justify-between gap-4 glass-panel p-6 rounded-2xl">
            <div>
                <h1 class="text-2xl font-bold text-sky-400 text-glow flex items-center gap-2">
                    <svg class="w-7 h-7" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z"/></svg>
                    Huntington Medicina Reprodutiva
                </h1>
                <p class="text-slate-400 text-sm mt-1">RD Station CRM Sales Pipeline Analytics</p>
            </div>
            <!-- Last updated badge -->
            <span class="text-xs bg-sky-950 text-sky-400 border border-sky-800 font-semibold px-3 py-2 rounded-lg flex items-center justify-center gap-1.5 align-self-start lg:align-self-center">
                <span class="w-2 h-2 rounded-full bg-sky-400 animate-pulse"></span>
                Atualizado em: <span id="last-update">--/--/---- --:--:--</span>
            </span>
        </header>

        <!-- Filters Control Panel -->
        <section class="glass-panel p-6 rounded-2xl space-y-4">
            <h2 class="text-sm font-semibold uppercase tracking-wider text-slate-400 flex items-center gap-2">
                <svg class="w-4 h-4 text-sky-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3 4a1 1 0 011-1h16a1 1 0 011 1v2.586a1 1 0 01-.293.707l-6.414 6.414a1 1 0 00-.293.707V17l-4 4v-6.586a1 1 0 00-.293-.707L3.293 7.293A1 1 0 013 6.586V4z"/></svg>
                Painel de Filtros Globais (Filtra todas as abas e gráficos simultaneamente)
            </h2>
            <div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
                <!-- Pipeline Select -->
                <div class="flex flex-col gap-1.5">
                    <label class="text-xs text-slate-400 font-semibold">Funil de Vendas</label>
                    <select id="filter-pipeline" class="bg-slate-900 border border-slate-700 text-slate-200 text-sm rounded-lg p-2.5 font-medium focus:outline-none focus:border-sky-500">
                        <!-- Populated by JS -->
                    </select>
                </div>
                <!-- Clinic Select -->
                <div class="flex flex-col gap-1.5">
                    <label class="text-xs text-slate-400 font-semibold">Unidade (Clínica)</label>
                    <select id="filter-clinic" class="bg-slate-900 border border-slate-700 text-slate-200 text-sm rounded-lg p-2.5 font-medium focus:outline-none focus:border-sky-500">
                        <!-- Populated by JS -->
                    </select>
                </div>
                <!-- Agent Select -->
                <div class="flex flex-col gap-1.5">
                    <label class="text-xs text-slate-400 font-semibold">Responsável (Vendedor)</label>
                    <select id="filter-agent" class="bg-slate-900 border border-slate-700 text-slate-200 text-sm rounded-lg p-2.5 font-medium focus:outline-none focus:border-sky-500">
                        <!-- Populated by JS -->
                    </select>
                </div>
                <!-- Date Preset / Range -->
                <div class="flex flex-col gap-1.5">
                    <label class="text-xs text-slate-400 font-semibold">Período de Criação</label>
                    <select id="filter-date-preset" class="bg-slate-900 border border-slate-700 text-slate-200 text-sm rounded-lg p-2.5 font-medium focus:outline-none focus:border-sky-500">
                        <option value="all">Todo o Período</option>
                        <option value="last30">Últimos 30 Dias</option>
                        <option value="last90">Últimos 90 Dias</option>
                        <option value="ytd">Ano Atual (YTD)</option>
                        <option value="custom">Período Personalizado</option>
                    </select>
                </div>
            </div>
            
            <!-- Custom Date Inputs (hidden by default) -->
            <div id="custom-date-container" class="grid grid-cols-2 gap-4 max-w-md hidden">
                <div class="flex flex-col gap-1">
                    <label class="text-xs text-slate-400">Data Inicial</label>
                    <input type="date" id="filter-start-date" class="bg-slate-900 border border-slate-700 text-slate-200 text-sm rounded-lg p-2 focus:outline-none">
                </div>
                <div class="flex flex-col gap-1">
                    <label class="text-xs text-slate-400">Data Final</label>
                    <input type="date" id="filter-end-date" class="bg-slate-900 border border-slate-700 text-slate-200 text-sm rounded-lg p-2 focus:outline-none">
                </div>
            </div>
        </section>

        <!-- Tab Navigation -->
        <div class="flex gap-2 border-b border-slate-800/80 pb-2">
            <button id="btn-tab-overview" class="px-4 py-2 text-sm font-semibold rounded-lg bg-sky-600 text-slate-100 hover:bg-sky-500 transition-colors focus:outline-none flex items-center gap-2">
                <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z"/></svg>
                Resultados Gerais (Gráficos)
            </button>
            <button id="btn-tab-details" class="px-4 py-2 text-sm font-semibold rounded-lg bg-slate-800 text-slate-400 hover:bg-slate-700 hover:text-slate-200 transition-colors focus:outline-none flex items-center gap-2">
                <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3 10h18M3 14h18m-9-4v8m-7 0h14a2 2 0 002-2V8a2 2 0 00-2-2H5a2 2 0 00-2 2v8a2 2 0 002 2z"/></svg>
                Oportunidades Detalhadas (Planilha)
            </button>
        </div>

        <!-- Tab 1: Overview Content -->
        <div id="tab-content-overview" class="space-y-6">
            <!-- KPI Cards Row -->
            <div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
                <!-- Opportunities Count & Win Rate -->
                <div class="glass-panel p-5 rounded-2xl flex flex-col justify-between">
                    <span class="text-slate-400 text-xs font-semibold uppercase tracking-wider">Volume & Conversão</span>
                    <div class="mt-2 space-y-1">
                        <div class="flex items-baseline gap-2">
                            <span id="kpi-total" class="text-3xl font-bold text-slate-100">0</span>
                            <span class="text-xs text-slate-400">oportunidades</span>
                        </div>
                        <div class="text-sm font-semibold text-emerald-400 flex items-center gap-1">
                            <span id="kpi-rate">0.0%</span>
                            <span class="text-xs text-slate-500 font-medium">taxa de conversão (Won)</span>
                        </div>
                    </div>
                </div>
                
                <!-- Revenue Forecast (Unweighted & Weighted) -->
                <div class="glass-panel p-5 rounded-2xl flex flex-col justify-between">
                    <span class="text-slate-400 text-xs font-semibold uppercase tracking-wider">Previsão de Faturamento</span>
                    <div class="mt-2 space-y-1">
                        <div class="flex items-baseline gap-2">
                            <span id="kpi-weighted-revenue" class="text-2xl font-bold text-indigo-400">R$ 0</span>
                            <span class="text-xs text-indigo-400/70 font-semibold">Ponderado</span>
                        </div>
                        <div class="text-xs text-slate-400 flex justify-between">
                            <span>Bruto (Ativos):</span>
                            <span id="kpi-raw-revenue" class="font-bold">R$ 0</span>
                        </div>
                    </div>
                </div>
                
                <!-- Velocity (Cycle Time & Average Age) -->
                <div class="glass-panel p-5 rounded-2xl flex flex-col justify-between">
                    <span class="text-slate-400 text-xs font-semibold uppercase tracking-wider">Ciclo & Velocidade</span>
                    <div class="mt-2 space-y-1">
                        <div class="flex items-baseline gap-2">
                            <span id="kpi-cycle" class="text-3xl font-bold text-slate-100">0</span>
                            <span class="text-xs text-slate-400">dias para fechar</span>
                        </div>
                        <div class="text-xs text-slate-400 flex justify-between">
                            <span>Idade média ativos:</span>
                            <span id="kpi-age" class="font-bold">0 dias</span>
                        </div>
                    </div>
                </div>

                <!-- Risk Warnings (Stalled & High Value) -->
                <div class="glass-panel p-5 rounded-2xl flex flex-col justify-between">
                    <span class="text-slate-400 text-xs font-semibold uppercase tracking-wider">Gargalos & Riscos</span>
                    <div class="mt-2 space-y-1">
                        <div class="flex items-baseline gap-2">
                            <span id="kpi-stalled" class="text-3xl font-bold text-rose-500">0</span>
                            <span class="text-xs text-rose-400/80 font-medium">oportunidades paradas</span>
                        </div>
                        <div class="text-xs text-slate-500 font-medium flex justify-between">
                            <span>Sem atualização há 15+ dias</span>
                        </div>
                    </div>
                </div>
            </div>

            <!-- Main Charts Row -->
            <div class="grid grid-cols-1 lg:grid-cols-12 gap-6">
                <!-- Pipeline Funnel -->
                <div class="lg:col-span-8 glass-panel p-6 rounded-2xl">
                    <h2 class="text-lg font-bold text-slate-100 mb-4 flex items-center gap-2">
                        <span class="w-1.5 h-4 bg-sky-500 rounded-full"></span>
                        Funil de Conversão (Oportunidades por Etapa)
                    </h2>
                    <div class="relative h-72">
                        <canvas id="chart-funnel"></canvas>
                    </div>
                </div>
                <!-- Lost Reasons -->
                <div class="lg:col-span-4 glass-panel p-6 rounded-2xl">
                    <h2 class="text-lg font-bold text-slate-100 mb-4 flex items-center gap-2">
                        <span class="w-1.5 h-4 bg-rose-500 rounded-full"></span>
                        Fatores de Perda (Motivos)
                    </h2>
                    <div class="relative h-72">
                        <canvas id="chart-lost"></canvas>
                    </div>
                </div>
            </div>

            <!-- Segments Grid -->
            <div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
                <!-- Clinic Unit Analytics -->
                <div class="glass-panel p-6 rounded-2xl">
                    <h2 class="text-lg font-bold text-slate-100 mb-4 flex items-center gap-2">
                        <span class="w-1.5 h-4 bg-teal-500 rounded-full"></span>
                        Volume & Conversão por Unidade (Clínica)
                    </h2>
                    <div class="relative h-64">
                        <canvas id="chart-clinics"></canvas>
                    </div>
                </div>
                <!-- Treatment Types -->
                <div class="glass-panel p-6 rounded-2xl">
                    <h2 class="text-lg font-bold text-slate-100 mb-4 flex items-center gap-2">
                        <span class="w-1.5 h-4 bg-purple-500 rounded-full"></span>
                        Procura & Win Rate por Tratamento
                    </h2>
                    <div class="relative h-64">
                        <canvas id="chart-treatments"></canvas>
                    </div>
                </div>
            </div>

            <!-- Marketing & Team Row -->
            <div class="grid grid-cols-1 lg:grid-cols-12 gap-6">
                <!-- Marketing Channels -->
                <div class="lg:col-span-5 glass-panel p-6 rounded-2xl">
                    <h2 class="text-lg font-bold text-slate-100 mb-4 flex items-center gap-2">
                        <span class="w-1.5 h-4 bg-amber-500 rounded-full"></span>
                        Conversão por Canal de Origem
                    </h2>
                    <div class="relative h-72">
                        <canvas id="chart-marketing"></canvas>
                    </div>
                </div>
                <!-- Sales Leaderboard -->
                <div class="lg:col-span-7 glass-panel p-6 rounded-2xl flex flex-col">
                    <h2 class="text-lg font-bold text-slate-100 mb-3 flex items-center gap-2">
                        <span class="w-1.5 h-4 bg-indigo-500 rounded-full"></span>
                        Leaderboard do Time Comercial
                    </h2>
                    <div class="flex-grow overflow-x-auto">
                        <table class="w-full text-left border-collapse text-sm">
                            <thead>
                                <tr class="border-b border-slate-800 text-slate-400 font-semibold">
                                    <th class="py-2.5 px-3">Agente</th>
                                    <th class="py-2.5 px-3 text-right">Oportunidades</th>
                                    <th class="py-2.5 px-3 text-right">Ativas</th>
                                    <th class="py-2.5 px-3 text-right">Ganhas</th>
                                    <th class="py-2.5 px-3 text-right">Perdidas</th>
                                    <th class="py-2.5 px-3 text-right">Win Rate</th>
                                    <th class="py-2.5 px-3 text-right">Ciclo Médio</th>
                                    <th class="py-2.5 px-3 text-right">Faturamento</th>
                                </tr>
                            </thead>
                            <tbody id="team-table-body" class="divide-y divide-slate-800/50">
                                <!-- Populated by JS -->
                            </tbody>
                        </table>
                    </div>
                </div>
            </div>
        </div>

        <!-- Tab 2: Detailed Table Content -->
        <div id="tab-content-details" class="glass-panel p-6 rounded-2xl hidden space-y-4">
            <div class="flex flex-col sm:flex-row sm:items-center justify-between gap-4">
                <div>
                    <h2 class="text-lg font-bold text-slate-100 flex items-center gap-2">
                        <span class="w-1.5 h-4 bg-sky-500 rounded-full"></span>
                        Oportunidades Detalhadas
                    </h2>
                    <p class="text-slate-400 text-xs mt-0.5">Veja e pesquise a lista de leads que atendem aos filtros selecionados</p>
                </div>
                <input type="text" id="details-search" placeholder="Pesquisar por paciente, responsável ou clínica..." class="bg-slate-900 border border-slate-700 text-slate-200 text-sm rounded-lg p-2.5 w-full sm:w-80 focus:outline-none focus:border-sky-500">
            </div>

            <div class="overflow-x-auto">
                <table class="w-full text-left border-collapse text-xs">
                    <thead>
                        <tr class="border-b border-slate-800 text-slate-400 font-semibold uppercase tracking-wider">
                            <th class="py-3 px-4">Negócio (Paciente)</th>
                            <th class="py-3 px-4">Status</th>
                            <th class="py-3 px-4">Funil</th>
                            <th class="py-3 px-4">Etapa</th>
                            <th class="py-3 px-4">Responsável</th>
                            <th class="py-3 px-4">Unidade</th>
                            <th class="py-3 px-4">Origem</th>
                            <th class="py-3 px-4 text-right">Valor</th>
                            <th class="py-3 px-4">Criação</th>
                            <th class="py-3 px-4">Motivo Perda</th>
                        </tr>
                    </thead>
                    <tbody id="details-table-body" class="divide-y divide-slate-800/40 text-slate-300">
                        <!-- Populated by JS -->
                    </tbody>
                </table>
            </div>

            <!-- Pagination controls -->
            <div class="flex flex-col sm:flex-row items-center justify-between border-t border-slate-800/60 pt-4 gap-4">
                <span class="text-xs text-slate-400" id="details-pagination-info">Mostrando 0 a 0 de 0 oportunidades</span>
                <div class="flex gap-2">
                    <button id="btn-page-prev" class="px-3 py-1.5 text-xs font-semibold rounded transition-colors focus:outline-none">Anterior</button>
                    <button id="btn-page-next" class="px-3 py-1.5 text-xs font-semibold rounded transition-colors focus:outline-none">Próximo</button>
                </div>
            </div>
        </div>
        
    </div>

    <!-- Data Injection -->
    <script>
        const rawData = __RAW_DATA__;
    </script>

    <!-- Main JavaScript Logic (BI Engine) -->
    <script>
        // Setup chart defaults
        Chart.defaults.color = '#94a3b8';
        Chart.defaults.font.family = "'Outfit', sans-serif";
        Chart.defaults.font.size = 11;

        const chartInstances = {};
        
        // Pagination state
        let currentPage = 1;
        const pageSize = 50;

        // Utility: Formatter for Currency
        const formatBRL = (val) => {
            return new Intl.NumberFormat('pt-BR', {
                style: 'currency',
                currency: 'BRL',
                maximumFractionDigits: 0
            }).format(val);
        };

        // Utility: Date parsing
        function parseDate(str) {
            if (!str) return null;
            return new Date(str + 'T00:00:00');
        }

        // Normalizes stage names for robust matching (handling accents, capitalization, abbreviation variations)
        function normalizeStage(name) {
            if (!name) return '';
            return name.toLowerCase()
                .normalize("NFD").replace(/[\u0300-\u036f]/g, "") // remove accents
                .replace(/pcte/g, 'paciente')
                .replace(/tto/g, 'tratamento')
                .replace(/pacientes/g, 'paciente')
                .replace(/\s+/g, ' ')
                .trim();
        }

        // Custom plugin to draw labels centered inside the floating funnel bars
        const funnelLabelsPlugin = {
            id: 'funnelLabels',
            afterDatasetsDraw(chart, args, options) {
                const {ctx, data} = chart;
                ctx.save();
                ctx.font = 'bold 11px Outfit, sans-serif';
                ctx.fillStyle = '#f8fafc';
                ctx.textAlign = 'center';
                ctx.textBaseline = 'middle';
                
                const meta = chart.getDatasetMeta(0);
                meta.data.forEach((bar, index) => {
                    const raw = data.datasets[0].data[index];
                    if (raw) {
                        const val = raw[1] - raw[0];
                        if (val > 0) {
                            const centerX = (bar.x + bar.base) / 2;
                            const centerY = bar.y;
                            const widthPx = Math.abs(bar.x - bar.base);
                            // Only draw the text inside the bar if the bar is wide enough
                            if (widthPx > 30) {
                                ctx.fillText(val.toLocaleString(), centerX, centerY);
                            }
                        }
                    }
                });
                ctx.restore();
            }
        };

        // Setup Dropdown Filters
        function initFilters() {
            const pipelines = ['Todos', ...Object.keys(rawData.stages).sort()];
            const clinics = ['Todos', ...[...new Set(rawData.deals.map(d => d.clinic).filter(c => c))].sort()];
            const agents = ['Todos', ...[...new Set(rawData.deals.map(d => d.agent).filter(a => a))].sort()];
            
            // Populate Pipeline select
            const pipeSel = document.getElementById('filter-pipeline');
            pipeSel.innerHTML = pipelines.map(p => `<option value="${p}">${p}</option>`).join('');
            
            // Populate Clinic select
            const clinicSel = document.getElementById('filter-clinic');
            clinicSel.innerHTML = clinics.map(c => `<option value="${c}">${c}</option>`).join('');
            
            // Populate Agent select
            const agentSel = document.getElementById('filter-agent');
            agentSel.innerHTML = agents.map(a => `<option value="${a}">${a}</option>`).join('');
            
            // Set timestamp
            document.getElementById('last-update').innerText = rawData.updated_at || '--/--/---- --:--:--';
            
            // Explicitly set default selected values to 'Todos' / 'Todos' / 'all'
            pipeSel.value = 'Todos';
            clinicSel.value = 'Todos';
            agentSel.value = 'Todos';
            document.getElementById('filter-date-preset').value = 'all';
            
            // Event Listeners for filters
            [pipeSel, clinicSel, agentSel].forEach(el => {
                el.addEventListener('change', () => {
                    currentPage = 1; // Reset pagination when filter changes
                    updateDashboard();
                });
            });
            
            // Date presets listener
            const datePreset = document.getElementById('filter-date-preset');
            const customDates = document.getElementById('custom-date-container');
            datePreset.addEventListener('change', (e) => {
                if (e.target.value === 'custom') {
                    customDates.classList.remove('hidden');
                } else {
                    customDates.classList.add('hidden');
                    currentPage = 1;
                    updateDashboard();
                }
            });
            
            document.getElementById('filter-start-date').addEventListener('change', () => {
                currentPage = 1;
                updateDashboard();
            });
            document.getElementById('filter-end-date').addEventListener('change', () => {
                currentPage = 1;
                updateDashboard();
            });
        }

        // Setup Tab Navigation
        function initTabs() {
            const btnOverview = document.getElementById('btn-tab-overview');
            const btnDetails = document.getElementById('btn-tab-details');
            const tabOverview = document.getElementById('tab-content-overview');
            const tabDetails = document.getElementById('tab-content-details');
            
            btnOverview.addEventListener('click', () => {
                btnOverview.className = "px-4 py-2 text-sm font-semibold rounded-lg bg-sky-600 text-slate-100 hover:bg-sky-500 transition-colors focus:outline-none flex items-center gap-2";
                btnDetails.className = "px-4 py-2 text-sm font-semibold rounded-lg bg-slate-800 text-slate-400 hover:bg-slate-700 hover:text-slate-200 transition-colors focus:outline-none flex items-center gap-2";
                tabOverview.classList.remove('hidden');
                tabDetails.classList.add('hidden');
            });
            
            btnDetails.addEventListener('click', () => {
                btnDetails.className = "px-4 py-2 text-sm font-semibold rounded-lg bg-sky-600 text-slate-100 hover:bg-sky-500 transition-colors focus:outline-none flex items-center gap-2";
                btnOverview.className = "px-4 py-2 text-sm font-semibold rounded-lg bg-slate-800 text-slate-400 hover:bg-slate-700 hover:text-slate-200 transition-colors focus:outline-none flex items-center gap-2";
                tabOverview.classList.add('hidden');
                tabDetails.classList.remove('hidden');
                
                currentPage = 1;
                renderDetailsTable();
            });
            
            // Search Input listener
            document.getElementById('details-search').addEventListener('input', () => {
                currentPage = 1;
                renderDetailsTable();
            });
            
            // Prev/Next buttons
            document.getElementById('btn-page-prev').addEventListener('click', () => {
                if (currentPage > 1) {
                    currentPage--;
                    renderDetailsTable();
                }
            });
            
            document.getElementById('btn-page-next').addEventListener('click', () => {
                currentPage++;
                renderDetailsTable();
            });
        }

        // Main In-Memory Filter Engine
        function getFilteredDeals() {
            const pipe = document.getElementById('filter-pipeline').value;
            const clinic = document.getElementById('filter-clinic').value;
            const agent = document.getElementById('filter-agent').value;
            const datePreset = document.getElementById('filter-date-preset').value;
            
            let start = null;
            let end = null;
            const now = new Date();
            
            if (datePreset === 'last30') {
                start = new Date();
                start.setDate(now.getDate() - 30);
            } else if (datePreset === 'last90') {
                start = new Date();
                start.setDate(now.getDate() - 90);
            } else if (datePreset === 'ytd') {
                start = new Date(now.getFullYear(), 0, 1);
            } else if (datePreset === 'custom') {
                const sVal = document.getElementById('filter-start-date').value;
                const eVal = document.getElementById('filter-end-date').value;
                if (sVal) start = new Date(sVal + 'T00:00:00');
                if (eVal) end = new Date(eVal + 'T23:59:59');
            }
            
            return rawData.deals.filter(d => {
                // Pipeline filter
                if (pipe !== 'Todos' && d.pipeline !== pipe) return false;
                
                // Clinic filter
                if (clinic !== 'Todos' && d.clinic !== clinic) return false;
                
                // Agent filter
                if (agent !== 'Todos' && d.agent !== agent) return false;
                
                // Date range filter (based on created_at)
                if (start || end) {
                    const cDate = parseDate(d.created_at);
                    if (!cDate) return false;
                    if (start && cDate < start) return false;
                    if (end && cDate > end) return false;
                }
                
                return true;
            });
        }

        // Get Filtered and Text-Searched Deals for Tab 2
        function getFilteredAndSearchedDeals() {
            const deals = getFilteredDeals();
            const searchVal = document.getElementById('details-search').value.toLowerCase().trim();
            
            if (!searchVal) return deals;
            
            return deals.filter(d => {
                return (d.name && d.name.toLowerCase().includes(searchVal)) || 
                       (d.agent && d.agent.toLowerCase().includes(searchVal)) ||
                       (d.clinic && d.clinic.toLowerCase().includes(searchVal)) ||
                       (d.lost_reason && d.lost_reason.toLowerCase().includes(searchVal)) ||
                       (d.channel && d.channel.toLowerCase().includes(searchVal));
            });
        }

        // Dynamic Calculations
        function updateDashboard() {
            const deals = getFilteredDeals();
            const pipe = document.getElementById('filter-pipeline').value;
            
            // 1. Calculate historical win rates per stage for forecasting
            const stageConversionRates = {};
            const stageTotal = {};
            const stageWon = {};
            
            deals.forEach(d => {
                if (d.status === 'won') {
                    stageWon[d.stage] = (stageWon[d.stage] || 0) + 1;
                    stageTotal[d.stage] = (stageTotal[d.stage] || 0) + 1;
                } else if (d.status === 'lost') {
                    stageTotal[d.stage] = (stageTotal[d.stage] || 0) + 1;
                }
            });
            
            Object.keys(stageTotal).forEach(st => {
                stageConversionRates[st] = stageWon[st] / stageTotal[st];
            });

            // 2. Compute KPIs
            const total = deals.length;
            const active = deals.filter(d => d.status === 'ongoing').length;
            const won = deals.filter(d => d.status === 'won').length;
            const lost = deals.filter(d => d.status === 'lost').length;
            
            const winRate = (won + lost) > 0 ? (won / (won + lost) * 100) : 0.0;
            
            // Revenue metrics
            let rawActiveRevenue = 0.0;
            let weightedActiveRevenue = 0.0;
            let wonRevenue = 0.0;
            
            // Use the latest update date in the dataset as the reference point for stalled calculations
            const dates = deals.map(d => parseDate(d.updated_at)).filter(d => d !== null);
            const today = dates.length > 0 ? new Date(Math.max(...dates)) : new Date();
            
            // Stalled count
            let stalledCount = 0;
            
            // Cycle Time calculation variables
            let totalCycleDays = 0;
            let wonDealsWithDates = 0;
            let totalAgeDays = 0;
            let activeDealsWithDates = 0;

            deals.forEach(d => {
                const created = parseDate(d.created_at);
                const updated = parseDate(d.updated_at);
                const closed = parseDate(d.closed_at);
                
                if (d.status === 'won') {
                    wonRevenue += d.revenue;
                    if (created && closed) {
                        totalCycleDays += (closed - created) / (1000 * 60 * 60 * 24);
                        wonDealsWithDates++;
                    }
                } else if (d.status === 'ongoing') {
                    rawActiveRevenue += d.revenue;
                    
                    // Apply stage conversion probability as forecast weight
                    const weight = stageConversionRates[d.stage] || 0.15; // default 15% if no history
                    weightedActiveRevenue += (d.revenue * weight);
                    
                    if (created) {
                        totalAgeDays += (today - created) / (1000 * 60 * 60 * 24);
                        activeDealsWithDates++;
                    }
                    
                    if (updated && (today - updated) / (1000 * 60 * 60 * 24) >= 15) {
                        stalledCount++;
                    }
                }
            });
            
            const avgCycle = wonDealsWithDates > 0 ? Math.round(totalCycleDays / wonDealsWithDates) : 0;
            const avgAge = activeDealsWithDates > 0 ? Math.round(totalAgeDays / activeDealsWithDates) : 0;

            // Render KPI cards
            document.getElementById('kpi-total').innerText = total.toLocaleString();
            document.getElementById('kpi-rate').innerText = winRate.toFixed(1) + '%';
            document.getElementById('kpi-weighted-revenue').innerText = formatBRL(weightedActiveRevenue);
            document.getElementById('kpi-raw-revenue').innerText = formatBRL(rawActiveRevenue);
            document.getElementById('kpi-cycle').innerText = avgCycle;
            document.getElementById('kpi-age').innerText = avgAge + ' dias';
            document.getElementById('kpi-stalled').innerText = stalledCount.toLocaleString();

            // Render Sales Reps Leaderboard table
            renderTeamTable(deals, today);
            
            // Draw / Update all charts
            renderFunnelChart(deals, pipe);
            renderLostReasonsChart(deals);
            renderClinicsChart(deals);
            renderTreatmentsChart(deals);
            renderMarketingChart(deals);
            
            // Render Details Table
            renderDetailsTable();
        }

        // Leaderboard table rendering
        function renderTeamTable(deals, today) {
            const agentsData = {};
            deals.forEach(d => {
                if (!d.agent) return;
                if (!agentsData[d.agent]) {
                    agentsData[d.agent] = {
                        agent: d.agent, total: 0, active: 0, won: 0, lost: 0, revenue: 0, cycleSum: 0, cycleCount: 0
                    };
                }
                const ag = agentsData[d.agent];
                ag.total++;
                if (d.status === 'ongoing') ag.active++;
                else if (d.status === 'won') {
                    ag.won++;
                    ag.revenue += d.revenue;
                    const created = parseDate(d.created_at);
                    const closed = parseDate(d.closed_at);
                    if (created && closed) {
                        ag.cycleSum += (closed - created) / (1000 * 60 * 60 * 24);
                        ag.cycleCount++;
                    }
                }
                else if (d.status === 'lost') ag.lost++;
            });

            const agentsList = Object.values(agentsData).map(ag => {
                const winRate = (ag.won + ag.lost) > 0 ? (ag.won / (ag.won + ag.lost) * 100) : 0.0;
                const cycle = ag.cycleCount > 0 ? Math.round(ag.cycleSum / ag.cycleCount) : 0;
                return { ...ag, winRate: winRate.toFixed(1), cycle };
            }).sort((a, b) => b.revenue - a.revenue);

            const tbody = document.getElementById('team-table-body');
            tbody.innerHTML = agentsList.map(ag => `
                <tr class="hover:bg-slate-900/40 text-slate-300 transition-colors">
                    <td class="py-2.5 px-3 font-medium">${ag.agent}</td>
                    <td class="py-2.5 px-3 text-right text-slate-400 font-semibold">${ag.total}</td>
                    <td class="py-2.5 px-3 text-right text-sky-400 font-semibold">${ag.active}</td>
                    <td class="py-2.5 px-3 text-right text-emerald-400 font-semibold">${ag.won}</td>
                    <td class="py-2.5 px-3 text-right text-rose-400 font-semibold">${ag.lost}</td>
                    <td class="py-2.5 px-3 text-right text-amber-400 font-semibold">${ag.winRate}%</td>
                    <td class="py-2.5 px-3 text-right text-slate-400">${ag.cycle > 0 ? ag.cycle + 'd' : '-'}</td>
                    <td class="py-2.5 px-3 text-right text-indigo-400 font-semibold">${formatBRL(ag.revenue)}</td>
                </tr>
            `).join('');
        }

        // Detailed Table pagination & rendering
        function renderDetailsTable() {
            const deals = getFilteredAndSearchedDeals();
            const total = deals.length;
            const totalPages = Math.max(1, Math.ceil(total / pageSize));
            
            if (currentPage > totalPages) currentPage = totalPages;
            if (currentPage < 1) currentPage = 1;
            
            const startIdx = (currentPage - 1) * pageSize;
            const endIdx = Math.min(startIdx + pageSize, total);
            
            const pageDeals = deals.slice(startIdx, endIdx);
            const tbody = document.getElementById('details-table-body');
            tbody.innerHTML = '';
            
            if (pageDeals.length === 0) {
                tbody.innerHTML = `
                    <tr>
                        <td colspan="10" class="py-8 text-center text-slate-500 font-semibold">Nenhuma oportunidade encontrada.</td>
                    </tr>
                `;
                document.getElementById('details-pagination-info').innerText = 'Mostrando 0 de 0 oportunidades';
                
                document.getElementById('btn-page-prev').disabled = true;
                document.getElementById('btn-page-prev').className = "px-3 py-1.5 text-xs font-semibold rounded bg-slate-800/40 text-slate-600 cursor-not-allowed transition-colors";
                document.getElementById('btn-page-next').disabled = true;
                document.getElementById('btn-page-next').className = "px-3 py-1.5 text-xs font-semibold rounded bg-slate-800/40 text-slate-600 cursor-not-allowed transition-colors";
                return;
            }
            
            tbody.innerHTML = pageDeals.map(d => {
                let statusBadge = '';
                if (d.status === 'won') {
                    statusBadge = '<span class="px-2 py-0.5 text-[10px] font-semibold rounded bg-emerald-950/80 text-emerald-400 border border-emerald-800/60">Ganho</span>';
                } else if (d.status === 'lost') {
                    statusBadge = '<span class="px-2 py-0.5 text-[10px] font-semibold rounded bg-rose-950/80 text-rose-400 border border-rose-800/60">Perdido</span>';
                } else {
                    statusBadge = '<span class="px-2 py-0.5 text-[10px] font-semibold rounded bg-sky-950/80 text-sky-400 border border-sky-800/60">Ativo</span>';
                }
                
                const valStr = d.revenue > 0 ? formatBRL(d.revenue) : '-';
                const lossStr = d.lost_reason !== 'Sem Motivo' ? d.lost_reason : '-';
                
                return `
                    <tr class="hover:bg-slate-900/35 transition-colors border-b border-slate-800/30">
                        <td class="py-3 px-4 font-medium text-slate-200">${d.name}</td>
                        <td class="py-3 px-4">${statusBadge}</td>
                        <td class="py-3 px-4 text-slate-400">${d.pipeline}</td>
                        <td class="py-3 px-4 text-slate-400">${d.stage}</td>
                        <td class="py-3 px-4 text-slate-300">${d.agent}</td>
                        <td class="py-3 px-4 text-slate-300">${d.clinic}</td>
                        <td class="py-3 px-4 text-slate-400">${d.channel}</td>
                        <td class="py-3 px-4 text-right font-semibold text-slate-200">${valStr}</td>
                        <td class="py-3 px-4 text-slate-400">${d.created_at}</td>
                        <td class="py-3 px-4 text-slate-400">${lossStr}</td>
                    </tr>
                `;
            }).join('');
            
            document.getElementById('details-pagination-info').innerText = `Mostrando ${startIdx + 1} a ${endIdx} de ${total} oportunidades`;
            
            // Prev Button
            const prevBtn = document.getElementById('btn-page-prev');
            prevBtn.disabled = (currentPage === 1);
            prevBtn.className = `px-3 py-1.5 text-xs font-semibold rounded transition-colors focus:outline-none ${currentPage === 1 ? 'bg-slate-800/40 text-slate-600 cursor-not-allowed' : 'bg-slate-800 hover:bg-slate-700 text-slate-300'}`;
            
            // Next Button
            const nextBtn = document.getElementById('btn-page-next');
            const isLast = (currentPage === totalPages);
            nextBtn.disabled = isLast;
            nextBtn.className = `px-3 py-1.5 text-xs font-semibold rounded transition-colors focus:outline-none ${isLast ? 'bg-slate-800/40 text-slate-600 cursor-not-allowed' : 'bg-slate-800 hover:bg-slate-700 text-slate-300'}`;
        }

        // Dynamic Funnel Chart rendering (Floating pyramid funnel shape)
        function renderFunnelChart(deals, activePipelineName) {
            const ctx = document.getElementById('chart-funnel').getContext('2d');
            if (chartInstances.funnel) chartInstances.funnel.destroy();
            
            let stagesList = [];
            if (activePipelineName === 'Todos') {
                stagesList = rawData.stages['Huntington/Cenafert'] || [];
            } else {
                stagesList = rawData.stages[activePipelineName] || [];
            }
            
            // Define terminal or non-progression stages to exclude from the funnel visual progression
            const terminalStages = [
                'perdeu', 'beta negativo', 'excluida', 'excluido',
                'paciente sem indicacao de tratamento', 'pcte sem indicacao de tto',
                'investigacao/tto.externo', 'sem etapa', 'sem funil'
            ];
            
            // Filter to get only the active progression stages
            const progressionStages = stagesList.filter(st => {
                const norm = normalizeStage(st);
                return !terminalStages.includes(norm);
            });
            
            // Calculate highest reached stage index for each deal
            function getHighestReachedIndex(deal) {
                const norm = normalizeStage(deal.stage);
                
                // Map terminal/lost stages to the stage right before they exited, or to index 0 if unknown
                let targetStage = deal.stage;
                if (norm === 'beta negativo' || norm === 'beta no') {
                    targetStage = 'Tratamento Iniciado';
                } else if (norm === 'paciente sem indicacao de tratamento' || norm === 'pcte sem indicacao de tto') {
                    targetStage = 'Compareceu a Consulta';
                } else if (norm === 'perdeu' || norm === 'excluida' || norm === 'excluido') {
                    // Unknown stage of loss, default to the first progression stage (Leads)
                    return 0;
                }
                
                const normTarget = normalizeStage(targetStage);
                const idx = progressionStages.findIndex(st => normalizeStage(st) === normTarget);
                
                // If it's a won deal (and not already mapped), it reached the last progression stage
                if (idx === -1 && deal.status === 'won') {
                    return progressionStages.length - 1;
                }
                
                return idx;
            }
            
            // Calculate cumulative counts for each progression stage
            const values = progressionStages.map((st, idx) => {
                return deals.filter(d => {
                    // For the selected pipeline (or all if 'Todos')
                    if (activePipelineName !== 'Todos' && d.pipeline !== activePipelineName) {
                        return false;
                    }
                    const reachedIdx = getHighestReachedIndex(d);
                    return reachedIdx >= idx;
                }).length;
            });
            
            // Calculate Stage-to-Stage Conversions
            const conversions = values.map((val, idx) => {
                if (idx === 0) return '100%';
                const prev = values[idx - 1];
                if (prev === 0) return '0%';
                return Math.round((val / prev) * 100) + '%';
            });
            
            // Calculate overall conversion from the first stage
            const overallConversions = values.map((val) => {
                if (values[0] === 0) return '0%';
                return Math.round((val / values[0]) * 100) + '%';
            });

            // Map values to floating ranges [-v/2, v/2] to center them around the y-axis (producing a visual funnel shape)
            const funnelRanges = values.map(v => [-v/2, v/2]);

            chartInstances.funnel = new Chart(ctx, {
                type: 'bar',
                plugins: [funnelLabelsPlugin],
                data: {
                    labels: progressionStages,
                    datasets: [{
                        label: 'Oportunidades',
                        data: funnelRanges,
                        backgroundColor: function(context) {
                            const chart = context.chart;
                            const {ctx, chartArea} = chart;
                            if (!chartArea) return null;
                            const gradient = ctx.createLinearGradient(chartArea.left, 0, chartArea.right, 0);
                            gradient.addColorStop(0, 'rgba(14, 165, 233, 0.85)'); // teal
                            gradient.addColorStop(0.5, 'rgba(59, 130, 246, 0.9)'); // blue
                            gradient.addColorStop(1, 'rgba(14, 165, 233, 0.85)'); // teal
                            return gradient;
                        },
                        borderColor: 'rgba(56, 189, 248, 0.5)',
                        borderWidth: 1,
                        borderRadius: 5,
                        borderSkipped: false,
                        barPercentage: 0.85,
                        categoryPercentage: 0.9
                    }]
                },
                options: {
                    indexAxis: 'y',
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: { display: false },
                        tooltip: {
                            backgroundColor: '#0f172a',
                            borderColor: 'rgba(255,255,255,0.08)',
                            borderWidth: 1,
                            padding: 10,
                            callbacks: {
                                label: function(context) {
                                    const raw = context.raw;
                                    const val = raw[1] - raw[0];
                                    const idx = context.dataIndex;
                                    return ` Oportunidades: ${val} (Conv: ${conversions[idx]} do anterior, ${overallConversions[idx]} do total)`;
                                }
                            }
                        }
                    },
                    scales: {
                        x: {
                            display: false, // Hide numeric axis to focus on the relative funnel shape
                            grid: { display: false }
                        },
                        y: {
                            grid: { display: false },
                            ticks: {
                                color: '#cbd5e1',
                                font: {
                                    family: 'Outfit, sans-serif',
                                    size: 11,
                                    weight: '500'
                                }
                            }
                        }
                    }
                }
            });
        }

        // Lost Reasons doughnut chart
        function renderLostReasonsChart(deals) {
            const ctx = document.getElementById('chart-lost').getContext('2d');
            if (chartInstances.lost) chartInstances.lost.destroy();

            const counts = {};
            deals.filter(d => d.status === 'lost').forEach(d => {
                counts[d.lost_reason] = (counts[d.lost_reason] || 0) + 1;
            });

            const sorted = Object.entries(counts).map(([reason, count]) => ({reason, count}))
                .sort((a, b) => b.count - a.count)
                .slice(0, 7);

            chartInstances.lost = new Chart(ctx, {
                type: 'doughnut',
                data: {
                    labels: sorted.map(s => s.reason),
                    datasets: [{
                        data: sorted.map(s => s.count),
                        backgroundColor: [
                            '#f43f5e', '#ec4899', '#d946ef', '#a855f7', 
                            '#8b5cf6', '#6366f1', '#4f46e5'
                        ],
                        borderWidth: 1,
                        borderColor: '#0b0f19'
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: {
                            position: 'right',
                            labels: { boxWidth: 10, color: '#cbd5e1', padding: 8 }
                        },
                        tooltip: {
                            backgroundColor: '#0f172a',
                            borderColor: 'rgba(255,255,255,0.08)',
                            borderWidth: 1
                        }
                    },
                    cutout: '65%'
                }
            });
        }

        // Clinic Analytics
        function renderClinicsChart(deals) {
            const ctx = document.getElementById('chart-clinics').getContext('2d');
            if (chartInstances.clinics) chartInstances.clinics.destroy();

            const data = {};
            deals.forEach(d => {
                if (!d.clinic) return;
                if (!data[d.clinic]) data[d.clinic] = { total: 0, won: 0, lost: 0 };
                data[d.clinic].total++;
                if (d.status === 'won') data[d.clinic].won++;
                else if (d.status === 'lost') data[d.clinic].lost++;
            });

            const clinics = Object.entries(data).map(([clinic, counts]) => {
                const winRate = (counts.won + counts.lost) > 0 ? (counts.won / (counts.won + counts.lost) * 100) : 0.0;
                return { clinic, total: counts.total, winRate: winRate.toFixed(1) };
            }).sort((a, b) => b.total - a.total).slice(0, 8);

            chartInstances.clinics = new Chart(ctx, {
                type: 'bar',
                data: {
                    labels: clinics.map(c => c.clinic),
                    datasets: [
                        {
                            label: 'Oportunidades',
                            data: clinics.map(c => c.total),
                            backgroundColor: '#3b82f6',
                            yAxisID: 'yVolume',
                            borderRadius: 4
                        },
                        {
                            label: 'Taxa de Conversão',
                            data: clinics.map(c => parseFloat(c.winRate)),
                            type: 'line',
                            borderColor: '#fbbf24',
                            pointBackgroundColor: '#fbbf24',
                            borderWidth: 2,
                            yAxisID: 'yRate',
                            tension: 0.15
                        }
                    ]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: { position: 'top', labels: { boxWidth: 10, color: '#cbd5e1' } }
                    },
                    scales: {
                        yVolume: { type: 'linear', position: 'left', grid: { color: 'rgba(255,255,255,0.05)' } },
                        yRate: {
                            type: 'linear', position: 'right', grid: { display: false },
                            ticks: { callback: v => v + '%' },
                            min: 0, max: 100
                        }
                    }
                }
            });
        }

        // Treatment Analytics
        function renderTreatmentsChart(deals) {
            const ctx = document.getElementById('chart-treatments').getContext('2d');
            if (chartInstances.treatments) chartInstances.treatments.destroy();

            const data = {};
            deals.forEach(d => {
                if (!d.treatment) return;
                if (!data[d.treatment]) data[d.treatment] = { total: 0, won: 0, lost: 0 };
                data[d.treatment].total++;
                if (d.status === 'won') data[d.treatment].won++;
                else if (d.status === 'lost') data[d.treatment].lost++;
            });

            const list = Object.entries(data).map(([treatment, counts]) => {
                const winRate = (counts.won + counts.lost) > 0 ? (counts.won / (counts.won + counts.lost) * 100) : 0.0;
                return { treatment, total: counts.total, winRate: winRate.toFixed(1) };
            }).sort((a, b) => b.total - a.total).slice(0, 8);

            chartInstances.treatments = new Chart(ctx, {
                type: 'bar',
                data: {
                    labels: list.map(c => c.treatment),
                    datasets: [
                        {
                            label: 'Oportunidades',
                            data: list.map(c => c.total),
                            backgroundColor: '#a855f7',
                            yAxisID: 'yVolume',
                            borderRadius: 4
                        },
                        {
                            label: 'Taxa de Conversão',
                            data: list.map(c => parseFloat(c.winRate)),
                            type: 'line',
                            borderColor: '#10b981',
                            pointBackgroundColor: '#10b981',
                            borderWidth: 2,
                            yAxisID: 'yRate',
                            tension: 0.15
                        }
                    ]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: { position: 'top', labels: { boxWidth: 10, color: '#cbd5e1' } }
                    },
                    scales: {
                        yVolume: { type: 'linear', position: 'left', grid: { color: 'rgba(255,255,255,0.05)' } },
                        yRate: {
                            type: 'linear', position: 'right', grid: { display: false },
                            ticks: { callback: v => v + '%' },
                            min: 0, max: 100
                        }
                    }
                }
            });
        }

        // Marketing Analytics
        function renderMarketingChart(deals) {
            const ctx = document.getElementById('chart-marketing').getContext('2d');
            if (chartInstances.marketing) chartInstances.marketing.destroy();

            const data = {};
            deals.forEach(d => {
                if (!d.channel) return;
                if (!data[d.channel]) data[d.channel] = { total: 0, won: 0, lost: 0 };
                data[d.channel].total++;
                if (d.status === 'won') data[d.channel].won++;
                else if (d.status === 'lost') data[d.channel].lost++;
            });

            const list = Object.entries(data).map(([channel, counts]) => {
                const winRate = (counts.won + counts.lost) > 0 ? (counts.won / (counts.won + counts.lost) * 100) : 0.0;
                return { channel, total: counts.total, winRate: winRate.toFixed(1) };
            }).sort((a, b) => b.total - a.total).slice(0, 8);

            chartInstances.marketing = new Chart(ctx, {
                type: 'bar',
                data: {
                    labels: list.map(c => c.channel),
                    datasets: [{
                        label: 'Oportunidades',
                        data: list.map(c => c.total),
                        backgroundColor: '#f59e0b',
                        borderRadius: 4
                    }]
                },
                options: {
                    indexAxis: 'y',
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: { display: false },
                        tooltip: {
                            backgroundColor: '#0f172a',
                            borderColor: 'rgba(255,255,255,0.08)',
                            borderWidth: 1,
                            callbacks: {
                                label: function(context) {
                                    const val = context.raw;
                                    const idx = context.dataIndex;
                                    return ` Oportunidades: ${val} (Win Rate: ${list[idx].winRate}%)`;
                                }
                            }
                        }
                    },
                    scales: {
                        x: { grid: { color: 'rgba(255,255,255,0.05)' } },
                        y: { grid: { display: false } }
                    }
                }
            });
        }

        // Initialize Dashboard Page
        window.addEventListener('DOMContentLoaded', () => {
            initFilters();
            initTabs();
            updateDashboard();
        });
    </script>
</body>
</html>
"""

def generate_dashboard():
    deals, stages = fetch_data()
    
    dashboard_data = {
        "deals": deals,
        "stages": stages,
        "updated_at": datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    }
    
    # Compile template using simple replace to bypass f-string escaping limits
    html_content = HTML_TEMPLATE.replace("__RAW_DATA__", json.dumps(dashboard_data, indent=4))
    
    logger.info(f"Writing dashboard HTML output to: {OUTPUT_HTML}")
    with open(OUTPUT_HTML, "w", encoding="utf-8") as out:
        out.write(html_content)
        
    logger.info("=== DASHBOARD GENERATION COMPLETED SUCCESSFULLY ===")

if __name__ == "__main__":
    generate_dashboard()
