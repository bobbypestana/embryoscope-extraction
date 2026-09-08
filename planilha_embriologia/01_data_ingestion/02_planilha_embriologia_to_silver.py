#!/usr/bin/env python3
"""
Planilha Embriologia Silver Transformation
Combines all bronze tables (one per Excel file) into a single silver table with:
- Standardized column names (normalized strings)
- Proper data type casting
- Data cleaning
"""

import logging
import pandas as pd
import numpy as np
import duckdb
from datetime import datetime
import os
import sys
import unicodedata
import re

# -- Path setup ---------------------------------------------------------------
_script_dir = os.path.dirname(os.path.abspath(__file__))
_root_dir   = os.path.dirname(os.path.dirname(_script_dir))  # Huntington/
sys.path.insert(0, _root_dir)

from commons.prontuario_matching_v1 import find_prontuarios

# Setup logging
LOGS_DIR = os.path.join(os.path.dirname(__file__), 'logs')
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

# Configuration
DUCKDB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'database', 'huntington_data_lake.duckdb')
BRONZE_PATTERN = 'planilha_%'  # Pattern to match all Planilha tables
SHEET_TYPES = ['fresh', 'fet', 'recep', 'fot', 'doadoras', 'fp_ovulos', 'fp_semen', 'iiu']  # Process 8 independent sheet types

# Refinement Configuration (All available years 2021-2026)
YEARS_TO_PROCESS = ['2021', '2022', '2023', '2024', '2025', '2026']
REFERENCE_TABLES = {
    'fresh': 'planilha_2024_ibira_fresh',
    'fet': 'planilha_2024_ibira_fet',
    'recep': 'planilha_2024_ibira_recep',
    'fot': 'planilha_2024_ibira_fot',
    'doadoras': 'planilha_2024_ibira_doadoras',
    'fp_ovulos': 'planilha_2024_ibira_fp_cong_ovulos_e_tecidos',
    'fp_semen': 'planilha_2024_ibira_fp_cong_de_semen',
    'iiu': 'planilha_2024_ibira_iiu'
}

# Column Whitelist (normalized names as snake_case)
WHITELIST = {
    'fresh': [
        'pin',
        'nome_da_paciente',
        'data_de_nasc',
        'data_da_puncao',
        'fator_1',
        'incubadora',
        'data_crio',
        'tipo_1',
        'tipo_de_inseminacao',
        'tipo_biopsia',
        'altura',
        'peso',
        'idade_espermatozoide',
        'origem',
        'tipo',
        'opu',
        'total_de_mii',
        'qtd_blasto',
        'qtd_blasto_tq_a_e_b',
        'no_biopsiados',
        'qtd_analisados',
        'qtd_normais',
        'dia_cryo',
        'data_da_fet',
        'result',
        'tipo_do_resultado',
        'gravidez_clinica',
        'gravidez_bioquimica',
        'no_nascidos',
        'dia_et',
        'no_et',
        'houve_transferencia',
        'data_parto',
        'tipo_de_parto',
        'peso_1'
    ],
    'fet': [
        'pin',
        'nome_da_paciente',
        'data_de_nasc',
        'data_da_fet',
        'data_crio',
        'result',
        'tipo_do_resultado',
        'no_nascidos',
        'tipo_1',
        'tipo_de_tratamento',
        'tipo_de_fet',
        'tipo_biopsia',
        'tipo_da_doacao',
        'idade_mulher',
        'idade_do_cong_de_embriao',
        'preparo_para_transferencia',
        'dia_cryo',
        'no_da_transfer_1a_2a_3a',
        'dia_et',
        'no_et',
        'gravidez_bioquimica',
        'gravidez_clinica',
        'houve_transferencia',
        'data_parto',
        'tipo_de_parto',
        'peso_1',
        'peso_2',
        'obs'
    ],
    'recep': [
        'pin',
        'nome_da_paciente',
        'data_de_nasc',
        'data_da_fet',
        'data_do_procedimento',
        'pin_doadora',
        'data_crio',
        'result',
        'tipo_do_resultado',
        'no_nascidos',
        'tipo_1',
        'tipo_de_tratamento',
        'tipo_de_fet',
        'tipo_biopsia',
        'tipo_da_doacao',
        'idade_mulher',
        'idade_do_cong_de_embriao',
        'preparo_para_transferencia',
        'dia_cryo',
        'no_da_transfer_1a_2a_3a',
        'dia_et',
        'no_et',
        'gravidez_bioquimica',
        'gravidez_clinica',
        'houve_transferencia',
        'data_parto',
        'tipo_de_parto',
        'peso_1',
        'obs'
    ],
    'fot': [
        'pin',
        'nome_da_paciente',
        'data_de_nasc',
        'data_da_puncao',
        'data_do_procedimento',
        'data_crio',
        'data_da_fet',
        'result',
        'tipo_do_resultado',
        'gravidez_clinica',
        'gravidez_bioquimica',
        'no_nascidos',
        'fator_1',
        'incubadora',
        'tipo_1',
        'tipo_de_inseminacao',
        'tipo_biopsia',
        'total_de_mii',
        'qtd_blasto',
        'dia_cryo',
        'houve_transferencia',
        'data_parto',
        'tipo_de_parto'
    ],
    'doadoras': [
        'pin',
        'nome_da_paciente',
        'data_de_nasc',
        'idade',
        'grupo_de_idade',
        'altura',
        'peso',
        'medico',
        'unidade',
        'tipo_de_tratamento',
        'tipo_da_doacao',
        'data_inicio_inducao',
        'data_do_procedimento',
        'protocolo',
        'gnrh_bloqueio',
        'fsh',
        'dose_total_fsh',
        'lh',
        'dose_total_lh',
        'medicamento_maturacao_ovulacao',
        'end_espessura',
        'n_fol_14',
        'resp_crio',
        'opu',
        'mii_total',
        'mii_doados_fresco',
        'mii_doados_crio'
    ],
    'fp_ovulos': [
        'pin',
        'nome_da_paciente',
        'data_de_nasc',
        'idade',
        'grupo_de_idade',
        'altura',
        'peso',
        'medico',
        'unidade',
        'tipo_1',
        'tipo_de_tratamento',
        'motivo_do_congelamento',
        'tipo_cancer',
        'data_inicio_inducao',
        'data_do_procedimento',
        'protocolo',
        'gnrh_bloqueio',
        'fsh',
        'dose_total_fsh',
        'lh',
        'dose_total_lh',
        'medicamento_maturacao_ovulacao',
        'end_espessura',
        'n_fol_14',
        'resp_crio',
        'opu',
        'mii_crio',
        'mi_crio',
        'data_da_cirurgia',
        'tipo_da_cirurgia',
        'amostra_do_tecido',
        'numero_de_fragmentos_crio',
        'ohss',
        'hemorragia',
        'infeccao'
    ],
    'fp_semen': [
        'pin',
        'nome_da_paciente',
        'data_de_nasc',
        'idade',
        'altura',
        'peso',
        'medico',
        'unidade',
        'tipo_de_tratamento',
        'motivo_do_congelamento',
        'tipo_cancer',
        'data_do_procedimento',
        'origem',
        'tipo',
        'concentr',
        'motilid',
        'morfo',
        'preparo',
        'no_de_palhetas_vials_crio',
        'concentracao_por_palheta_vials',
        'metodo_crio'
    ],
    'iiu': [
        'pin',
        'nome_da_paciente',
        'data_de_nasc',
        'idade',
        'grupo_idade',
        'altura',
        'peso',
        'medico',
        'unidade',
        'tipo_1',
        'tipo_de_tratamento',
        'data_inicial_da_inducao',
        'data_do_procedimento',
        'conjuge',
        'indicacao_clinica',
        'medicamento_indutor',
        'fsh',
        'lh',
        'responsavel_pelo_preparo',
        'tecnica_de_preparo',
        'total_sptz_amostra_final',
        'result',
        'tipo_do_resultado',
        'gravidez_clinica',
        'gravidez_bioquimica',
        'no_sg',
        'no_nascidos',
        'data_parto',
        'tipo_de_parto'
    ]
}

# Values for TIPO 1 filtering (used as prefixes for shared tables)
TIPO_FILTERS = {
    'fresh': ['FIC/ICSI', 'FIV/ICSI', 'FRESH', 'ICSI', 'FIV', 'CONG', 'OR', 'PUNÇÃO', 'PUNCAO'],
    'fet': ['FET', 'FET/OR', 'FET/ER', 'TEC', 'DESCONG EMBRIAO', 'DESCONG EMBRIÃO'],
    'recep': ['RECEPTORA', 'RECEP', 'DOAÇÃO', 'DOACAO', 'RECEPT', 'DONOR'],
    'fot': ['FOT', 'FOT OR', 'DESCONG OVO', 'DESCONG OVULO', 'DESCONG ÓVULO'],
    'doadoras': ['DOADORA', 'DOADORAS'],
    'fp_ovulos': ['EGG FREEZING', 'CRIO DE ÓVULOS', 'CRIO DE OVULOS', 'CRIO OVULOS', 'CRIO DE OÓCITOS', 'CONG. ÓVULOS', 'CONG ÓVULOS', 'FP', 'CRIO TECIDO'],
    'fp_semen': ['CRIO DE SPTZ', 'CRIO SPTZ', 'CONG SEMEN', 'CONGELAMENTO DE SEMEN'],
    'iiu': ['IIU', 'IUI', 'INSEMINAÇÃO', 'INSEMINACAO']
}

# Explicit Synonyms (Global heuristics)
SYNONYMS = {
    'resultado': 'result',
    'tipo_resultado': 'tipo_do_resultado',
    'tipo_de_resultado': 'tipo_do_resultado',
    'houve_transferencia': 'houve_transferencia',
    'houve_transf': 'houve_transferencia',
    'n_nascidos': 'no_nascidos',
    'num_nascidos': 'no_nascidos',
    'no_nascidos': 'no_nascidos',
    'na_nascidos': 'no_nascidos',
    'n_o_nascidos': 'no_nascidos',
    'n_biopsiados': 'no_biopsiados',
    'n_analisados': 'qtd_analisados',
    'n_et': 'no_et',
    'num_et': 'no_et',
    'n_da_transfer': 'no_da_transfer_1a_2a_3a',
    'numero_da_transfer_1a_2a_3a': 'no_da_transfer_1a_2a_3a',
    'numero_da_transfer_1_2_3': 'no_da_transfer_1a_2a_3a',
    'numero_da_transfer': 'no_da_transfer_1a_2a_3a',
    'data_crio_somente_a_primeira_data_do_cong': 'data_crio',
    'data_crio_embriao': 'data_crio',
    'data_crio_embrioes': 'data_crio',
    'tipo_de_tratamento': 'tipo_1',
    'data_cryo': 'data_crio',
    'dia_crio': 'dia_cryo',
    'nome': 'nome_da_paciente',
    'paciente': 'nome_da_paciente',
    'nasc': 'data_de_nasc',
    'data_nasc': 'data_de_nasc',
    'prontuario': 'pin',
    'pronturio': 'pin',
    'idade': 'idade_mulher',
    'idade_da_mulher': 'idade_mulher',
    'idade_do_esperma': 'idade_espermatozoide',
    'idade_do_espermatozoide': 'idade_espermatozoide',
    'origem_sptz': 'origem',
    'tipo_sptz': 'tipo',
    'origem_do_espermatozoide': 'origem',
    'tipo_do_espermatozoide': 'tipo',
    'data_do_fot': 'data_do_procedimento',
    'data_do_procedimento': 'data_do_procedimento',
    'data_procedimento': 'data_do_procedimento',
    'data_da_coleta': 'data_da_puncao',
    'data_transferencia': 'data_da_fet',
    'data_da_transferencia': 'data_da_fet',
    'data_cong': 'data_crio',
    'beta': 'result',
    'transf': 'no_et',
    'blast': 'qtd_blasto',
    'blasto': 'qtd_blasto',
    'normais': 'qtd_normais',
    'causa': 'fator_1',
    'incub': 'incubadora',
    'incub_d5': 'incubadora',
    'tipo_de_inseminacao_ou_icsi': 'tipo_de_inseminacao',
    'tipo_inseminacao': 'tipo_de_inseminacao',
    'tipo_da_doacao_recepcao': 'tipo_da_doacao',
    'data_parto': 'data_parto',
    'tipo_parto': 'tipo_de_parto',
    'tipo_de_parto': 'tipo_de_parto',
    'peso_1': 'peso_1',
    'peso_2': 'peso_2',
    'nome_do_paciente': 'nome_da_paciente',
    'total_dose_fsh': 'dose_total_fsh',
    'total_dose_lh': 'dose_total_lh',
    'n_sg': 'no_sg',
    'num_sg': 'no_sg',
    'n_de_nascidos': 'no_nascidos',
    'n_de_palhetas_vials_crio': 'no_de_palhetas_vials_crio',
    'num_de_palhetas_vials_crio': 'no_de_palhetas_vials_crio',
    'motivo_cancer': 'tipo_cancer',
    'resp__crio': 'resp_crio',
    'gnrh__bloqueio': 'gnrh_bloqueio',
    'end__espessura': 'end_espessura',
    'n_fol__14': 'n_fol_14',
    'altura__cm': 'altura',
}

# ==============================================================================
# EXPLICT PER-TABLE CONFIGURATIONS ("BY HAND")
# ==============================================================================
# Use this section to map specific columns and set filters for individual tables.
# If a table is listed here, it overrides the global heuristics.
# ==============================================================================
TABLE_CONFIGS = {
    'planilha_2021_ibira_anual_jan_dez_certo': {
        'sheet_name': 'TOTAL',
        'header_row': 1,
        'fresh': {
            'mapping': {
                'nome_da_paciente': 'NOME',
                'data_de_nasc': 'DATA DE NASC.',
                'pin': 'PIN',
                'tipo_1': 'TIPO 1',
                'data_da_puncao': 'DIA',
                'fator_1': 'FATOR 1',
                'incubadora': 'INCUB',
                'data_crio': 'DATA CRIO',
                # New columns - suggested mappings:
                'tipo_de_inseminacao': 'TIPO 2',  
                'tipo_biopsia': 'TIPO 3',  
                'altura': 'ALTURA',
                'peso': 'PESO',
                'idade_espermatozoide': '', 
                'origem_espermatozoide': '',
                'tipo_espermatozoide': '',
                'opu': 'OPU',
                'total_de_mii': 'MII',
                'qtd_blasto': '# BLASTO',
                'qtd_blasto_tq_a_e_b': '# BLASTO TQ',  
                'no_biopsiados': '# DPI',  
                'qtd_analisados': 'N° ANALISADOS', 
                'qtd_normais': '# DPI NL', 
                'dia_cryo': 'DIA CRIO'
            },
            'filters': ['FIC/ICSI', 'FIV/ICSI', 'FOT', 'FOT OR', 'OR', 'FRESH']
        },
        'fet': {
            'mapping': {
                'nome_da_paciente': 'NOME',
                'data_de_nasc': 'DATA DE NASC.',
                'pin': 'PIN',
                'tipo_1': 'TIPO 1',
                'data_da_fet': 'DIA',
                'data_crio': 'DATA CRIO',
                'result': 'RESULT',
                'tipo_do_resultado': 'ADMINSTRAÇÃO 1',
                'no_nascidos': '',
                'tipo_de_tratamento': 'TIPO 1',
                'tipo_de_fet': 'TIPO 2',
                'tipo_biopsia': 'TIPO 3',
                'tipo_da_doacao': 'TIPO 1',
                'idade_mulher': 'IDADE',
                'idade_do_cong_de_embriao': 'IDADE OÓ NO CONG (PARA FET OU FOT)',
                'preparo_para_transferencia': '',
                'dia_cryo': 'DIA CRIO',
                'no_da_transfer_1a_2a_3a': '',
                'dia_et': 'DIA ET',
                'no_et': 'NºET',
                'gravidez_bioquimica': '',
                'gravidez_clinica': '',
                'obs': 'OBS'
            },
            'filters': ['FET', 'FET/OR', 'FET/ER']
        }
    },
    'planilha_2021_sj_total_2021': {
        'sheet_name': 'TOTAL 2021',
        'header_row': 2,
        'fresh': {
            'mapping': {
                'nome_da_paciente': 'NOME',
                'data_de_nasc': 'DATA DE NASC.',
                'pin': 'PIN',
                'tipo_1': 'TIPO 1',
                'data_da_puncao': 'DATA',
                'fator_1': 'FATOR 1',
                'incubadora': 'INCUB',
                'data_crio': 'DATA CRIO',
                # New columns - suggested mappings:
                'tipo_de_inseminacao': 'TIPO 2',  
                'tipo_biopsia': 'TIPO 3',  
                'altura': 'ALTURA',
                'peso': 'PESO',
                'idade_espermatozoide': '', 
                'origem_espermatozoide': '',
                'tipo_espermatozoide': '',
                'opu': 'OPU',
                'total_de_mii': 'MII',
                'qtd_blasto': '# BLASTO',
                'qtd_blasto_tq_a_e_b': '# BLASTO TQ',  
                'no_biopsiados': '# DPI',  
                'qtd_analisados': '#ANALISADOS DPI', 
                'qtd_normais': '# DPI NL', 
                'dia_cryo': 'DIA CRYO'
            },
            'filters': ['FIC/ICSI', 'FIV/ICSI', 'FOT', 'FOT OR', 'OR', 'FRESH']
        },
        'fet': {
            'mapping': {
                'nome_da_paciente': 'NOME',
                'data_de_nasc': 'DATA DE NASC.',
                'pin': 'PIN',
                'tipo_1': 'TIPO 1',
                'data_da_fet': 'DATA',
                'data_crio': 'DATA CRIO',
                'result': 'RESULT',
                'tipo_do_resultado': 'ADMINSTRAÇÃO 1',
                'no_nascidos': '',
                'tipo_de_tratamento': 'TIPO 1',
                'tipo_de_fet': 'TIPO 2',
                'tipo_biopsia': 'TIPO 3',
                'tipo_da_doacao': 'TIPO 1',
                'idade_mulher': 'IDADE',
                'idade_do_cong_de_embriao': 'IDADE OÓ NO CONG',
                'preparo_para_transferencia': '',
                'dia_cryo': 'DIA CRYO',
                'no_da_transfer_1a_2a_3a': '',
                'dia_et': 'DIA ET',
                'no_et': 'NºET',
                'gravidez_bioquimica': '',
                'gravidez_clinica': '',
                'obs': 'OBS'
            },
            'filters': ['FET', 'FET/OR', 'FET/ER']
        }
    },
    'planilha_2021_vm_total': {
        'sheet_name': 'TOTAL',
        'header_row': 2,
        'fresh': {
            'mapping': {
                'nome_da_paciente': 'NOME',
                'data_de_nasc': 'DATA DE NASC.',
                'pin': 'PIN',
                'tipo_1': 'TIPO 1',
                'data_da_puncao': 'DIA',
                'fator_1': 'FATOR 1',
                'incubadora': 'INCUB D5',
                'data_crio': 'DATA CRIO',
                # New columns - suggested mappings:
                'tipo_de_inseminacao': 'TIPO 2',  
                'tipo_biopsia': 'TIPO 3',  
                'altura': 'ALTURA',
                'peso': 'PESO',
                'idade_espermatozoide': '', 
                'origem_espermatozoide': '',
                'tipo_espermatozoide': '',
                'opu': 'OPU',
                'total_de_mii': 'MII',
                'qtd_blasto': '# BLASTO',
                'qtd_blasto_tq_a_e_b': '# BLASTO TQ',  
                'no_biopsiados': '# DPI',  
                'qtd_analisados': 'Nº analisados', 
                'qtd_normais': '# DPI NL', 
                'dia_cryo': 'DIA CRYO'
            },
            'filters': ['FIC/ICSI', 'FIV/ICSI', 'FOT', 'FOT OR', 'OR', 'FRESH']
        },
        'fet': {
            'mapping': {
                'nome_da_paciente': 'NOME',
                'data_de_nasc': 'DATA DE NASC.',
                'pin': 'PIN',
                'tipo_1': 'TIPO 1',
                'data_da_fet': 'DIA',
                'data_crio': 'DATA CRIO',
                'result': 'RESULT',
                'tipo_do_resultado': 'ADMINSTRAÇÃO 1',
                'no_nascidos': '',
                'tipo_de_tratamento': 'TIPO 1',
                'tipo_de_fet': 'TIPO 2',
                'tipo_biopsia': 'TIPO 3',
                'tipo_da_doacao': 'TIPO 1',
                'idade_mulher': 'IDADE',
                'idade_do_cong_de_embriao': '',
                'preparo_para_transferencia': '',
                'dia_cryo': 'DIA CRYO',
                'no_da_transfer_1a_2a_3a': '',
                'dia_et': 'DIA ET',
                'no_et': 'NºET',
                'gravidez_bioquimica': '',
                'gravidez_clinica': '',
                'obs': 'OBS'
            },
            'filters': ['FET', 'FET/OR', 'FET/ER']
        }
    },
    'planilha_2022_ibira_total': {
        'sheet_name': 'TOTAL',
        'header_row': 1,
        'fresh': {
            'mapping': {
                'nome_da_paciente': 'NOME',
                'data_de_nasc': 'DATA DE NASC.',
                'pin': 'PIN',
                'tipo_1': 'TIPO 1',
                'data_da_puncao': 'DIA',
                'fator_1': 'FATOR 1',
                'incubadora': 'INCUB',
                'data_crio': 'DATA CRIO',
                # New columns - suggested mappings:
                'tipo_de_inseminacao': 'TIPO 2',  
                'tipo_biopsia': 'TIPO 3',  
                'altura': 'ALTURA',
                'peso': 'PESO',
                'idade_espermatozoide': '', 
                'origem_espermatozoide': '',
                'tipo_espermatozoide': '',
                'opu': 'OPU',
                'total_de_mii': 'MII',
                'qtd_blasto': '# BLASTO',
                'qtd_blasto_tq_a_e_b': '# BLASTO TQ',  
                'no_biopsiados': '# DPI',  
                'qtd_analisados': 'N° ANALISADOS', 
                'qtd_normais': '# DPI NL', 
                'dia_cryo': 'DIA CRIO'
            },
            'filters': ['FIC/ICSI', 'FIV/ICSI', 'FOT', 'FOT OR', 'OR', 'FRESH']
        },
        'fet': {
            'mapping': {
                'nome_da_paciente': 'NOME',
                'data_de_nasc': 'DATA DE NASC.',
                'pin': 'PIN',
                'tipo_1': 'TIPO 1',
                'data_da_fet': 'DIA',
                'data_crio': 'DATA CRIO',
                'result': 'RESULT',
                'tipo_do_resultado': 'ADMINISTRAÇÃO 1',
                'no_nascidos': '',
                'tipo_de_tratamento': 'TIPO 1',
                'tipo_de_fet': 'TIPO 2',
                'tipo_biopsia': 'TIPO 3',
                'tipo_da_doacao': 'TIPO 1',
                'idade_mulher': 'IDADE',
                'idade_do_cong_de_embriao': 'IDADE OÓ NO CONG (PARA FET OU FOT)',
                'preparo_para_transferencia': '',
                'dia_cryo': 'DIA CRIO',
                'no_da_transfer_1a_2a_3a': '',
                'dia_et': 'DIA ET',
                'no_et': 'NºET',
                'gravidez_bioquimica': '',
                'gravidez_clinica': '',
                'obs': 'OBS'
            },
            'filters': ['FET', 'FET/OR', 'FET/ER']
        }
    },
    'planilha_2022_sj_total_2022': {
        'sheet_name': 'TOTAL 2022',
        'header_row': 2,
        'fresh': {
            'mapping': {
                'nome_da_paciente': 'NOME',
                'data_de_nasc': 'DATA DE NASC.',
                'pin': 'PIN',
                'tipo_1': 'TIPO 1',
                'data_da_puncao': 'DIA',
                'fator_1': 'FATOR 1',
                'incubadora': 'INCUB',
                'data_crio': 'DATA CRIO',
                # New columns - suggested mappings:
                'tipo_de_inseminacao': 'TIPO 2',  
                'tipo_biopsia': 'TIPO 3',  
                'altura': 'ALTURA',
                'peso': 'PESO',
                'idade_espermatozoide': '', 
                'origem_espermatozoide': '',
                'tipo_espermatozoide': '',
                'opu': 'OPU',
                'total_de_mii': 'MII',
                'qtd_blasto': '# BLASTO',
                'qtd_blasto_tq_a_e_b': '# BLASTO TQ',  
                'no_biopsiados': '# DPI',  
                'qtd_analisados': '#ANALISADOS DPI', 
                'qtd_normais': '# DPI NL', 
                'dia_cryo': 'DIA CRYO'
            },
            'filters': ['FIC/ICSI', 'FIV/ICSI', 'FOT', 'FOT OR', 'OR', 'FRESH']
        },
        'fet': {
            'mapping': {
                'nome_da_paciente': 'NOME',
                'data_de_nasc': 'DATA DE NASC.',
                'pin': 'PIN',
                'tipo_1': 'TIPO 1',
                'data_da_fet': 'DIA',
                'data_crio': 'DATA CRIO',
                'result': 'RESULT',
                'tipo_do_resultado': 'ADMINISTRAÇÃO 1',
                'no_nascidos': '',
                'tipo_de_tratamento': 'TIPO 1',
                'tipo_de_fet': 'TIPO 2',
                'tipo_biopsia': 'TIPO 3',
                'tipo_da_doacao': 'TIPO 1',
                'idade_mulher': 'IDADE',
                'idade_do_cong_de_embriao': 'IDADE OÓ NO CONG',
                'preparo_para_transferencia': '',
                'dia_cryo': 'DIA CRYO',
                'no_da_transfer_1a_2a_3a': '',
                'dia_et': 'DIA ET',
                'no_et': 'NºET',
                'gravidez_bioquimica': '',
                'gravidez_clinica': '',
                'obs': 'OBS'
            },
            'filters': ['FET', 'FET/OR', 'FET/ER']
        }
    },
    'planilha_2022_vm_total': {
        'sheet_name': 'TOTAL',
        'header_row': 2,
        'fresh': {
            'mapping': {
                'nome_da_paciente': 'NOME',
                'data_de_nasc': 'DATA DE NASC.',
                'pin': 'PIN',
                'tipo_1': 'TIPO 1',
                'data_da_puncao': 'DIA',
                'fator_1': 'FATOR 1',
                'incubadora': 'INCUB D5',
                'data_crio': 'DATA CRIO',
                # New columns - suggested mappings:
                'tipo_de_inseminacao': 'TIPO 2',  
                'tipo_biopsia': 'TIPO 3',  
                'altura': 'ALTURA',
                'peso': 'PESO',
                'idade_espermatozoide': '', 
                'origem_espermatozoide': '',
                'tipo_espermatozoide': '',
                'opu': 'OPU',
                'total_de_mii': 'MII',
                'qtd_blasto': '# BLASTO',
                'qtd_blasto_tq_a_e_b': '# BLASTO TQ',  
                'no_biopsiados': '# DPI',  
                'qtd_analisados': 'Nº analisados', 
                'qtd_normais': '# DPI NL', 
                'dia_cryo': 'DIA CRYO'
            },
            'filters': ['FIC/ICSI', 'FIV/ICSI', 'FOT', 'FOT OR', 'OR', 'FRESH']
        },
        'fet': {
            'mapping': {
                'nome_da_paciente': 'NOME',
                'data_de_nasc': 'DATA DE NASC.',
                'pin': 'PIN',
                'tipo_1': 'TIPO 1',
                'data_da_fet': 'DIA',
                'data_crio': 'DATA CRIO',
                'result': 'RESULT',
                'tipo_do_resultado': 'ADMINSTRAÇÃO 1',
                'no_nascidos': '',
                'tipo_de_tratamento': 'TIPO 1',
                'tipo_de_fet': 'TIPO 2',
                'tipo_biopsia': 'TIPO 3',
                'tipo_da_doacao': 'TIPO 1',
                'idade_mulher': 'IDADE',
                'idade_do_cong_de_embriao': '',
                'preparo_para_transferencia': '',
                'dia_cryo': 'DIA CRYO',
                'no_da_transfer_1a_2a_3a': '',
                'dia_et': 'DIA ET',
                'no_et': 'NºET',
                'gravidez_bioquimica': '',
                'gravidez_clinica': '',
                'obs': 'OBS'
            },
            'filters': ['FET', 'FET/OR', 'FET/ER']
        }
    },
    'planilha_2023_ibira_total_2023': {
        'sheet_name': 'TOTAL 2023 Nova',
        'header_row': 1,
        'fresh': {
            'mapping': {
                'nome_da_paciente': 'NOME',
                'data_de_nasc': 'DATA DE NASC.',
                'pin': 'PIN',
                'tipo_1': 'TIPO 1',
                'data_da_puncao': 'DIA',
                'fator_1': 'FATOR 1',
                'incubadora': 'INCUB',
                'data_crio': 'DATA CRIO',
                # New columns - suggested mappings:
                'tipo_de_inseminacao': 'TIPO 2',  
                'tipo_biopsia': 'TIPO 3',  
                'altura': 'ALTURA',
                'peso': 'PESO',
                'idade_espermatozoide': '', 
                'origem_espermatozoide': '',
                'tipo_espermatozoide': '',
                'opu': 'OPU',
                'total_de_mii': 'MII',
                'qtd_blasto': '# BLASTO',
                'qtd_blasto_tq_a_e_b': '# BLASTO TQ',  
                'no_biopsiados': '# DPI',  
                'qtd_analisados': 'N° ANALISADOS', 
                'qtd_normais': '# DPI NL', 
                'dia_cryo': 'DIA CRIO'
            },
            'filters': ['FIC/ICSI', 'FIV/ICSI', 'FOT', 'FOT OR', 'OR', 'FRESH']
        },
        'fet': {
            'mapping': {
                'nome_da_paciente': 'NOME',
                'data_de_nasc': 'DATA DE NASC.',
                'pin': 'PIN',
                'tipo_1': 'TIPO 1',
                'data_da_fet': 'DIA',
                'data_crio': 'DATA CRIO',
                'result': 'RESULT',
                'tipo_do_resultado': 'ADMINISTRAÇÃO 1',
                'no_nascidos': '',
                'tipo_de_tratamento': 'TIPO 1',
                'tipo_de_fet': 'TIPO 2',
                'tipo_biopsia': 'TIPO 3',
                'tipo_da_doacao': 'TIPO 1',
                'idade_mulher': 'IDADE',
                'idade_do_cong_de_embriao': 'IDADE OÓ NO CONG (PARA FET OU FOT)',
                'preparo_para_transferencia': '',
                'dia_cryo': 'DIA CRIO',
                'no_da_transfer_1a_2a_3a': '',
                'dia_et': 'DIA ET',
                'no_et': 'NºET',
                'gravidez_bioquimica': '',
                'gravidez_clinica': '',
                'obs': 'OBS'
            },
            'filters': ['FET', 'FET/OR', 'FET/ER']
        }
    },
    'planilha_2023_sj_total_2023': {
        'sheet_name': 'TOTAL 2023',
        'header_row': 2,
        'fresh': {
            'mapping': {
                'nome_da_paciente': 'NOME',
                'data_de_nasc': 'DATA DE NASC.',
                'pin': 'PIN',
                'tipo_1': 'TIPO 1',
                'data_da_puncao': 'DIA',
                'fator_1': 'FATOR 1',
                'incubadora': 'INCUB',
                'data_crio': 'DATA CRIO',
                # New columns - suggested mappings:
                'tipo_de_inseminacao': 'TIPO 2',  
                'tipo_biopsia': 'TIPO 3',  
                'altura': 'ALTURA',
                'peso': 'PESO',
                'idade_espermatozoide': '', 
                'origem_espermatozoide': '',
                'tipo_espermatozoide': '',
                'opu': 'OPU',
                'total_de_mii': 'MII',
                'qtd_blasto': '# BLASTO',
                'qtd_blasto_tq_a_e_b': '# BLASTO TQ',  
                'no_biopsiados': '# DPI',  
                'qtd_analisados': '#ANALISADOS DPI', 
                'qtd_normais': '# DPI NL', 
                'dia_cryo': 'DIA CRYO'
            },
            'filters': ['FIC/ICSI', 'FIV/ICSI', 'FOT', 'FOT OR', 'OR', 'FRESH']
        },
        'fet': {
            'mapping': {
                'nome_da_paciente': 'NOME',
                'data_de_nasc': 'DATA DE NASC.',
                'pin': 'PIN',
                'tipo_1': 'TIPO 1',
                'data_da_fet': 'DIA',
                'data_crio': 'DATA CRIO',
                'result': 'RESULT',
                'tipo_do_resultado': 'ADMINSTRAÇÃO 1',
                'no_nascidos': '',
                'tipo_de_tratamento': 'TIPO 1',
                'tipo_de_fet': 'TIPO 2',
                'tipo_biopsia': 'TIPO 3',
                'tipo_da_doacao': 'TIPO 1',
                'idade_mulher': 'IDADE',
                'idade_do_cong_de_embriao': 'IDADE OÓ NO CONG',
                'preparo_para_transferencia': '',
                'dia_cryo': 'DIA CRYO',
                'no_da_transfer_1a_2a_3a': '',
                'dia_et': 'DIA ET',
                'no_et': 'NºET',
                'gravidez_bioquimica': '',
                'gravidez_clinica': '',
                'obs': 'OBS'
            },
            'filters': ['FET', 'FET/OR', 'FET/ER']
        }
    },
    'planilha_2023_vm_geral_2023': {
        'sheet_name': 'GERAL 2023',
        'header_row': 2,
        'fresh': {
            'mapping': {
                'nome_da_paciente': 'NOME',
                'data_de_nasc': 'DATA DE NASC.',
                'pin': 'PIN',
                'tipo_1': 'TIPO 1',
                'data_da_puncao': 'DIA',
                'fator_1': 'FATOR 1',
                'incubadora': 'INCUB D5',
                'data_crio': 'DATA CRIO',
                # New columns - suggested mappings:
                'tipo_de_inseminacao': 'TIPO 2',  
                'tipo_biopsia': 'TIPO 3',  
                'altura': 'ALTURA',
                'peso': 'PESO',
                'idade_espermatozoide': '', 
                'origem_espermatozoide': '',
                'tipo_espermatozoide': '',
                'opu': 'OPU',
                'total_de_mii': 'MII',
                'qtd_blasto': '# BLASTO',
                'qtd_blasto_tq_a_e_b': '# BLASTO TQ',  
                'no_biopsiados': '# DPI',  
                'qtd_analisados': 'Nº analisados', 
                'qtd_normais': '# DPI NL', 
                'dia_cryo': 'DIA CRYO'
            },
            'filters': ['FIC/ICSI', 'FIV/ICSI', 'FOT', 'FOT OR', 'OR', 'FRESH']
        },
        'fet': {
            'mapping': {
                'nome_da_paciente': 'NOME',
                'data_de_nasc': 'DATA DE NASC.',
                'pin': 'PIN',
                'tipo_1': 'TIPO 1',
                'data_da_fet': 'DIA',
                'data_crio': 'DATA CRIO',
                'result': 'RESULT',
                'tipo_do_resultado': 'ADMINSTRAÇÃO 1',
                'no_nascidos': '',
                'tipo_de_tratamento': 'TIPO 1',
                'tipo_de_fet': 'TIPO 2',
                'tipo_biopsia': 'TIPO 3',
                'tipo_da_doacao': 'TIPO 1',
                'idade_mulher': 'IDADE',
                'idade_do_cong_de_embriao': '',
                'preparo_para_transferencia': '',
                'dia_cryo': 'DIA CRYO',
                'no_da_transfer_1a_2a_3a': '',
                'dia_et': 'DIA ET',
                'no_et': 'NºET',
                'gravidez_bioquimica': '',
                'gravidez_clinica': '',
                'obs': 'OBS'
            },
            'filters': ['FET', 'FET/OR', 'FET/ER']
        }
    },
    'planilha_2024_ibira_fresh': {
        'sheet_name': '2024',
        'header_row': 1,
        'fresh': {
            'mapping': {
                'nome_da_paciente': 'NOME',
                'data_de_nasc': 'DATA DE NASC.',
                'pin': 'PIN',
                'tipo_1': 'TIPO DE TRATAMENTO',
                'data_da_puncao': 'DATA DA PUNÇÃO',
                'fator_1': 'FATOR 1',
                'incubadora': 'INCUBADORA',
                'data_crio': 'DATA CRIO (SOMENTE A PRIMEIRA DATA DO CONG)',
                'tipo_de_inseminacao': 'TIPO DE INSEMINAÇÃO',
                'tipo_biopsia': 'TIPO BIÓPSIA',
                'altura': 'ALTURA',
                'peso': 'PESO',
                'idade_espermatozoide': 'IDADE ESPERMATOZOIDE',
                'origem_espermatozoide': 'ORIGEM',
                'tipo_espermatozoide': 'TIPO',
                'opu': 'OPU',
                'total_de_mii': 'TOTAL DE MII',
                'qtd_blasto': 'QTD BLASTO',
                'qtd_blasto_tq_a_e_b': 'QTD BLASTO TQ (A E B)',
                'no_biopsiados': 'Nº BIOPSIADOS',
                'qtd_analisados': 'QTD ANALISADOS',
                'qtd_normais': 'QTD NORMAIS',
                'dia_cryo': 'DIA CRYO'
            },
            'filters': ['FRESH', 'FRESH + FOT PRÓPRIO']
        }
    },
    'planilha_2024_ibira_fet': {
        'sheet_name': '2024',
        'header_row': 1,
        'fet': {
            'mapping': {
                'nome_da_paciente': 'NOME',
                'data_de_nasc': 'DATA DE NASC.',
                'pin': 'PIN',
                'tipo_1': 'TIPO DE TRATAMENTO',
                'data_da_fet': 'DATA DA FET',
                'data_crio': 'DATA CRIO',
                'result': 'RESULT',
                'tipo_do_resultado': 'TIPO DO RESULTADO',
                'no_nascidos': 'Nº NASCIDOS',
                'tipo_de_tratamento': 'TIPO DE TRATAMENTO',
                'tipo_de_fet': 'TIPO DE FET',
                'tipo_biopsia': 'TIPO BIÓPSIA',
                'tipo_da_doacao': 'TIPO DA DOAÇÃO',
                'idade_mulher': 'IDADE MULHER',
                'idade_do_cong_de_embriao': 'IDADE DO CONG DE EMBRIÃO',
                'preparo_para_transferencia': 'PREPARO PARA TRANSFERENCIA',
                'dia_cryo': 'DIA CRYO',
                'no_da_transfer_1a_2a_3a': 'NÚMERO DA TRANSFER (1ª, 2ª, 3ª...)',
                'dia_et': 'DIA ET',
                'no_et': 'Nº ET',
                'gravidez_bioquimica': 'GRAVIDEZ BIOQUIMICA',
                'gravidez_clinica': 'GRAVIDEZ CLINICA'
            },
            'filters': []  # Include all rows for FET
        }
    },
    'planilha_2022_bh_2022': {
        'sheet_name': '2022',
        'fresh': {
            'mapping': {
                'nome_da_paciente': 'NOME',
                'data_de_nasc': 'DATA DE NASC.',
                'pin': 'PIN',
                'tipo_1': 'TIPO 1',
                'data_da_puncao': 'DATA',
                'fator_1': 'FATOR 1',
                'incubadora': 'INCUB',
                'data_crio': 'DATA CRIO',
                'tipo_de_inseminacao': 'TIPO 2',
                'tipo_biopsia': 'TIPO 3',
                'altura': 'ALTURA',
                'peso': 'PESO',
                'idade_espermatozoide': '',
                'origem': 'ORIGEM',
                'tipo': 'TIPO',
                'opu': 'OPU',
                'total_de_mii': 'MII',
                'qtd_blasto': '# BLASTO',
                'qtd_blasto_tq_a_e_b': '# BLASTO TQ',
                'no_biopsiados': '# DPI',
                'qtd_analisados': 'QTD ANALISADOS',
                'qtd_normais': 'QTD NORMAIS',
                'dia_cryo': 'DIA CRYO'
            },
            'filters': ['FIC/ICSI', 'FIV/ICSI', 'FOT', 'FOT OR', 'OR', 'FRESH', 'CONG. ÓVULOS', 'CONG']
        },
        'fet': {
            'mapping': {
                'nome_da_paciente': 'NOME',
                'data_de_nasc': 'DATA DE NASC.',
                'pin': 'PIN',
                'tipo_1': 'TIPO 1',
                'data_da_fet': 'DATA',
                'data_crio': 'DATA CRIO',
                'result': 'RESULT',
                'tipo_do_resultado': 'TIPO DO RESULTADO',
                'no_nascidos': '',
                'tipo_de_tratamento': 'TIPO 1',
                'tipo_de_fet': 'TIPO 2',
                'tipo_biopsia': 'TIPO 3',
                'tipo_da_doacao': 'TIPO 1',
                'idade_mulher': 'IDADE',
                'idade_do_cong_de_embriao': '',
                'preparo_para_transferencia': '',
                'dia_cryo': 'DIA CRYO',
                'no_da_transfer_1a_2a_3a': '',
                'dia_et': 'DIA ET',
                'no_et': 'NºET',
                'gravidez_bioquimica': '',
                'gravidez_clinica': '',
                'obs': 'OBS'
            },
            'filters': ['FET', 'FET/OR', 'FET/ER', 'DESCONG']
        }
    },
    'planilha_2023_ibira_total_2023_nova': {
        'sheet_name': 'Total 2023 Nova ',
        'header_row': 1,
        'fresh': {
            'mapping': {
                'nome_da_paciente': 'NOME',
                'data_de_nasc': 'DATA DE NASC.',
                'pin': 'PIN',
                'tipo_1': 'TIPO 1',
                'data_da_puncao': 'DIA',
                'fator_1': 'FATOR 1',
                'incubadora': 'INCUB',
                'data_crio': 'DATA CRIO',
                'tipo_de_inseminacao': 'TIPO 2',  
                'tipo_biopsia': 'TIPO 3',  
                'altura': 'ALTURA',
                'peso': 'PESO',
                'idade_espermatozoide': '', 
                'origem': 'ORIGEM',
                'tipo': 'TIPO',
                'opu': 'OPU',
                'total_de_mii': 'MII',
                'qtd_blasto': '# BLASTO',
                'qtd_blasto_tq_a_e_b': '# BLASTO TQ',  
                'no_biopsiados': '# DPI',  
                'qtd_analisados': 'N° ANALISADOS', 
                'qtd_normais': '# DPI NL', 
                'dia_cryo': 'DIA CRIO'
            },
            'filters': ['FIC/ICSI', 'FIV/ICSI', 'FOT', 'FOT OR', 'OR', 'FRESH']
        },
        'fet': {
            'mapping': {
                'nome_da_paciente': 'NOME',
                'data_de_nasc': 'DATA DE NASC.',
                'pin': 'PIN',
                'tipo_1': 'TIPO 1',
                'data_da_fet': 'DIA',
                'data_crio': 'DATA CRIO',
                'result': 'RESULT',
                'tipo_do_resultado': 'ADMINISTRAÇÃO 1',
                'no_nascidos': '',
                'tipo_de_tratamento': 'TIPO 1',
                'tipo_de_fet': 'TIPO 2',
                'tipo_biopsia': 'TIPO 3',
                'tipo_da_doacao': 'TIPO 1',
                'idade_mulher': 'IDADE',
                'idade_do_cong_de_embriao': 'IDADE OÓ NO CONG (PARA FET OU FOT)',
                'preparo_para_transferencia': '',
                'dia_cryo': 'DIA CRIO',
                'no_da_transfer_1a_2a_3a': '',
                'dia_et': 'DIA ET',
                'no_et': 'NºET',
                'gravidez_bioquimica': '',
                'gravidez_clinica': '',
                'obs': 'OBS'
            },
            'filters': ['FET', 'FET/OR', 'FET/ER']
        }
    },
    'planilha_2022_ssa_fiv': {
        'sheet_name': 'FIV',
        'fresh': {
            'mapping': {
                'nome_da_paciente': 'Paciente',
                'data_de_nasc': 'Nasc.',
                'pin': 'Prontuário',
                'tipo_1': 'Proced.',
                'data_da_puncao': 'Data',
                'fator_1': 'Causa',
                'incubadora': 'Incub.',
                'data_crio': 'Vitrif',
                'tipo_de_inseminacao': 'ICSI',
                'tipo_biopsia': '',
                'altura': '',
                'peso': '',
                'idade_espermatozoide': '',
                'origem': 'origem',
                'tipo': 'Tipo',
                'opu': 'Capt',
                'total_de_mii': 'MII',
                'qtd_blasto': 'Blast',
                'qtd_blasto_tq_a_e_b': 'Vitrif.1',
                'no_biopsiados': '#BLAST DPI',
                'qtd_analisados': '#BLAST ANALISE',
                'qtd_normais': 'NORMAIS',
                'dia_cryo': 'Dia'
            },
            'filters': []
        }
    },
    'planilha_2022_ssa_tec': {
        'sheet_name': 'TEC',
        'fet': {
            'mapping': {
                'nome_da_paciente': 'Paciente',
                'data_de_nasc': 'Nasc.',
                'pin': 'Prontuário',
                'tipo_1': 'Proced.',
                'data_da_fet': 'Data',
                'data_crio': 'Data.1',
                'result': 'Beta',
                'tipo_do_resultado': 'Tipo',
                'no_nascidos': '',
                'tipo_de_tratamento': 'Proced.',
                'tipo_de_fet': 'Proced.',
                'tipo_biopsia': '',
                'tipo_da_doacao': '',
                'idade_mulher': 'Idade',
                'idade_do_cong_de_embriao': '',
                'preparo_para_transferencia': '',
                'dia_cryo': 'D',
                'no_da_transfer_1a_2a_3a': 'nº',
                'dia_et': 'Dia',
                'no_et': 'Transf',
                'gravidez_bioquimica': 'Beta',
                'gravidez_clinica': 'SG',
                'obs': 'Obs'
            },
            'filters': []
        }
    },
    'planilha_2023_ssa_fiv': {
        'sheet_name': 'FIV',
        'fresh': {
            'mapping': {
                'nome_da_paciente': 'NOME',
                'data_de_nasc': '',
                'pin': 'PRONTUÁRIO',
                'tipo_1': 'TIPO 1',
                'data_da_puncao': 'DATA',
                'fator_1': '',
                'incubadora': 'Incubadora',
                'data_crio': 'EMB.CRYO',
                'tipo_de_inseminacao': 'TIPO 2',
                'tipo_biopsia': '',
                'altura': '',
                'peso': '',
                'idade_espermatozoide': '',
                'origem': 'ORIGEM SPTZ',
                'tipo': 'TIPO SPTZ',
                'opu': 'OPU',
                'total_de_mii': 'MII',
                'qtd_blasto': '# BLASTO',
                'qtd_blasto_tq_a_e_b': '# BLASTO TQ',
                'no_biopsiados': '# DPI',
                'qtd_analisados': 'Nº ANALISADOS',
                'qtd_normais': '# DPI NL',
                'dia_cryo': 'DIA CRYO'
            },
            'filters': []
        }
    },
    'planilha_2023_ssa_tec': {
        'sheet_name': 'TEC',
        'fet': {
            'mapping': {
                'nome_da_paciente': 'Paciente',
                'data_de_nasc': '',
                'pin': 'Prontuário',
                'tipo_1': 'Tipo 1',
                'data_da_fet': 'Data',
                'data_crio': 'Data Cong.',
                'result': 'Beta',
                'tipo_do_resultado': 'Tipo 2',
                'no_nascidos': '',
                'tipo_de_tratamento': 'Tipo 1',
                'tipo_de_fet': 'Tipo 2',
                'tipo_biopsia': '',
                'tipo_da_doacao': '',
                'idade_mulher': 'Idade',
                'idade_do_cong_de_embriao': '',
                'preparo_para_transferencia': '',
                'dia_cryo': 'D',
                'no_da_transfer_1a_2a_3a': '',
                'dia_et': 'Dia transf.',
                'no_et': 'Transf',
                'gravidez_bioquimica': 'Beta',
                'gravidez_clinica': 'SG',
                'obs': 'Observações'
            },
            'filters': []
        }
    }
}

def get_duckdb_connection():
    """Create DuckDB connection"""
    try:
        logger.info(f"Attempting to connect to DuckDB at: {DUCKDB_PATH}")
        con = duckdb.connect(DUCKDB_PATH)
        logger.info("DuckDB connection successful")
        return con
    except Exception as e:
        logger.error(f"Failed to connect to DuckDB: {e}")
        raise

def normalize_column_name(col_name):
    """Normalize column name: remove accents, lowercase, trim spaces, handle special chars"""
    if pd.isna(col_name) or col_name is None:
        return None
    
    # Convert to string
    col_str = str(col_name)
    
    # Replace all whitespace characters (including newlines) with a single space
    col_str = re.sub(r'\s+', ' ', col_str).strip()
    
    # Remove accents/diacritics
    col_str = unicodedata.normalize('NFD', col_str)
    col_str = ''.join(c for c in col_str if unicodedata.category(c) != 'Mn')
    
    # Convert to lowercase
    col_str = col_str.lower()

    # Convert to snake_case: replace any non-alphanumeric with underscores, then collapse
    col_str = re.sub(r'[^a-z0-9]', '_', col_str)
    col_str = re.sub(r'_+', '_', col_str).strip('_')
    
    # Handle specific common variations to ensure they normalize to the same key
    # 1. Spacing variations in "1 PN"
    if col_str == "1pn":
        col_str = "1_pn"
    
    # 2. Known direct synonyms (including user-defined)
    synonyms = {
        'idade do espermatozoide ': 'idade espermatozoide'
    }
    synonyms.update(SYNONYMS)
    
    if col_str in synonyms:
        col_str = synonyms[col_str]
    
    return col_str

def get_bronze_tables(con, sheet_type=None):
    """Get all bronze tables matching the pattern, optionally filtered by sheet type, including shared tables."""
    try:
        shared_condition = "(table_name LIKE '%_total%' OR table_name LIKE '%_geral%' OR table_name LIKE '%_anual%' OR table_name LIKE '%_2022' OR table_name LIKE '%_sheet1')"
        if sheet_type == 'fresh':
            condition = f"(table_name LIKE '%_fresh' OR table_name LIKE '%_fiv' OR {shared_condition})"
        elif sheet_type == 'fet':
            condition = f"(table_name LIKE '%_fet' OR table_name LIKE '%_tec' OR {shared_condition})"
        elif sheet_type == 'recep':
            condition = f"(table_name LIKE '%_recep' OR {shared_condition})"
        elif sheet_type == 'fot':
            condition = f"(table_name LIKE '%_fot' OR {shared_condition})"
        elif sheet_type == 'doadoras':
            condition = "(table_name LIKE '%_doadoras')"
        elif sheet_type == 'fp_ovulos':
            condition = f"(((table_name LIKE '%_fp_cong_ovulos%' OR table_name LIKE '%_fp') AND table_name NOT LIKE '%_semen%') OR {shared_condition})"
        elif sheet_type == 'fp_semen':
            condition = "(table_name LIKE '%_fp_cong_de_semen%' OR table_name LIKE '%_semen%')"
        elif sheet_type == 'iiu':
            condition = f"(table_name LIKE '%_iiu' OR {shared_condition})"
        else:
            condition = "1=1"

        query = f"""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'bronze' 
            AND table_name LIKE '{BRONZE_PATTERN}'
            AND {condition}
            ORDER BY table_name
        """
        bronze_tables = con.execute(query).fetchdf()['table_name'].tolist()
        
        # Filter for years we want to process
        bronze_tables = [
            t for t in bronze_tables 
            if any(year in t for year in YEARS_TO_PROCESS)
        ]
        
        if sheet_type:
            logger.info(f"Found {len(bronze_tables)} bronze tables for '{sheet_type}' for years {YEARS_TO_PROCESS}")
        
        return bronze_tables
    except Exception as e:
        logger.error(f"Error getting bronze tables: {e}")
        return []

def collect_all_columns_from_tables(con, bronze_tables, sheet_type):
    """Collect all unique columns and create standardization mapping, prioritizing Ibirapuera 2024"""
    logger.info(f"Collecting columns from all bronze tables for {sheet_type}...")
    
    all_original_columns = {}  # normalized_name -> list of original names
    table_columns = {}  # table_name -> list of original columns
    
    # Identify reference table
    reference_table = REFERENCE_TABLES.get(sheet_type)
    reference_columns_map = {} # normalized -> original_name from reference table
    
    logger.info(f"Using reference table: {reference_table}")
    
    for table_name in bronze_tables:
        try:
            # Get column names from table
            columns_info = con.execute(f"DESCRIBE bronze.{table_name}").fetchdf()
            original_cols = [col for col in columns_info['column_name'].tolist() 
                           if col not in ['line_number', 'extraction_timestamp', 'file_name', 'sheet_name']]
            table_columns[table_name] = original_cols
            
            # Normalize and collect
            for orig_col in original_cols:
                normalized = normalize_column_name(orig_col)
                # Filter by whitelist
                if normalized in WHITELIST.get(sheet_type, []):
                    if normalized not in all_original_columns:
                        all_original_columns[normalized] = []
                    if orig_col not in all_original_columns[normalized]:
                        all_original_columns[normalized].append(orig_col)
                    
                    # Store as reference if this is the reference table
                    if table_name == reference_table:
                        reference_columns_map[normalized] = orig_col
            
            # logger.info(f"  {table_name}: {len(original_cols)} columns") # Too noisy
        except Exception as e:
            logger.warning(f"Could not get columns from {table_name}: {e}")

    # Create standardization mapping
    standardization_map = {}
    problematic_columns = []
    
    for normalized, original_list in all_original_columns.items():
        # User requested snake_case for normalized names.
        # We use the 'normalized' key itself as the standard name.
        standardization_map[normalized] = normalized
        
        # If there were multiple variants, log them for awareness
        if len(set(original_list)) > 1:
            problematic_columns.append({
                'normalized': normalized,
                'variants': list(set(original_list)),
                'chosen': normalized,
                'reason': 'Standardized to snake_case normalized name'
            })
    
    logger.info(f"Total unique normalized columns: {len(standardization_map)}")
    if problematic_columns:
        logger.info(f"Standardized {len(problematic_columns)} columns with multiple variants.")
    
    return standardization_map, problematic_columns, table_columns

def detect_column_types(df, sample_size=1000):
    """Detect column types by analyzing data patterns"""
    logger.info("Detecting column data types...")
    
    data_columns = [col for col in df.columns if col not in ['line_number', 'extraction_timestamp', 'file_name', 'sheet_name']]
    column_types = {}
    
    # Sample data for analysis (to speed up detection)
    sample_df = df[data_columns].head(min(sample_size, len(df)))
    
    ALWAYS_TEXT_COLS = {
        'pin', 'prontuario', 'nome_da_paciente', 'tipo_1', 'fator_1', 'incubadora',
        'tipo', 'origem', 'obs', 'result', 'tipo_do_resultado', 'tipo_biopsia',
        'tipo_de_inseminacao', 'tipo_de_fet', 'tipo_de_tratamento', 'tipo_da_doacao',
        'preparo_para_transferencia', 'gravidez_bioquimica', 'gravidez_clinica',
        'dia_cryo', 'dia_et', 'file_name', 'sheet_name',
        'motivo_do_congelamento', 'tipo_cancer', 'protocolo', 'gnrh_bloqueio',
        'fsh', 'lh', 'medicamento_maturacao_ovulacao', 'tipo_da_cirurgia',
        'amostra_do_tecido', 'preparo', 'metodo_crio', 'conjuge',
        'indicacao_clinica', 'medicamento_indutor', 'responsavel_pelo_preparo',
        'tecnica_de_preparo', 'tipo_de_parto', 'ohss', 'hemorragia', 'infeccao'
    }

    COUNT_COLS = {
        'opu', 'mii_total', 'mii_doados_fresco', 'mii_doados_crio', 'mii_crio', 'mi_crio',
        'numero_de_fragmentos_crio', 'no_de_palhetas_vials_crio', 'no_sg', 'no_nascidos',
        'qtd_blasto', 'qtd_blasto_tq_a_e_b', 'no_biopsiados', 'qtd_analisados', 'qtd_normais', 'no_et'
    }
    
    for col in data_columns:
        col_norm = col.lower()
        if col_norm in ALWAYS_TEXT_COLS:
            column_types[col] = 'VARCHAR'
            continue
        if col_norm in COUNT_COLS:
            column_types[col] = 'INTEGER'
            continue
            
        # Get non-null values
        non_null_values = sample_df[col].dropna()
        
        if len(non_null_values) == 0:
            column_types[col] = 'VARCHAR'  # Default to VARCHAR if all null
            continue
        
        # Try to detect dates
        date_count = 0
        for val in non_null_values.head(100):  # Check first 100 non-null values
            val_str = str(val).strip()
            # Check if it looks like a date (contains date patterns)
            if val_str and (
                '202' in val_str or '201' in val_str or '200' in val_str or  # Years
                '/' in val_str or '-' in val_str or  # Date separators
                len(val_str) >= 8  # Date-like length
            ):
                try:
                    # Try parsing as date
                    pd.to_datetime(val_str, errors='raise')
                    date_count += 1
                except:
                    pass
        
        # If >50% of values are dates, treat as date column
        if date_count > len(non_null_values.head(100)) * 0.5:
            column_types[col] = 'TIMESTAMP'
            logger.debug(f"  {col}: detected as TIMESTAMP ({date_count}/{len(non_null_values.head(100))} date values)")
            continue
        
        # Try to detect numbers
        numeric_count = 0
        for val in non_null_values.head(100):
            val_str = str(val).strip()
            if val_str:
                # Remove common non-numeric characters but keep decimal point and minus
                cleaned = val_str.replace(',', '').replace(' ', '')
                try:
                    float(cleaned)
                    numeric_count += 1
                except:
                    pass
        
        # If >80% of values are numeric, treat as numeric column
        if numeric_count > len(non_null_values.head(100)) * 0.8:
            # Check if it's integer or float
            is_integer = True
            for val in non_null_values.head(50):
                val_str = str(val).strip().replace(',', '').replace(' ', '')
                try:
                    if '.' in val_str or 'e' in val_str.lower() or 'E' in val_str:
                        is_integer = False
                        break
                except:
                    pass
            
            if is_integer:
                column_types[col] = 'INTEGER'
                logger.debug(f"  {col}: detected as INTEGER ({numeric_count}/{len(non_null_values.head(100))} numeric values)")
            else:
                column_types[col] = 'DOUBLE'
                logger.debug(f"  {col}: detected as DOUBLE ({numeric_count}/{len(non_null_values.head(100))} numeric values)")
        else:
            column_types[col] = 'VARCHAR'
    
    logger.info(f"Column type detection completed: {sum(1 for t in column_types.values() if t == 'TIMESTAMP')} dates, "
                f"{sum(1 for t in column_types.values() if t in ['INTEGER', 'DOUBLE'])} numeric, "
                f"{sum(1 for t in column_types.values() if t == 'VARCHAR')} text")
    
    return column_types

def standardize_dataframe_columns(df, standardization_map):
    """Standardize column names in DataFrame using the mapping, ensuring unique column names"""
    column_mapping = {}
    used_standard_names = {}  # Track how many times we've used each standard name
    
    for orig_col in df.columns:
        if orig_col in ['line_number', 'extraction_timestamp', 'file_name', 'sheet_name']:
            # Keep metadata columns as-is
            column_mapping[orig_col] = orig_col
        else:
            # Normalize and map to standard name
            normalized = normalize_column_name(orig_col)
            if normalized and normalized in standardization_map:
                standard_name = standardization_map[normalized]
                
                # Handle duplicates within a single table: 
                # if we've already used this standard name, append counter
                if standard_name in used_standard_names:
                    used_standard_names[standard_name] += 1
                    unique_standard_name = f"{standard_name}_{used_standard_names[standard_name]}"
                    column_mapping[orig_col] = unique_standard_name
                    logger.debug(f"Renamed duplicate standard column '{standard_name}' to '{unique_standard_name}' for '{orig_col}'")
                else:
                    used_standard_names[standard_name] = 0
                    column_mapping[orig_col] = standard_name
            else:
                # If not in map (not whitelisted), drop it (don't add to mapping)
                continue
    
    # Rename and filter columns
    # cols_to_keep are those that were mapped plus existing metadata columns
    df_renamed = df[list(column_mapping.keys())].rename(columns=column_mapping)
    
    # Verify no duplicates
    if len(df_renamed.columns) != len(set(df_renamed.columns)):
        duplicates = [col for col in df_renamed.columns if list(df_renamed.columns).count(col) > 1]
        logger.error(f"ERROR: Still have duplicate columns after standardization: {set(duplicates)}")
        raise ValueError(f"Duplicate columns found: {set(duplicates)}")
    
    return df_renamed

def clean_data(df, sheet_type):
    """Clean data by removing blank lines, rows with AUXILIAR = 0, and rows missing both PIN and procedure date"""
    logger.info("Cleaning data...")
    
    initial_count = len(df)
    
    # Get data columns (exclude metadata AND AUXILIAR)
    # AUXILIAR is excluded because rows with only AUXILIAR are considered blank
    metadata_cols = ['file_name', 'sheet_name']
    auxiliar_cols = ['AUXILIAR', 'Auxiliar', 'auxiliar']  # Handle different casings
    exclude_cols = metadata_cols + auxiliar_cols + ['line_number', 'extraction_timestamp']
    data_cols = [col for col in df.columns if col not in exclude_cols]
    
    # Step 1: Remove rows where AUXILIAR = 0 or '0'
    auxiliar_col = None
    for col in auxiliar_cols:
        if col in df.columns:
            auxiliar_col = col
            break
    
    if auxiliar_col:
        # Remove rows where AUXILIAR is 0 or '0'
        mask_auxiliar = ~((df[auxiliar_col] == 0) | (df[auxiliar_col] == '0'))
        df = df[mask_auxiliar].copy()
        auxiliar_removed = initial_count - len(df)
        if auxiliar_removed > 0:
            logger.info(f"Removed {auxiliar_removed:,} rows with {auxiliar_col} = 0")
    
    # Step 2: Remove completely blank rows (excluding AUXILIAR from check)
    # Create a mask for rows where ALL data columns are blank (NaN or empty string)
    # A row is considered blank if all data columns are either NaN, None, or empty string
    def is_blank(val):
        if pd.isna(val):
            return True
        if isinstance(val, str) and val.strip() == '':
            return True
        return False
    
    # Check each row - keep only rows that have at least one non-blank data value
    mask = df[data_cols].apply(lambda row: not all(is_blank(val) for val in row), axis=1)
    df_clean = df[mask].copy()
    
    blank_removed = len(df) - len(df_clean)
    if blank_removed > 0:
        logger.info(f"Removed {blank_removed:,} completely blank rows")
    
    # Step 3: Remove rows where both PIN and procedure date are blank
    df = df_clean.copy()
    initial_step3_count = len(df)
    
    # Find PIN column
    pin_col = next((col for col in df.columns if normalize_column_name(col) == 'pin'), 'pin')
    
    # Determine procedure date column based on sheet type
    if sheet_type.upper() in ['FRESH', 'FOT', 'DOADORAS', 'FP_OVULOS']:
        date_col = next((col for col in df.columns if normalize_column_name(col) in ['data_da_puncao', 'data_do_procedimento', 'data_inicio_inducao', 'data_crio', 'dia_cryo', 'dia', 'data_da_cirurgia']), 'data_da_puncao')
    elif sheet_type.upper() in ['IIU']:
        date_col = next((col for col in df.columns if normalize_column_name(col) in ['data_do_procedimento', 'data_inicial_da_inducao', 'data']), 'data_do_procedimento')
    elif sheet_type.upper() in ['FP_SEMEN']:
        date_col = next((col for col in df.columns if normalize_column_name(col) in ['data_do_procedimento', 'data']), 'data_do_procedimento')
    else:  # FET, RECEP
        date_col = next((col for col in df.columns if normalize_column_name(col) in ['data_da_fet', 'data_da_transferencia', 'data_do_procedimento', 'data_crio', 'dia_cryo', 'dia']), 'data_da_fet')
    
    if pin_col in df.columns and date_col in df.columns:
        # A row is kept if either PIN or date is NOT blank
        mask_keys = ~(df[pin_col].apply(is_blank) & df[date_col].apply(is_blank))
        df_clean = df[mask_keys].copy()
        keys_removed = initial_step3_count - len(df_clean)
        if keys_removed > 0:
            logger.info(f"Removed {keys_removed:,} rows missing both {pin_col} and {date_col}")
    else:
        logger.warning(f"Could not find {pin_col} or {date_col} for key-based cleaning (Columns present: {pin_col in df.columns}, {date_col in df.columns})")
        df_clean = df

    total_removed = initial_count - len(df_clean)
    logger.info(f"Total rows removed: {total_removed:,}")
    logger.info(f"Total rows after cleaning: {len(df_clean):,}")
    
    return df_clean

def transform_data_types(df, column_types):
    """Transform DataFrame columns to proper data types"""
    logger.info("Transforming data types...")
    
    df_transformed = df.copy()
    exclude_cols = ['line_number', 'extraction_timestamp', 'file_name', 'sheet_name']
    data_columns = [col for col in df.columns if col not in exclude_cols]
    
    for col in data_columns:
        col_type = column_types.get(col, 'VARCHAR')
        
        if col_type == 'TIMESTAMP':
            # Convert to datetime
            df_transformed[col] = pd.to_datetime(df_transformed[col], errors='coerce')
            logger.debug(f"  Converted {col} to TIMESTAMP")
        
        elif col_type == 'INTEGER':
            # Convert to integer (handle commas, spaces, etc.)
            df_transformed[col] = df_transformed[col].astype(str).str.replace(',', '').str.replace(' ', '')
            # Convert to float first, then round, then to nullable integer
            numeric_series = pd.to_numeric(df_transformed[col], errors='coerce')
            # Round to nearest integer (handles cases like 0.0, 1.0, etc.)
            numeric_series = numeric_series.round()
            # Convert float to int using numpy, then to nullable Int64
            # Handle NaN values properly by only converting non-NaN values
            mask = pd.isna(numeric_series)
            # Create array with NaN where mask is True, int values where mask is False
            int_values = np.full(len(numeric_series), np.nan, dtype=float)
            if not mask.all():
                int_values[~mask] = numeric_series[~mask].astype(int)
            df_transformed[col] = pd.array(int_values, dtype='Int64')
            logger.debug(f"  Converted {col} to INTEGER")
        
        elif col_type == 'DOUBLE':
            # Convert to float
            df_transformed[col] = df_transformed[col].astype(str).str.replace(',', '').str.replace(' ', '')
            df_transformed[col] = pd.to_numeric(df_transformed[col], errors='coerce')
            logger.debug(f"  Converted {col} to DOUBLE")
        
        else:
            # Clean VARCHAR columns: strip whitespace and uppercase standard outcome fields
            if col in ['result', 'tipo_do_resultado', 'gravidez_clinica', 'gravidez_bioquimica', 'ohss', 'hemorragia', 'infeccao']:
                df_transformed[col] = df_transformed[col].astype(str).str.strip().str.upper()
                df_transformed[col] = df_transformed[col].replace({'NAN': None, 'NONE': None, '<NA>': None, '': None})
    
    logger.info("Data type transformation completed")
    return df_transformed

def add_prontuario_column(con, silver_table):
    """Populate prontuario column via find_prontuarios (Strategy L)."""
    logger.info(f"Running prontuario matching for silver.{silver_table} ...")

    _TIER_LABELS = {
        0: 'Tier 0 (Direct ID)',
        1: 'Tier 1 (CPF)',
        2: 'Tier 2 (ID + Birthdate)',
        3: 'Tier 3 (Spousal link)',
    }

    try:
        clinisys_db_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
            'database', 'clinisys_all.duckdb'
        )
        df_matches = find_prontuarios(
            source_con=con,
            clinisys_db_path=clinisys_db_path,
            source_schema='silver',
            source_table=silver_table,
            id_col='pin',
            name_col='nome_da_paciente',
            birthdate_col='data_de_nasc',
            cpf_col=None,
            label=silver_table,
            suffix='',
        )

        total   = len(df_matches)
        matched = int((df_matches['prontuario'] != -1).sum())
        rate    = matched / total * 100 if total else 0.0

        logger.info(f"=== PRONTUARIO MATCHING SUMMARY for {silver_table} ===")
        logger.info(f"  Total    : {total:,}")
        logger.info(f"  Matched  : {matched:,}  ({rate:.2f}%)")
        logger.info(f"  Unmatched: {total - matched:,}")
        if total > 0:
            tier_counts = (
                df_matches[df_matches['prontuario'] != -1]
                .groupby('match_tier')['source_id']
                .count()
                .sort_index()
            )
            for tier, cnt in tier_counts.items():
                logger.info(f"    {_TIER_LABELS.get(tier, f'Tier {tier}')}: {cnt:,}")
        if rate >= 95:
            logger.info(f"  Quality: EXCELLENT (>=95%)")
        elif rate >= 85:
            logger.info(f"  Quality: GOOD (>=85%)")
        elif rate >= 70:
            logger.info(f"  Quality: ACCEPTABLE (>=70%)")
        else:
            logger.warning(f"  Quality: NEEDS ATTENTION (<70%)")
        logger.info(f"=== END PRONTUARIO MATCHING SUMMARY ===")

    except Exception as e:
        logger.error(f"Error in prontuario matching for {silver_table}: {e}")
        raise

def create_silver_table(con, df, column_types, silver_table):
    """Create silver table with proper schema based on detected column types"""
    logger.info(f"Creating silver table: {silver_table}")
    
    # Create silver schema if it doesn't exist
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    
    # Drop existing table to ensure fresh data
    con.execute(f"DROP TABLE IF EXISTS silver.{silver_table}")
    logger.info(f"Dropped existing silver.{silver_table} table")
    
    # Get all columns from DataFrame (excluding metadata columns)
    exclude_cols = ['line_number', 'extraction_timestamp', 'file_name', 'sheet_name']
    data_columns = [col for col in df.columns if col not in exclude_cols]
    
    # Create column definitions based on detected types
    column_definitions = []
    for col in data_columns:
        col_type = column_types.get(col, 'VARCHAR')
        # Map INTEGER to BIGINT for DuckDB (INTEGER in DuckDB is INT32, which is too small)
        if col_type == 'INTEGER':
            col_type = 'BIGINT'
        # Keep original column name in quotes for SQL
        column_definitions.append(f'"{col}" {col_type}')
    
    # Add metadata columns
    column_definitions.extend([
        'file_name VARCHAR',
        'sheet_name VARCHAR',
        'prontuario INTEGER'  # Add prontuario column
    ])
    
    create_table_sql = f"""
    CREATE TABLE silver.{silver_table} (
        {', '.join(column_definitions)}
    )
    """
    
    con.execute(create_table_sql)
    logger.info(f"Table silver.{silver_table} created successfully with {len(data_columns)} data columns")

def process_bronze_to_silver(con, sheet_type):
    """Process bronze tables for a specific sheet type and create corresponding silver table"""
    silver_table = f'planilha_embriologia_{sheet_type}'
    
    logger.info("=" * 50)
    logger.info(f"BRONZE TO SILVER TRANSFORMATION - {sheet_type.upper()}")
    logger.info("=" * 50)
    
    # Get all bronze tables
    bronze_tables = get_bronze_tables(con, sheet_type)
    
    if not bronze_tables:
        logger.warning("No bronze tables found matching pattern")
        return 0, []
    
    # Collect and standardize columns
    standardization_map, problematic_columns, table_columns = collect_all_columns_from_tables(con, bronze_tables, sheet_type)
    
    # Read and combine all bronze tables
    all_dataframes = []
    table_dataframe_map = {}  # Map table_name to dataframe
    
    for table_name in bronze_tables:
        logger.info(f"Reading data from bronze.{table_name}...")
        try:
            df = con.execute(f"SELECT * FROM bronze.{table_name}").fetchdf()
            
            # 1. Apply Per-Table Config or Global Heuristic
            table_config = TABLE_CONFIGS.get(table_name)
            if not table_config and any(k in table_name for k in ['_total', '_geral', '_anual', '_2022']):
                alias = table_name.replace('_ibi_', '_ibira_')
                if alias in TABLE_CONFIGS:
                    table_config = TABLE_CONFIGS[alias]
            
            # Check if there is a specific config for this sheet_type inside the table_config
            type_config = None
            if table_config:
                if sheet_type in table_config:
                    type_config = table_config[sheet_type]
                elif sheet_type in ['recep', 'fot']:
                    base_type = 'fet' if sheet_type == 'recep' else 'fresh'
                    if base_type in table_config:
                        base_map = table_config[base_type].get('mapping', {}).copy()
                        if sheet_type == 'recep' and 'pin_doadora' not in base_map:
                            base_map['pin_doadora'] = 'PIN DOADORA'
                        type_config = {
                            'mapping': base_map,
                            'filters': TIPO_FILTERS.get(sheet_type, [])
                        }
                elif sheet_type == 'fp_ovulos':
                    if 'fresh' in table_config:
                        fresh_map = table_config['fresh'].get('mapping', {})
                        fp_map = {
                            'nome_da_paciente': fresh_map.get('nome_da_paciente', 'NOME'),
                            'data_de_nasc': fresh_map.get('data_de_nasc', 'DATA DE NASC.'),
                            'pin': fresh_map.get('pin', 'PIN'),
                            'tipo_1': fresh_map.get('tipo_1', 'TIPO 1'),
                            'tipo_de_tratamento': fresh_map.get('tipo_1', 'TIPO 1'),
                            'data_do_procedimento': fresh_map.get('data_da_puncao', 'DIA'),
                            'opu': fresh_map.get('opu', 'OPU'),
                            'mii_crio': fresh_map.get('total_de_mii', 'MII'),
                            'fator_1': fresh_map.get('fator_1', 'FATOR 1'),
                            'incubadora': fresh_map.get('incubadora', 'INCUB'),
                            'dia_cryo': fresh_map.get('dia_cryo', 'DIA CRYO'),
                            'idade': fresh_map.get('idade', 'IDADE'),
                        }
                        type_config = {
                            'mapping': fp_map,
                            'filters': TIPO_FILTERS.get(sheet_type, [])
                        }
                elif sheet_type == 'iiu':
                    ref_map = table_config.get('fresh', table_config.get('fet', {})).get('mapping', {})
                    iiu_map = {
                        'nome_da_paciente': ref_map.get('nome_da_paciente', 'NOME'),
                        'data_de_nasc': ref_map.get('data_de_nasc', 'DATA DE NASC.'),
                        'pin': ref_map.get('pin', 'PIN'),
                        'tipo_1': ref_map.get('tipo_1', 'TIPO 1'),
                        'tipo_de_tratamento': ref_map.get('tipo_1', 'TIPO 1'),
                        'data_do_procedimento': ref_map.get('data_da_puncao', ref_map.get('data_da_fet', 'DIA')),
                        'result': 'RESULT',
                        'no_sg': 'SG',
                        'fator_1': ref_map.get('fator_1', 'FATOR 1'),
                    }
                    type_config = {
                        'mapping': iiu_map,
                        'filters': TIPO_FILTERS.get(sheet_type, [])
                    }
                elif 'mapping' in table_config:
                    # Fallback for old structure or shared mapping
                    type_config = table_config
            
            if type_config:
                logger.info(f"  Using explicit 'by hand' configuration for {table_name} ({sheet_type})")
                # Manual Mapping
                manual_map = type_config.get('mapping', {})
                cols_in_table = df.columns.tolist()
                
                # Build df_standardized directly from manual_map without dict key collision
                df_standardized = pd.DataFrame(index=df.index)
                for silver_col, bronze_col in manual_map.items():
                    if bronze_col:
                        match = next((c for c in cols_in_table if c == bronze_col or normalize_column_name(c) == normalize_column_name(bronze_col)), None)
                        if match and match in df.columns:
                            df_standardized[silver_col] = df[match]
                        else:
                            df_standardized[silver_col] = None
                    else:
                        df_standardized[silver_col] = None
                
                # Include metadata columns
                for meta_col in ['file_name', 'sheet_name', 'line_number', 'extraction_timestamp']:
                    if meta_col in df.columns:
                        df_standardized[meta_col] = df[meta_col]
                
                logger.info(f"  After column selection: {len(df_standardized)} rows, {len(df_standardized.columns)} columns")
                
                # Manual Filters
                allowed_types = type_config.get('filters', [])

            else:
                # Global Standardization logic
                df_standardized = standardize_dataframe_columns(df, standardization_map)
                is_shared_table = any(k in table_name.lower() for k in ['_total', '_geral', '_anual', '_2022'])
                if is_shared_table:
                    allowed_types = TIPO_FILTERS.get(sheet_type, [])
                else:
                    allowed_types = []  # Dedicated procedure sheet (keep all rows)

            # 2. Filter by TIPO 1 (Prefix matching) immediately
            if 'tipo_1' in df_standardized.columns and allowed_types:
                initial_count = len(df_standardized)
                mask_tipo = df_standardized['tipo_1'].fillna('').apply(
                    lambda x: any(str(x).upper().strip().startswith(t.upper()) for t in allowed_types)
                )
                df_standardized = df_standardized[mask_tipo].copy()
                removed = initial_count - len(df_standardized)
                if removed > 0:
                    logger.info(f"  Removed {removed:,} rows not matching {sheet_type} prefixes {allowed_types}")
            
            if len(df_standardized) > 0:
                all_dataframes.append(df_standardized)
                logger.info(f"  Read {len(df):,} rows, kept {len(df_standardized):,} after TIPO 1 filtering")
            else:
                logger.info(f"  No rows remaining after TIPO 1 filtering")
                
        except Exception as e:
            logger.error(f"Error processing table {table_name}: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            continue
    
    if not all_dataframes:
        logger.warning("No data to process")
        return 0, problematic_columns
    
    # Combine all dataframes
    logger.info("Combining all dataframes...")
    
    # Get all unique columns from all dataframes
    all_columns_set = set()
    for df in all_dataframes:
        all_columns_set.update(df.columns)
    
    # Get column order from reference table
    # Since we changed table names, we'll try to find any 'planilha_..._fet' table as reference
    reference_table_found = False
    metadata_cols = ['file_name', 'sheet_name']
    all_metadata_cols = ['line_number', 'extraction_timestamp', 'file_name', 'sheet_name']
    
    # Look for a reference table (prefer ..._ibi_fet / ..._ibi_fresh)
    possible_references = [t for t in bronze_tables if 'ibi' in t.lower() and sheet_type in t.lower()]
    if not possible_references:
        # Fallback to any FET/FRESH table
        possible_references = [t for t in bronze_tables if sheet_type in t.lower()]
    if not possible_references:
        # Fallback to any table
        possible_references = bronze_tables
        
    reference_cols_original = None
    
    if possible_references:
        reference_table_name = possible_references[0]
        try:
            # Get original columns from bronze table (before standardization)
            columns_info = con.execute(f"DESCRIBE bronze.{reference_table_name}").fetchdf()
            original_cols = [col for col in columns_info['column_name'].tolist() 
                           if col not in all_metadata_cols]
            reference_cols_original = original_cols
            reference_table_found = True
            logger.info(f"Using {reference_table_name} as reference for column order ({len(original_cols)} columns)")
        except Exception as e:
            logger.warning(f"Could not get columns from {reference_table_name}: {e}")
    
    # If we found the reference table, use its column order (after standardization)
    if reference_cols_original is not None:
        # Map original columns to standardized names
        reference_cols_standardized = []
        for orig_col in reference_cols_original:
            # Apply standardization mapping
            normalized = normalize_column_name(orig_col)
            if normalized and normalized in standardization_map:
                standard_name = standardization_map[normalized]
                if standard_name not in reference_cols_standardized:
                    reference_cols_standardized.append(standard_name)
            # ELSE: If not in map, it's not whitelisted. DROP IT.
        
        # Move PIN to first position if it exists
        pin_cols = [col for col in reference_cols_standardized if normalize_column_name(col) == normalize_column_name('PIN')]
        if pin_cols:
            pin_col = pin_cols[0]
            reference_cols_standardized = [pin_col] + [col for col in reference_cols_standardized if col != pin_col]
            logger.info(f"Moving PIN column '{pin_col}' to first position")
        
        # Add any columns from other tables that aren't in the reference
        other_cols = [col for col in all_columns_set if col not in reference_cols_standardized and col not in metadata_cols]
        data_cols = reference_cols_standardized + sorted(other_cols)  # Add missing columns in alphabetical order
    else:
        # Fallback to alphabetical if reference table not found
        logger.warning("No reference table found, using alphabetical order")
        all_columns_ordered = sorted(list(all_columns_set))
        data_cols = [col for col in all_columns_ordered if col not in metadata_cols]
        # Move PIN to first if it exists
        pin_cols = [col for col in data_cols if normalize_column_name(col) == normalize_column_name('PIN')]
        if pin_cols:
            pin_col = pin_cols[0]
            data_cols = [pin_col] + [col for col in data_cols if col != pin_col]
    
    final_column_order = data_cols + [col for col in metadata_cols if col in all_columns_set]
    
    # Ensure all dataframes have the same columns (add missing as None)
    standardized_dfs = []
    for df in all_dataframes:
        df_aligned = df.copy()
        # Add missing columns
        for col in final_column_order:
            if col not in df_aligned.columns:
                df_aligned[col] = None
        # Reorder columns
        df_aligned = df_aligned[final_column_order]
        standardized_dfs.append(df_aligned)
    
    df_combined = pd.concat(standardized_dfs, ignore_index=True)
    logger.info(f"Combined {len(all_dataframes)} tables into {len(df_combined)} rows with {len(final_column_order)} columns")
    
    # Clean data
    df_clean = clean_data(df_combined, sheet_type)
    
    if len(df_clean) == 0:
        logger.warning("No data remaining after cleaning")
        return 0, problematic_columns
    
    # Detect column types
    column_types = detect_column_types(df_clean)
    
    # Transform data types
    df_transformed = transform_data_types(df_clean, column_types)
    
    # Create silver table
    create_silver_table(con, df_transformed, column_types, silver_table)
    
    logger.info(f"Inserting {len(df_transformed)} rows to silver layer")
    
    # Register DataFrame for SQL insertion
    con.register('temp_silver_data', df_transformed)
    
    # Build INSERT statement with proper type casting
    exclude_cols = ['line_number', 'extraction_timestamp', 'file_name', 'sheet_name']
    data_columns = [col for col in df_transformed.columns if col not in exclude_cols]
    all_columns = data_columns + ['file_name', 'sheet_name']
    
    # Build column list and select list with proper casting
    column_list = ', '.join([f'"{col}"' if col in data_columns else col for col in all_columns])
    select_parts = []
    for col in all_columns:
        if col in data_columns:
            col_type = column_types.get(col, 'VARCHAR')
            if col_type == 'TIMESTAMP':
                select_parts.append(f'CAST("{col}" AS TIMESTAMP) as "{col}"')
            elif col_type == 'INTEGER':
                # Use BIGINT for DuckDB (INTEGER in DuckDB is INT32, which is too small)
                select_parts.append(f'CAST("{col}" AS BIGINT) as "{col}"')
            elif col_type == 'DOUBLE':
                select_parts.append(f'CAST("{col}" AS DOUBLE) as "{col}"')
            else:
                select_parts.append(f'CAST("{col}" AS VARCHAR) as "{col}"')
        else:
            select_parts.append(f'CAST({col} AS VARCHAR) as {col}')
    
    select_list = ', '.join(select_parts)
    
    insert_sql = f"""
    INSERT INTO silver.{silver_table} ({column_list})
    SELECT {select_list}
    FROM temp_silver_data
    """
    
    con.execute(insert_sql)
    
    # Clean up temporary table
    con.execute("DROP VIEW IF EXISTS temp_silver_data")
    
    logger.info(f"Successfully inserted {len(df_transformed)} rows to silver.{silver_table}")
    
    # Add prontuario column matching
    logger.info(f"Adding prontuario column to silver.{silver_table}...")
    add_prontuario_column(con, silver_table)
    
    logger.info(f"Successfully inserted {len(df_transformed)} rows to silver.{silver_table}")
    return len(df_transformed), problematic_columns

def main():
    """Main function to transform planilha_embriologia to silver"""
    logger.info("Starting Planilha Embriologia silver transformation")
    logger.info(f"DuckDB path: {DUCKDB_PATH}")
    logger.info(f"Processing sheet types: {SHEET_TYPES}")
    
    try:
        # Create DuckDB connection
        logger.info("Creating DuckDB connection...")
        con = get_duckdb_connection()
        logger.info("DuckDB connection created successfully")
        
        # Process each sheet type separately
        total_new_rows = 0
        all_problematic_columns = {}
        
        for sheet_type in SHEET_TYPES:
            logger.info("")
            logger.info("#" * 50)
            logger.info(f"Processing {sheet_type.upper()} tables")
            logger.info("#" * 50)
            
            new_rows, problematic_columns = process_bronze_to_silver(con, sheet_type)
            total_new_rows += new_rows
            
            if problematic_columns:
                all_problematic_columns[sheet_type] = problematic_columns
            
            # Get final table statistics for this sheet type
            silver_table = f'planilha_embriologia_{sheet_type}'
            result = con.execute(f'SELECT COUNT(*) FROM silver.{silver_table}').fetchone()
            total_rows = result[0] if result else 0
            
            logger.info(f"Rows inserted to silver.{silver_table}: {new_rows:,}")
            logger.info(f"Total rows in silver.{silver_table}: {total_rows:,}")
        
        # Final summary
        logger.info("")
        logger.info("=" * 50)
        logger.info("SILVER TRANSFORMATION SUMMARY")
        logger.info("=" * 50)
        logger.info(f"Total rows inserted across all tables: {total_new_rows:,}")
        
        # Report problematic columns for each sheet type
        if all_problematic_columns:
            logger.info("=" * 50)
            logger.info("COLUMNS WITH MULTIPLE VARIANTS (standardization issues):")
            logger.info("=" * 50)
            for sheet_type, problematic_columns in all_problematic_columns.items():
                logger.info(f"\n{sheet_type.upper()} sheet:")
                for item in problematic_columns:
                    logger.info(f"Normalized: '{item['normalized']}'")
                    logger.info(f"  Variants found: {item['variants']}")
                    logger.info(f"  Chosen standard: '{item['chosen']}'")
                    logger.info(f"  Reason: {item['reason']}")
                    logger.info("")
        else:
            logger.info("All columns standardized successfully!")
        
        logger.info("=" * 50)
        
        # Close connection
        con.close()
        logger.info("Planilha Embriologia silver transformation completed")
        
    except Exception as e:
        logger.error(f"Error in main function: {e}")
        raise

if __name__ == "__main__":
    main()
