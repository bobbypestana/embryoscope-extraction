import duckdb
import pandas as pd
import os
import logging
import numpy as np
from datetime import datetime
import yaml
from typing import Dict, List, Tuple, Any
import re

# Setup logging
LOGS_DIR = os.path.join(os.path.dirname(__file__), 'logs')
os.makedirs(LOGS_DIR, exist_ok=True)
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
LOG_PATH = os.path.join(LOGS_DIR, f'silver_loader_pandas_{timestamp}.log')

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.FileHandler(LOG_PATH),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load config
CONFIG_PATH = os.path.join(os.path.dirname(__file__), 'params.yml')
with open(CONFIG_PATH, 'r') as f:
    config = yaml.safe_load(f)

db_path = config.get('duckdb_path', '../database/clinisys_all.duckdb')

class DataQualityLogger:
    """Logs data quality issues for each table"""
    
    def __init__(self, table_name: str):
        self.table_name = table_name
        self.issues = []
        self.problematic_files = set()
        
    def log_issue(self, column: str, issue_type: str, value: Any, row_count: int = 1):
        """Log a data quality issue"""
        self.issues.append({
            'column': column,
            'issue_type': issue_type,
            'value': str(value)[:100],  # Truncate long values
            'row_count': row_count
        })
        
    def log_problematic_file(self, file_path: str, error: str):
        """Log a problematic file"""
        self.problematic_files.add((file_path, error))
        
    def get_summary(self) -> Dict:
        """Get summary of all issues"""
        return {
            'table_name': self.table_name,
            'total_issues': len(self.issues),
            'problematic_files': len(self.problematic_files),
            'issues_by_column': self._group_issues_by_column(),
            'issues_by_type': self._group_issues_by_type(),
            'problematic_files_details': list(self.problematic_files)
        }
    
    def _group_issues_by_column(self) -> Dict:
        """Group issues by column"""
        grouped = {}
        for issue in self.issues:
            col = issue['column']
            if col not in grouped:
                grouped[col] = []
            grouped[col].append(issue)
        return grouped
    
    def _group_issues_by_type(self) -> Dict:
        """Group issues by type"""
        grouped = {}
        for issue in self.issues:
            issue_type = issue['issue_type']
            if issue_type not in grouped:
                grouped[issue_type] = []
            grouped[issue_type].append(issue)
        return grouped

def safe_date_parse_pandas(series: pd.Series, logger: DataQualityLogger) -> pd.Series:
    """Safely parse dates using pandas, casting invalid dates to NULL"""
    if series.dtype == 'object':
        # Convert to string first
        series = series.astype(str)
    
    # Replace empty strings and 'nan' with None
    series = series.replace(['', 'nan', 'None', 'NULL'], None)
    
    # Try to parse dates
    try:
        # First try DD/MM/YYYY format
        parsed = pd.to_datetime(series, format='%d/%m/%Y', errors='coerce')
        
        # Log invalid dates
        invalid_mask = series.notna() & parsed.isna()
        if invalid_mask.any():
            invalid_values = series[invalid_mask].unique()
            for val in invalid_values:
                logger.log_issue('date_parsing', 'invalid_date_format', val, 
                               (series == val).sum())
        
        return parsed
    except Exception as e:
        logger.log_issue('date_parsing', 'parsing_error', str(e))
        return pd.Series([None] * len(series))

def safe_time_parse_pandas(series: pd.Series, logger: DataQualityLogger) -> pd.Series:
    """Safely parse times using pandas, casting invalid times to NULL"""
    if series.dtype == 'object':
        series = series.astype(str)
    
    # Replace empty strings and 'nan' with None
    series = series.replace(['', 'nan', 'None', 'NULL'], None)
    
    try:
        # Try to parse HH:MM format
        parsed = pd.to_datetime(series, format='%H:%M', errors='coerce').dt.time
        
        # Log invalid times
        invalid_mask = series.notna() & parsed.isna()
        if invalid_mask.any():
            invalid_values = series[invalid_mask].unique()
            for val in invalid_values:
                logger.log_issue('time_parsing', 'invalid_time_format', val,
                               (series == val).sum())
        
        return parsed
    except Exception as e:
        logger.log_issue('time_parsing', 'parsing_error', str(e))
        return pd.Series([None] * len(series))

def safe_numeric_parse_pandas(series: pd.Series, dtype: str, logger: DataQualityLogger) -> pd.Series:
    """Safely parse numeric values, casting invalid numbers to NULL"""
    if series.dtype == 'object':
        series = series.astype(str)
    
    # Replace empty strings and 'nan' with None
    series = series.replace(['', 'nan', 'None', 'NULL'], None)
    
    try:
        if dtype == 'int':
            # Handle comma-separated thousands and decimal points
            series_clean = series.str.replace(',', '').str.replace('.', '', regex=False)
            parsed = pd.to_numeric(series_clean, errors='coerce').astype('Int64')
        elif dtype == 'float':
            # Handle comma as decimal separator
            series_clean = series.str.replace(',', '.', regex=False)
            parsed = pd.to_numeric(series_clean, errors='coerce')
        else:
            parsed = pd.to_numeric(series, errors='coerce')
        
        # Log invalid numbers
        invalid_mask = series.notna() & parsed.isna()
        if invalid_mask.any():
            invalid_values = series[invalid_mask].unique()
            for val in invalid_values:
                logger.log_issue('numeric_parsing', 'invalid_number', val,
                               (series == val).sum())
        
        return parsed
    except Exception as e:
        logger.log_issue('numeric_parsing', 'parsing_error', str(e))
        return pd.Series([None] * len(series))

def safe_string_parse_pandas(series: pd.Series, logger: DataQualityLogger) -> pd.Series:
    """Safely parse strings, handling encoding issues"""
    try:
        # Convert to string, handling None values
        parsed = series.astype(str)
        parsed = parsed.replace('nan', None)
        return parsed
    except Exception as e:
        logger.log_issue('string_parsing', 'encoding_error', str(e))
        return pd.Series([None] * len(series))

# Define table transformations using pandas
table_transformations = {
    'view_medicamentos': {
        'id': ('int', safe_numeric_parse_pandas),
        'tipo': ('string', safe_string_parse_pandas),
        'tipo_ficha': ('string', safe_string_parse_pandas),
        'medicamento': ('string', safe_string_parse_pandas),
        'observacoes': ('string', safe_string_parse_pandas),
        'hash': ('string', safe_string_parse_pandas),
        'extraction_timestamp': ('datetime', lambda s, logger: pd.to_datetime(s, format='%Y%m%d_%H%M%S', errors='coerce'))
    },
    'view_medicamentos_prescricoes': {
        'id': ('int', safe_numeric_parse_pandas),
        'prontuario': ('int', safe_numeric_parse_pandas),
        'ficha_tipo': ('string', safe_string_parse_pandas),
        'ficha_id': ('int', safe_numeric_parse_pandas),
        'data_inicial': ('date', safe_date_parse_pandas),
        'data_final': ('date', safe_date_parse_pandas),
        'hora': ('time', safe_time_parse_pandas),
        'medicamento': ('int', safe_numeric_parse_pandas),
        'dose': ('string', safe_string_parse_pandas),
        'unidade': ('string', safe_string_parse_pandas),
        'via': ('string', safe_string_parse_pandas),
        'intervalo': ('float', safe_numeric_parse_pandas),
        'observacoes': ('string', safe_string_parse_pandas),
        'quantidade': ('int', safe_numeric_parse_pandas),
        'forma': ('string', safe_string_parse_pandas),
        'duracao': ('int', safe_numeric_parse_pandas),
        'hash': ('string', safe_string_parse_pandas),
        'extraction_timestamp': ('datetime', lambda s, logger: pd.to_datetime(s, format='%Y%m%d_%H%M%S', errors='coerce'))
    },
    'view_medicos': {
        'id': ('int', safe_numeric_parse_pandas),
        'nome': ('string', safe_string_parse_pandas),
        'tipo_medico': ('string', safe_string_parse_pandas),
        'registro': ('int', safe_numeric_parse_pandas),
        'especialidade': ('string', safe_string_parse_pandas),
        'rqe': ('float', safe_numeric_parse_pandas),
        'rqe_tipo': ('string', safe_string_parse_pandas),
        'hash': ('string', safe_string_parse_pandas),
        'extraction_timestamp': ('datetime', lambda s, logger: pd.to_datetime(s, format='%Y%m%d_%H%M%S', errors='coerce'))
    },
    'view_pacientes': {
        'codigo': ('int', safe_numeric_parse_pandas),
        'prontuario_esposa': ('int', safe_numeric_parse_pandas),
        'prontuario_marido': ('int', safe_numeric_parse_pandas),
        'prontuario_responsavel1': ('int', safe_numeric_parse_pandas),
        'prontuario_responsavel2': ('int', safe_numeric_parse_pandas),
        'prontuario_esposa_pel': ('int', safe_numeric_parse_pandas),
        'prontuario_marido_pel': ('int', safe_numeric_parse_pandas),
        'prontuario_esposa_pc': ('int', safe_numeric_parse_pandas),
        'prontuario_marido_pc': ('int', safe_numeric_parse_pandas),
        'prontuario_responsavel1_pc': ('int', safe_numeric_parse_pandas),
        'prontuario_responsavel2_pc': ('int', safe_numeric_parse_pandas),
        'prontuario_esposa_fc': ('int', safe_numeric_parse_pandas),
        'prontuario_marido_fc': ('int', safe_numeric_parse_pandas),
        'prontuario_esposa_ba': ('int', safe_numeric_parse_pandas),
        'prontuario_marido_ba': ('int', safe_numeric_parse_pandas),
        'esposa_nome': ('string', safe_string_parse_pandas),
        'marido_nome': ('string', safe_string_parse_pandas),
        'unidade_origem': ('float', safe_numeric_parse_pandas),
        'medico': ('string', safe_string_parse_pandas),
        'medico_encaminhante': ('string', safe_string_parse_pandas),
        'empresa_indicacao': ('string', safe_string_parse_pandas),
        'como_conheceu_huntington_outros': ('string', safe_string_parse_pandas),
        'cidade': ('string', safe_string_parse_pandas),
        'estado': ('string', safe_string_parse_pandas),
        'hash': ('string', safe_string_parse_pandas),
        'extraction_timestamp': ('datetime', lambda s, logger: pd.to_datetime(s, format='%Y%m%d_%H%M%S', errors='coerce'))
    },
    'view_tratamentos': {
        'id': ('int', safe_numeric_parse_pandas),
        'prontuario': ('int', safe_numeric_parse_pandas),
        'unidade': ('float', safe_numeric_parse_pandas),
        'idade_esposa': ('float', safe_numeric_parse_pandas),
        'idade_marido': ('string', safe_string_parse_pandas),
        'hash': ('string', safe_string_parse_pandas),
        'extraction_timestamp': ('datetime', lambda s, logger: pd.to_datetime(s, format='%Y%m%d_%H%M%S', errors='coerce'))
    },
    'view_unidades': {
        'id': ('int', safe_numeric_parse_pandas),
        'nome': ('string', safe_string_parse_pandas),
        'hash': ('string', safe_string_parse_pandas),
        'extraction_timestamp': ('datetime', lambda s, logger: pd.to_datetime(s, format='%Y%m%d_%H%M%S', errors='coerce'))
    },
    'view_congelamentos_embrioes': {
        'id': ('int', safe_numeric_parse_pandas),
        'CodCongelamento': ('string', safe_string_parse_pandas),
        'Unidade': ('int', safe_numeric_parse_pandas),
        'prontuario': ('int', safe_numeric_parse_pandas),
        'paciente': ('string', safe_string_parse_pandas),
        'Data': ('date', safe_date_parse_pandas),
        'Hora': ('time', safe_time_parse_pandas),
        'Ciclo': ('string', safe_string_parse_pandas),
        'CicloRecongelamento': ('string', safe_string_parse_pandas),
        'condicoes_amostra': ('string', safe_string_parse_pandas),
        'empresa_transporte': ('string', safe_string_parse_pandas),
        'clinica_origem': ('string', safe_string_parse_pandas),
        'responsavel_recebimento': ('float', safe_numeric_parse_pandas),
        'responsavel_recebimento_data': ('date', safe_date_parse_pandas),
        'responsavel_armazenamento': ('float', safe_numeric_parse_pandas),
        'responsavel_armazenamento_data': ('date', safe_date_parse_pandas),
        'NEmbrioes': ('int', safe_numeric_parse_pandas),
        'NPailletes': ('string', safe_string_parse_pandas),
        'Identificacao': ('string', safe_string_parse_pandas),
        'Tambor': ('string', safe_string_parse_pandas),
        'Cane': ('string', safe_string_parse_pandas),
        'Cane2': ('string', safe_string_parse_pandas),
        'Tecnica': ('string', safe_string_parse_pandas),
        'Ovulo': ('float', safe_numeric_parse_pandas),
        'D2': ('float', safe_numeric_parse_pandas),
        'D3': ('float', safe_numeric_parse_pandas),
        'D4': ('float', safe_numeric_parse_pandas),
        'D5': ('float', safe_numeric_parse_pandas),
        'D6': ('float', safe_numeric_parse_pandas),
        'D7': ('float', safe_numeric_parse_pandas),
        'rack': ('string', safe_string_parse_pandas),
        'rack2': ('float', safe_numeric_parse_pandas),
        'rack3': ('float', safe_numeric_parse_pandas),
        'rack4': ('float', safe_numeric_parse_pandas),
        'Observacoes': ('string', safe_string_parse_pandas),
        'BiologoResponsavel': ('float', safe_numeric_parse_pandas),
        'BiologoFIV': ('string', safe_string_parse_pandas),
        'BiologoFIV2': ('string', safe_string_parse_pandas),
        'status_financeiro': ('string', safe_string_parse_pandas),
        'responsavel_congelamento_d5': ('float', safe_numeric_parse_pandas),
        'responsavel_checagem_d5': ('float', safe_numeric_parse_pandas),
        'responsavel_congelamento_d6': ('float', safe_numeric_parse_pandas),
        'responsavel_checagem_d6': ('float', safe_numeric_parse_pandas),
        'responsavel_congelamento_d7': ('float', safe_numeric_parse_pandas),
        'responsavel_checagem_d7': ('float', safe_numeric_parse_pandas),
        'hash': ('string', safe_string_parse_pandas),
        'extraction_timestamp': ('datetime', lambda s, logger: pd.to_datetime(s, format='%Y%m%d_%H%M%S', errors='coerce'))
    },
    'view_congelamentos_ovulos': {
        'id': ('int', safe_numeric_parse_pandas),
        'CodCongelamento': ('string', safe_string_parse_pandas),
        'Unidade': ('int', safe_numeric_parse_pandas),
        'prontuario': ('int', safe_numeric_parse_pandas),
        'paciente': ('string', safe_string_parse_pandas),
        'Data': ('date', safe_date_parse_pandas),
        'Hora': ('time', safe_time_parse_pandas),
        'Ciclo': ('string', safe_string_parse_pandas),
        'condicoes_amostra': ('string', safe_string_parse_pandas),
        'empresa_transporte': ('string', safe_string_parse_pandas),
        'clinica_origem': ('string', safe_string_parse_pandas),
        'responsavel_recebimento': ('float', safe_numeric_parse_pandas),
        'responsavel_recebimento_data': ('date', safe_date_parse_pandas),
        'responsavel_armazenamento': ('float', safe_numeric_parse_pandas),
        'responsavel_armazenamento_data': ('date', safe_date_parse_pandas),
        'NOvulos': ('int', safe_numeric_parse_pandas),
        'NPailletes': ('string', safe_string_parse_pandas),
        'Identificacao': ('string', safe_string_parse_pandas),
        'Tambor': ('string', safe_string_parse_pandas),
        'Cane': ('string', safe_string_parse_pandas),
        'Cane2': ('string', safe_string_parse_pandas),
        'Tecnica': ('string', safe_string_parse_pandas),
        'Motivo': ('string', safe_string_parse_pandas),
        'Observacoes': ('string', safe_string_parse_pandas),
        'BiologoResponsavel': ('float', safe_numeric_parse_pandas),
        'BiologoFIV': ('string', safe_string_parse_pandas),
        'BiologoFIV2': ('string', safe_string_parse_pandas),
        'status_financeiro': ('string', safe_string_parse_pandas),
        'hash': ('string', safe_string_parse_pandas),
        'extraction_timestamp': ('datetime', lambda s, logger: pd.to_datetime(s, format='%Y%m%d_%H%M%S', errors='coerce'))
    },
    'view_descongelamentos_embrioes': {
        'id': ('int', safe_numeric_parse_pandas),
        'CodDescongelamento': ('string', safe_string_parse_pandas),
        'Unidade': ('float', safe_numeric_parse_pandas),
        'prontuario': ('int', safe_numeric_parse_pandas),
        'doadora': ('int', safe_numeric_parse_pandas),
        'DataCongelamento': ('date', safe_date_parse_pandas),
        'DataDescongelamento': ('date', safe_date_parse_pandas),
        'Ciclo': ('string', safe_string_parse_pandas),
        'Identificacao': ('string', safe_string_parse_pandas),
        'CodCongelamento': ('string', safe_string_parse_pandas),
        'Tambor': ('string', safe_string_parse_pandas),
        'Cane': ('string', safe_string_parse_pandas),
        'PailletesDescongeladas': ('string', safe_string_parse_pandas),
        'Tecnica': ('string', safe_string_parse_pandas),
        'Transferencia': ('float', safe_numeric_parse_pandas),
        'DataTransferencia': ('date', safe_date_parse_pandas),
        'Prateleira': ('float', safe_numeric_parse_pandas),
        'Incubadora': ('string', safe_string_parse_pandas),
        'transferidos_transferencia': ('string', safe_string_parse_pandas),
        'cateter_transferencia': ('string', safe_string_parse_pandas),
        'lote_transferencia': ('string', safe_string_parse_pandas),
        'validade_transferencia': ('string', safe_string_parse_pandas),
        'intercorrencia_transferencia': ('string', safe_string_parse_pandas),
        'sangue_interno_transferencia': ('string', safe_string_parse_pandas),
        'sangue_externo_transferencia': ('string', safe_string_parse_pandas),
        'retorno_transferencia': ('string', safe_string_parse_pandas),
        'vezes_retorno_transferencia': ('string', safe_string_parse_pandas),
        'Transfer_D5': ('string', safe_string_parse_pandas),
        'responsavel_transferencia': ('float', safe_numeric_parse_pandas),
        'Observacoes': ('string', safe_string_parse_pandas),
        'BiologoFIV': ('string', safe_string_parse_pandas),
        'BiologoFIV2': ('string', safe_string_parse_pandas),
        'hash': ('string', safe_string_parse_pandas),
        'extraction_timestamp': ('datetime', lambda s, logger: pd.to_datetime(s, format='%Y%m%d_%H%M%S', errors='coerce'))
    },
    'view_descongelamentos_ovulos': {
        'id': ('int', safe_numeric_parse_pandas),
        'CodDescongelamento': ('string', safe_string_parse_pandas),
        'Unidade': ('float', safe_numeric_parse_pandas),
        'prontuario': ('int', safe_numeric_parse_pandas),
        'doadora': ('int', safe_numeric_parse_pandas),
        'DataCongelamento': ('date', safe_date_parse_pandas),
        'DataDescongelamento': ('date', safe_date_parse_pandas),
        'Ciclo': ('string', safe_string_parse_pandas),
        'Identificacao': ('string', safe_string_parse_pandas),
        'CodCongelamento': ('string', safe_string_parse_pandas),
        'Tambor': ('string', safe_string_parse_pandas),
        'Cane': ('string', safe_string_parse_pandas),
        'PailletesDescongeladas': ('string', safe_string_parse_pandas),
        'Tecnica': ('string', safe_string_parse_pandas),
        'Observacoes': ('string', safe_string_parse_pandas),
        'BiologoFIV': ('string', safe_string_parse_pandas),
        'BiologoFIV2': ('string', safe_string_parse_pandas),
        'hash': ('string', safe_string_parse_pandas),
        'extraction_timestamp': ('datetime', lambda s, logger: pd.to_datetime(s, format='%Y%m%d_%H%M%S', errors='coerce'))
    },
    'view_embrioes_congelados': {
        'id': ('int', safe_numeric_parse_pandas),
        'id_oocito': ('int', safe_numeric_parse_pandas),
        'id_congelamento': ('int', safe_numeric_parse_pandas),
        'id_descongelamento': ('int', safe_numeric_parse_pandas),
        'prontuario': ('int', safe_numeric_parse_pandas),
        'hash': ('string', safe_string_parse_pandas),
        'extraction_timestamp': ('datetime', lambda s, logger: pd.to_datetime(s, format='%Y%m%d_%H%M%S', errors='coerce'))
    },
    'view_extrato_atendimentos_central': {
        'agendamento_id': ('int', safe_numeric_parse_pandas),
        'data': ('date', safe_date_parse_pandas),
        'inicio': ('string', safe_string_parse_pandas),
        'data_agendamento_original': ('date', safe_date_parse_pandas),
        'medico': ('float', safe_numeric_parse_pandas),
        'medico2': ('float', safe_numeric_parse_pandas),
        'prontuario': ('int', safe_numeric_parse_pandas),
        'evento': ('int', safe_numeric_parse_pandas),
        'evento2': ('string', safe_string_parse_pandas),
        'centro_custos': ('int', safe_numeric_parse_pandas),
        'agenda': ('int', safe_numeric_parse_pandas),
        'chegou': ('string', safe_string_parse_pandas),
        'confirmado': ('int', safe_numeric_parse_pandas),
        'paciente_codigo': ('float', safe_numeric_parse_pandas),
        'paciente_nome': ('string', safe_string_parse_pandas),
        'medico_nome': ('string', safe_string_parse_pandas),
        'medico_sobrenome': ('string', safe_string_parse_pandas),
        'medico2_nome': ('string', safe_string_parse_pandas),
        'centro_custos_nome': ('string', safe_string_parse_pandas),
        'agenda_nome': ('string', safe_string_parse_pandas),
        'procedimento_nome': ('string', safe_string_parse_pandas),
        'hash': ('string', safe_string_parse_pandas),
        'extraction_timestamp': ('datetime', lambda s, logger: pd.to_datetime(s, format='%Y%m%d_%H%M%S', errors='coerce'))
    },
    'view_micromanipulacao': {
        'codigo_ficha': ('int', safe_numeric_parse_pandas),
        'numero_caso': ('string', safe_string_parse_pandas),
        'prontuario': ('int', safe_numeric_parse_pandas),
        'IdadeEsposa_DG': ('float', safe_numeric_parse_pandas),
        'IdadeMarido_DG': ('string', safe_string_parse_pandas),
        'hash': ('string', safe_string_parse_pandas),
        'extraction_timestamp': ('datetime', lambda s, logger: pd.to_datetime(s, format='%Y%m%d_%H%M%S', errors='coerce'))
    },
    'view_micromanipulacao_oocitos': {
        'id': ('int', safe_numeric_parse_pandas),
        'id_micromanipulacao': ('int', safe_numeric_parse_pandas),
        'diaseguinte': ('string', safe_string_parse_pandas),
        'Maturidade': ('string', safe_string_parse_pandas),
        'hash': ('string', safe_string_parse_pandas),
        'extraction_timestamp': ('datetime', lambda s, logger: pd.to_datetime(s, format='%Y%m%d_%H%M%S', errors='coerce'))
    },
    'view_orcamentos': {
        'id': ('string', safe_string_parse_pandas),
        'prontuario': ('string', safe_string_parse_pandas),
        'paciente': ('string', safe_string_parse_pandas),
        'clinica': ('string', safe_string_parse_pandas),
        'tipo_cotacao': ('string', safe_string_parse_pandas),
        'profissional': ('string', safe_string_parse_pandas),
        'status': ('string', safe_string_parse_pandas),
        'status_entrega': ('string', safe_string_parse_pandas),
        'nome_contato': ('string', safe_string_parse_pandas),
        'telefone_contato': ('string', safe_string_parse_pandas),
        'email_contato': ('string', safe_string_parse_pandas),
        'comentario_para_paciente': ('string', safe_string_parse_pandas),
        'comentario_do_paciente': ('string', safe_string_parse_pandas),
        'orcamento_texto': ('string', safe_string_parse_pandas),
        'descricao': ('string', safe_string_parse_pandas),
        'fornecedor': ('string', safe_string_parse_pandas),
        'qtd_cotada': ('string', safe_string_parse_pandas),
        'unidade': ('string', safe_string_parse_pandas),
        'valor_unidade': ('string', safe_string_parse_pandas),
        'total': ('string', safe_string_parse_pandas),
        'data_entrega': ('date', safe_date_parse_pandas),
        'centro_custos': ('string', safe_string_parse_pandas),
        'valor_total': ('string', safe_string_parse_pandas),
        'forma': ('string', safe_string_parse_pandas),
        'parcelas': ('string', safe_string_parse_pandas),
        'comentarios': ('string', safe_string_parse_pandas),
        'forma_parcela': ('string', safe_string_parse_pandas),
        'valor': ('string', safe_string_parse_pandas),
        'data_pagamento': ('date', safe_date_parse_pandas),
        'descricao_pagamento': ('string', safe_string_parse_pandas),
        'data': ('date', safe_date_parse_pandas),
        'responsavel': ('string', safe_string_parse_pandas),
        'data_entrega_orcamento': ('date', safe_date_parse_pandas),
        'data_ultima_modificacao': ('date', safe_date_parse_pandas),
        'hash': ('string', safe_string_parse_pandas),
        'extraction_timestamp': ('datetime', lambda s, logger: pd.to_datetime(s, format='%Y%m%d_%H%M%S', errors='coerce'))
    },
    'view_procedimentos_financas': {
        'id': ('int', safe_numeric_parse_pandas),
        'procedimento': ('string', safe_string_parse_pandas),
        'valor': ('float', lambda s, logger: safe_numeric_parse_pandas(s.str.replace(',', '.').str.replace('.', '', regex=False), logger)),
        'duracao': ('int', safe_numeric_parse_pandas),
        'hash': ('string', safe_string_parse_pandas),
        'extraction_timestamp': ('datetime', lambda s, logger: pd.to_datetime(s, format='%Y%m%d_%H%M%S', errors='coerce'))
    }
}

def transform_table_pandas(table_name: str, df: pd.DataFrame, quality_logger: DataQualityLogger) -> pd.DataFrame:
    """Transform a table using pandas with data quality logging"""
    logging.getLogger(__name__).info(f'Starting transformation for {table_name} with {len(df)} rows')
    
    if table_name not in table_transformations:
        logging.getLogger(__name__).error(f'No transformation defined for table {table_name}')
        return df
    
    transformations = table_transformations[table_name]
    transformed_df = df.copy()
    
    for column, (dtype, transform_func) in transformations.items():
        if column in df.columns:
            try:
                logging.getLogger(__name__).debug(f'Transforming column {column} to {dtype}')
                transformed_df[column] = transform_func(df[column], quality_logger)
            except Exception as e:
                quality_logger.log_issue(column, 'transformation_error', str(e))
                # Keep original column if transformation fails
                transformed_df[column] = df[column]
        else:
            quality_logger.log_issue(column, 'missing_column', 'Column not found in dataframe')
    
    # Remove duplicates based on hash, keeping the latest extraction_timestamp
    if 'hash' in transformed_df.columns and 'extraction_timestamp' in transformed_df.columns:
        transformed_df = transformed_df.sort_values('extraction_timestamp', ascending=False)
        transformed_df = transformed_df.drop_duplicates(subset=['hash'], keep='first')
        logging.getLogger(__name__).info(f'After deduplication: {len(transformed_df)} rows')
    
    return transformed_df

def process_table(table_name: str, con: duckdb.DuckDBPyConnection) -> DataQualityLogger:
    """Process a single table from bronze to silver using pandas"""
    logger = DataQualityLogger(table_name)
    
    try:
        # Read from bronze
        query = f"SELECT * FROM bronze.{table_name}"
        df = con.execute(query).df()
        
        if df.empty:
            logger.log_issue('general', 'empty_table', 'No data found in bronze table')
            return logger
        
        # Transform using pandas
        transformed_df = transform_table_pandas(table_name, df, logger)
        
        # Create silver schema if it doesn't exist
        con.execute("CREATE SCHEMA IF NOT EXISTS silver")
        
        # Write to silver
        con.execute(f"DROP TABLE IF EXISTS silver.{table_name}")
        con.execute(f"CREATE TABLE silver.{table_name} AS SELECT * FROM transformed_df")
        
        logger.info(f'Successfully processed {table_name}: {len(transformed_df)} rows')
        
    except Exception as e:
        logger.log_issue('general', 'processing_error', str(e))
        logger.log_problematic_file(table_name, str(e))
    
    return logger

def main():
    """Main function to process all tables from bronze to silver"""
    logger.info('Starting pandas-based silver loader')
    logger.info(f'Database path: {db_path}')
    
    all_quality_logs = {}
    
    try:
        with duckdb.connect(db_path) as con:
            # Get list of bronze tables
            bronze_tables = con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'bronze'").df()['table_name'].tolist()
            logger.info(f'Found {len(bronze_tables)} bronze tables: {bronze_tables}')
            
            for table_name in bronze_tables:
                logger.info(f'Processing table: {table_name}')
                quality_logger = process_table(table_name, con)
                all_quality_logs[table_name] = quality_logger.get_summary()
                
                # Log summary for this table
                summary = quality_logger.get_summary()
                logger.info(f'Table {table_name} summary: {summary["total_issues"]} issues, {summary["problematic_files"]} problematic files')
        
        # Write comprehensive quality report
        write_quality_report(all_quality_logs)
        
        logger.info('Pandas-based silver loader completed successfully')
        
    except Exception as e:
        logger.error(f'Error in main processing: {e}', exc_info=True)

def write_quality_report(quality_logs: Dict):
    """Write a comprehensive data quality report"""
    report_path = os.path.join(LOGS_DIR, f'data_quality_report_{timestamp}.txt')
    
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write("=" * 80 + "\n")
        f.write("DATA QUALITY REPORT - PANDAS SILVER LOADER\n")
        f.write("=" * 80 + "\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        total_issues = 0
        total_problematic_files = 0
        
        for table_name, summary in quality_logs.items():
            f.write(f"TABLE: {table_name}\n")
            f.write("-" * 40 + "\n")
            f.write(f"Total Issues: {summary['total_issues']}\n")
            f.write(f"Problematic Files: {summary['problematic_files']}\n")
            
            total_issues += summary['total_issues']
            total_problematic_files += summary['problematic_files']
            
            # Issues by column
            if summary['issues_by_column']:
                f.write("\nIssues by Column:\n")
                for col, issues in summary['issues_by_column'].items():
                    f.write(f"  {col}: {len(issues)} issues\n")
                    for issue in issues[:3]:  # Show first 3 issues per column
                        f.write(f"    - {issue['issue_type']}: {issue['value']} (affects {issue['row_count']} rows)\n")
            
            # Issues by type
            if summary['issues_by_type']:
                f.write("\nIssues by Type:\n")
                for issue_type, issues in summary['issues_by_type'].items():
                    f.write(f"  {issue_type}: {len(issues)} issues\n")
            
            # Problematic files
            if summary['problematic_files_details']:
                f.write("\nProblematic Files:\n")
                for file_path, error in summary['problematic_files_details']:
                    f.write(f"  {file_path}: {error}\n")
            
            f.write("\n" + "=" * 80 + "\n\n")
        
        # Overall summary
        f.write("OVERALL SUMMARY\n")
        f.write("-" * 40 + "\n")
        f.write(f"Total Tables Processed: {len(quality_logs)}\n")
        f.write(f"Total Issues: {total_issues}\n")
        f.write(f"Total Problematic Files: {total_problematic_files}\n")
        f.write(f"Average Issues per Table: {total_issues / len(quality_logs) if quality_logs else 0:.2f}\n")
    
    logger.info(f'Data quality report written to: {report_path}')

if __name__ == '__main__':
    main() 