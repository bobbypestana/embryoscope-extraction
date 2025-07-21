import duckdb
import os
import logging
from datetime import datetime

# Setup logging
LOGS_DIR = os.path.join(os.path.dirname(__file__), 'logs')
os.makedirs(LOGS_DIR, exist_ok=True)
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
LOG_PATH = os.path.join(LOGS_DIR, f'silver_loader_final_{timestamp}.log')
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.FileHandler(LOG_PATH),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

db_path = os.path.join('..', 'database', 'clinisys_all.duckdb') if not os.path.exists('database/clinisys_all.duckdb') else 'database/clinisys_all.duckdb'

def is_valid_date(date_str):
    """Helper function to validate date strings"""
    if not date_str or len(date_str) != 10 or date_str[2] != '/' or date_str[5] != '/':
        return False
    
    try:
        day = int(date_str[:2])
        month = int(date_str[3:5])
        year = int(date_str[6:10])
        
        # Basic validation
        if month < 1 or month > 12:
            return False
        if day < 1 or day > 31:
            return False
        if year < 1900 or year > 2100:
            return False
        
        # Check for invalid dates like February 30th
        if month == 2:
            if year % 4 == 0 and (year % 100 != 0 or year % 400 == 0):  # Leap year
                return day <= 29
            else:
                return day <= 28
        elif month in [4, 6, 9, 11]:  # 30-day months
            return day <= 30
        else:  # 31-day months
            return day <= 31
    except:
        return False

table_casts = {
    'view_medicamentos': '''
        TRY_CAST(id AS INTEGER) AS id,
        CAST(tipo AS VARCHAR) AS tipo,
        CAST(tipo_ficha AS VARCHAR) AS tipo_ficha,
        CAST(medicamento AS VARCHAR) AS medicamento,
        CAST(observacoes AS VARCHAR) AS observacoes,
        hash,
        STRPTIME(extraction_timestamp, '%Y%m%d_%H%M%S') AS extraction_timestamp
    ''',
    'view_medicamentos_prescricoes': '''
        TRY_CAST(id AS INTEGER) AS id,
        TRY_CAST(prontuario AS INTEGER) AS prontuario,
        CAST(ficha_tipo AS VARCHAR) AS ficha_tipo,
        TRY_CAST(ficha_id AS INTEGER) AS ficha_id,
        CASE WHEN data_inicial_valid THEN TRY_CAST(STRPTIME(data_inicial, '%d/%m/%Y') AS DATE) ELSE NULL END AS data_inicial,
        CASE WHEN data_final_valid THEN TRY_CAST(STRPTIME(data_final, '%d/%m/%Y') AS DATE) ELSE NULL END AS data_final,
        CASE WHEN hora_valid THEN TRY_CAST(STRPTIME(hora, '%H:%M') AS TIME) ELSE NULL END AS hora,
        TRY_CAST(medicamento AS INTEGER) AS medicamento,
        CAST(dose AS VARCHAR) AS dose,
        CAST(unidade AS VARCHAR) AS unidade,
        CAST(via AS VARCHAR) AS via,
        TRY_CAST(intervalo AS DOUBLE) AS intervalo,
        CAST(observacoes AS VARCHAR) AS observacoes,
        TRY_CAST(quantidade AS INTEGER) AS quantidade,
        CAST(forma AS VARCHAR) AS forma,
        TRY_CAST(duracao AS INTEGER) AS duracao,
        hash,
        STRPTIME(extraction_timestamp, '%Y%m%d_%H%M%S') AS extraction_timestamp
    ''',
    'view_medicos': '''
        TRY_CAST(id AS INTEGER) AS id,
        CAST(nome AS VARCHAR) AS nome,
        CAST(tipo_medico AS VARCHAR) AS tipo_medico,
        TRY_CAST(registro AS INTEGER) AS registro,
        CAST(especialidade AS VARCHAR) AS especialidade,
        TRY_CAST(rqe AS DOUBLE) AS rqe,
        CAST(rqe_tipo AS VARCHAR) AS rqe_tipo,
        hash,
        STRPTIME(extraction_timestamp, '%Y%m%d_%H%M%S') AS extraction_timestamp
    ''',
    'view_pacientes': '''
        TRY_CAST(codigo AS INTEGER) AS codigo,
        TRY_CAST(prontuario_esposa AS INTEGER) AS prontuario_esposa,
        TRY_CAST(prontuario_marido AS INTEGER) AS prontuario_marido,
        TRY_CAST(prontuario_responsavel1 AS INTEGER) AS prontuario_responsavel1,
        TRY_CAST(prontuario_responsavel2 AS INTEGER) AS prontuario_responsavel2,
        TRY_CAST(prontuario_esposa_pel AS INTEGER) AS prontuario_esposa_pel,
        TRY_CAST(prontuario_marido_pel AS INTEGER) AS prontuario_marido_pel,
        TRY_CAST(prontuario_esposa_pc AS INTEGER) AS prontuario_esposa_pc,
        TRY_CAST(prontuario_marido_pc AS INTEGER) AS prontuario_marido_pc,
        TRY_CAST(prontuario_responsavel1_pc AS INTEGER) AS prontuario_responsavel1_pc,
        TRY_CAST(prontuario_responsavel2_pc AS INTEGER) AS prontuario_responsavel2_pc,
        TRY_CAST(prontuario_esposa_fc AS INTEGER) AS prontuario_esposa_fc,
        TRY_CAST(prontuario_marido_fc AS INTEGER) AS prontuario_marido_fc,
        TRY_CAST(prontuario_esposa_ba AS INTEGER) AS prontuario_esposa_ba,
        TRY_CAST(prontuario_marido_ba AS INTEGER) AS prontuario_marido_ba,
        CAST(esposa_nome AS VARCHAR) AS esposa_nome,
        CAST(marido_nome AS VARCHAR) AS marido_nome,
        TRY_CAST(unidade_origem AS DOUBLE) AS unidade_origem,
        CAST(medico AS VARCHAR) AS medico,
        CAST(medico_encaminhante AS VARCHAR) AS medico_encaminhante,
        CAST(empresa_indicacao AS VARCHAR) AS empresa_indicacao,
        CAST(como_conheceu_huntington_outros AS VARCHAR) AS como_conheceu_huntington_outros,
        CAST(cidade AS VARCHAR) AS cidade,
        CAST(estado AS VARCHAR) AS estado,
        hash,
        STRPTIME(extraction_timestamp, '%Y%m%d_%H%M%S') AS extraction_timestamp
    ''',
    'view_tratamentos': '''
        TRY_CAST(id AS INTEGER) AS id,
        TRY_CAST(prontuario AS INTEGER) AS prontuario,
        TRY_CAST(unidade AS DOUBLE) AS unidade,
        TRY_CAST(idade_esposa AS DOUBLE) AS idade_esposa,
        CAST(idade_marido AS VARCHAR) AS idade_marido,
        hash,
        STRPTIME(extraction_timestamp, '%Y%m%d_%H%M%S') AS extraction_timestamp
    ''',
    'view_unidades': '''
        TRY_CAST(id AS INTEGER) AS id,
        CAST(nome AS VARCHAR) AS nome,
        hash,
        STRPTIME(extraction_timestamp, '%Y%m%d_%H%M%S') AS extraction_timestamp
    ''',
    'view_congelamentos_embrioes': '''
        TRY_CAST(id AS INTEGER) AS id,
        CAST(CodCongelamento AS VARCHAR) AS CodCongelamento,
        TRY_CAST(Unidade AS INTEGER) AS Unidade,
        TRY_CAST(prontuario AS INTEGER) AS prontuario,
        CAST(paciente AS VARCHAR) AS paciente,
        CASE WHEN Data_valid THEN TRY_CAST(STRPTIME(Data, '%d/%m/%Y') AS DATE) ELSE NULL END AS Data,
        CASE WHEN Hora_valid THEN TRY_CAST(STRPTIME(Hora, '%H:%M') AS TIME) ELSE NULL END AS Hora,
        CAST(Ciclo AS VARCHAR) AS Ciclo,
        CAST(CicloRecongelamento AS VARCHAR) AS CicloRecongelamento,
        CAST(condicoes_amostra AS VARCHAR) AS condicoes_amostra,
        CAST(empresa_transporte AS VARCHAR) AS empresa_transporte,
        CAST(clinica_origem AS VARCHAR) AS clinica_origem,
        TRY_CAST(responsavel_recebimento AS DOUBLE) AS responsavel_recebimento,
        CASE WHEN responsavel_recebimento_data_valid THEN TRY_CAST(STRPTIME(responsavel_recebimento_data, '%d/%m/%Y') AS DATE) ELSE NULL END AS responsavel_recebimento_data,
        TRY_CAST(responsavel_armazenamento AS DOUBLE) AS responsavel_armazenamento,
        CASE WHEN responsavel_armazenamento_data_valid THEN TRY_CAST(STRPTIME(responsavel_armazenamento_data, '%d/%m/%Y') AS DATE) ELSE NULL END AS responsavel_armazenamento_data,
        TRY_CAST(NEmbrioes AS INTEGER) AS NEmbrioes,
        CAST(NPailletes AS VARCHAR) AS NPailletes,
        CAST(Identificacao AS VARCHAR) AS Identificacao,
        CAST(Tambor AS VARCHAR) AS Tambor,
        CAST(Cane AS VARCHAR) AS Cane,
        CAST(Cane2 AS VARCHAR) AS Cane2,
        CAST(Tecnica AS VARCHAR) AS Tecnica,
        TRY_CAST(Ovulo AS DOUBLE) AS Ovulo,
        TRY_CAST(D2 AS DOUBLE) AS D2,
        TRY_CAST(D3 AS DOUBLE) AS D3,
        TRY_CAST(D4 AS DOUBLE) AS D4,
        TRY_CAST(D5 AS DOUBLE) AS D5,
        TRY_CAST(D6 AS DOUBLE) AS D6,
        TRY_CAST(D7 AS DOUBLE) AS D7,
        CAST(rack AS VARCHAR) AS rack,
        TRY_CAST(rack2 AS DOUBLE) AS rack2,
        TRY_CAST(rack3 AS DOUBLE) AS rack3,
        TRY_CAST(rack4 AS DOUBLE) AS rack4,
        CAST(Observacoes AS VARCHAR) AS Observacoes,
        TRY_CAST(BiologoResponsavel AS DOUBLE) AS BiologoResponsavel,
        CAST(BiologoFIV AS VARCHAR) AS BiologoFIV,
        CAST(BiologoFIV2 AS VARCHAR) AS BiologoFIV2,
        CAST(status_financeiro AS VARCHAR) AS status_financeiro,
        TRY_CAST(responsavel_congelamento_d5 AS DOUBLE) AS responsavel_congelamento_d5,
        TRY_CAST(responsavel_checagem_d5 AS DOUBLE) AS responsavel_checagem_d5,
        TRY_CAST(responsavel_congelamento_d6 AS DOUBLE) AS responsavel_congelamento_d6,
        TRY_CAST(responsavel_checagem_d6 AS DOUBLE) AS responsavel_checagem_d6,
        TRY_CAST(responsavel_congelamento_d7 AS DOUBLE) AS responsavel_congelamento_d7,
        TRY_CAST(responsavel_checagem_d7 AS DOUBLE) AS responsavel_checagem_d7,
        hash,
        STRPTIME(extraction_timestamp, '%Y%m%d_%H%M%S') AS extraction_timestamp
    ''',
    'view_congelamentos_ovulos': '''
        TRY_CAST(id AS INTEGER) AS id,
        CAST(CodCongelamento AS VARCHAR) AS CodCongelamento,
        TRY_CAST(Unidade AS INTEGER) AS Unidade,
        TRY_CAST(prontuario AS INTEGER) AS prontuario,
        CAST(paciente AS VARCHAR) AS paciente,
        CASE WHEN Data_valid THEN TRY_CAST(STRPTIME(Data, '%d/%m/%Y') AS DATE) ELSE NULL END AS Data,
        CASE WHEN Hora_valid THEN TRY_CAST(STRPTIME(Hora, '%H:%M') AS TIME) ELSE NULL END AS Hora,
        CAST(Ciclo AS VARCHAR) AS Ciclo,
        CAST(condicoes_amostra AS VARCHAR) AS condicoes_amostra,
        CAST(empresa_transporte AS VARCHAR) AS empresa_transporte,
        CAST(clinica_origem AS VARCHAR) AS clinica_origem,
        TRY_CAST(responsavel_recebimento AS DOUBLE) AS responsavel_recebimento,
        CASE WHEN responsavel_recebimento_data_valid THEN TRY_CAST(STRPTIME(responsavel_recebimento_data, '%d/%m/%Y') AS DATE) ELSE NULL END AS responsavel_recebimento_data,
        TRY_CAST(responsavel_armazenamento AS DOUBLE) AS responsavel_armazenamento,
        CASE WHEN responsavel_armazenamento_data_valid THEN TRY_CAST(STRPTIME(responsavel_armazenamento_data, '%d/%m/%Y') AS DATE) ELSE NULL END AS responsavel_armazenamento_data,
        TRY_CAST(NOvulos AS INTEGER) AS NOvulos,
        CAST(NPailletes AS VARCHAR) AS NPailletes,
        CAST(Identificacao AS VARCHAR) AS Identificacao,
        CAST(Tambor AS VARCHAR) AS Tambor,
        CAST(Cane AS VARCHAR) AS Cane,
        CAST(Cane2 AS VARCHAR) AS Cane2,
        CAST(Tecnica AS VARCHAR) AS Tecnica,
        CAST(Motivo AS VARCHAR) AS Motivo,
        CAST(Observacoes AS VARCHAR) AS Observacoes,
        TRY_CAST(BiologoResponsavel AS DOUBLE) AS BiologoResponsavel,
        CAST(BiologoFIV AS VARCHAR) AS BiologoFIV,
        CAST(BiologoFIV2 AS VARCHAR) AS BiologoFIV2,
        CAST(status_financeiro AS VARCHAR) AS status_financeiro,
        hash,
        STRPTIME(extraction_timestamp, '%Y%m%d_%H%M%S') AS extraction_timestamp
    ''',
    'view_descongelamentos_embrioes': '''
        TRY_CAST(id AS INTEGER) AS id,
        CAST(CodDescongelamento AS VARCHAR) AS CodDescongelamento,
        TRY_CAST(Unidade AS DOUBLE) AS Unidade,
        TRY_CAST(prontuario AS INTEGER) AS prontuario,
        TRY_CAST(doadora AS INTEGER) AS doadora,
        CASE WHEN DataCongelamento_valid THEN TRY_CAST(STRPTIME(DataCongelamento, '%d/%m/%Y') AS DATE) ELSE NULL END AS DataCongelamento,
        CASE WHEN DataDescongelamento_valid THEN TRY_CAST(STRPTIME(DataDescongelamento, '%d/%m/%Y') AS DATE) ELSE NULL END AS DataDescongelamento,
        CAST(Ciclo AS VARCHAR) AS Ciclo,
        CAST(Identificacao AS VARCHAR) AS Identificacao,
        CAST(CodCongelamento AS VARCHAR) AS CodCongelamento,
        CAST(Tambor AS VARCHAR) AS Tambor,
        CAST(Cane AS VARCHAR) AS Cane,
        CAST(PailletesDescongeladas AS VARCHAR) AS PailletesDescongeladas,
        CAST(Tecnica AS VARCHAR) AS Tecnica,
        TRY_CAST(Transferencia AS DOUBLE) AS Transferencia,
        CASE WHEN DataTransferencia_valid THEN TRY_CAST(STRPTIME(DataTransferencia, '%d/%m/%Y') AS DATE) ELSE NULL END AS DataTransferencia,
        TRY_CAST(Prateleira AS DOUBLE) AS Prateleira,
        CAST(Incubadora AS VARCHAR) AS Incubadora,
        CAST(transferidos_transferencia AS VARCHAR) AS transferidos_transferencia,
        CAST(cateter_transferencia AS VARCHAR) AS cateter_transferencia,
        CAST(lote_transferencia AS VARCHAR) AS lote_transferencia,
        CAST(validade_transferencia AS VARCHAR) AS validade_transferencia,
        CAST(intercorrencia_transferencia AS VARCHAR) AS intercorrencia_transferencia,
        CAST(sangue_interno_transferencia AS VARCHAR) AS sangue_interno_transferencia,
        CAST(sangue_externo_transferencia AS VARCHAR) AS sangue_externo_transferencia,
        CAST(retorno_transferencia AS VARCHAR) AS retorno_transferencia,
        CAST(vezes_retorno_transferencia AS VARCHAR) AS vezes_retorno_transferencia,
        CAST(Transfer_D5 AS VARCHAR) AS Transfer_D5,
        TRY_CAST(responsavel_transferencia AS DOUBLE) AS responsavel_transferencia,
        CAST(Observacoes AS VARCHAR) AS Observacoes,
        CAST(BiologoFIV AS VARCHAR) AS BiologoFIV,
        CAST(BiologoFIV2 AS VARCHAR) AS BiologoFIV2,
        hash,
        STRPTIME(extraction_timestamp, '%Y%m%d_%H%M%S') AS extraction_timestamp
    ''',
    'view_descongelamentos_ovulos': '''
        TRY_CAST(id AS INTEGER) AS id,
        CAST(CodDescongelamento AS VARCHAR) AS CodDescongelamento,
        TRY_CAST(Unidade AS DOUBLE) AS Unidade,
        TRY_CAST(prontuario AS INTEGER) AS prontuario,
        TRY_CAST(doadora AS INTEGER) AS doadora,
        CASE WHEN DataCongelamento_valid THEN TRY_CAST(STRPTIME(DataCongelamento, '%d/%m/%Y') AS DATE) ELSE NULL END AS DataCongelamento,
        CASE WHEN DataDescongelamento_valid THEN TRY_CAST(STRPTIME(DataDescongelamento, '%d/%m/%Y') AS DATE) ELSE NULL END AS DataDescongelamento,
        CAST(Ciclo AS VARCHAR) AS Ciclo,
        CAST(Identificacao AS VARCHAR) AS Identificacao,
        CAST(CodCongelamento AS VARCHAR) AS CodCongelamento,
        CAST(Tambor AS VARCHAR) AS Tambor,
        CAST(Cane AS VARCHAR) AS Cane,
        CAST(PailletesDescongeladas AS VARCHAR) AS PailletesDescongeladas,
        CAST(Tecnica AS VARCHAR) AS Tecnica,
        CAST(Observacoes AS VARCHAR) AS Observacoes,
        CAST(BiologoFIV AS VARCHAR) AS BiologoFIV,
        CAST(BiologoFIV2 AS VARCHAR) AS BiologoFIV2,
        hash,
        STRPTIME(extraction_timestamp, '%Y%m%d_%H%M%S') AS extraction_timestamp
    ''',
    'view_embrioes_congelados': '''
        TRY_CAST(id AS INTEGER) AS id,
        TRY_CAST(id_oocito AS INTEGER) AS id_oocito,
        TRY_CAST(id_congelamento AS INTEGER) AS id_congelamento,
        TRY_CAST(id_descongelamento AS INTEGER) AS id_descongelamento,
        TRY_CAST(prontuario AS INTEGER) AS prontuario,
        hash,
        STRPTIME(extraction_timestamp, '%Y%m%d_%H%M%S') AS extraction_timestamp
    ''',
    'view_extrato_atendimentos_central': '''
        TRY_CAST(agendamento_id AS INTEGER) AS agendamento_id,
        CASE WHEN data_valid THEN TRY_CAST(STRPTIME(data, '%d/%m/%Y') AS DATE) ELSE NULL END AS data,
        CAST(inicio AS VARCHAR) AS inicio,
        CASE WHEN data_agendamento_original_valid THEN TRY_CAST(STRPTIME(data_agendamento_original, '%d/%m/%Y') AS DATE) ELSE NULL END AS data_agendamento_original,
        TRY_CAST(medico AS DOUBLE) AS medico,
        TRY_CAST(medico2 AS DOUBLE) AS medico2,
        TRY_CAST(prontuario AS BIGINT) AS prontuario,
        TRY_CAST(evento AS BIGINT) AS evento,
        CAST(evento2 AS VARCHAR) AS evento2,
        TRY_CAST(centro_custos AS INTEGER) AS centro_custos,
        TRY_CAST(agenda AS INTEGER) AS agenda,
        CAST(chegou AS VARCHAR) AS chegou,
        TRY_CAST(confirmado AS INTEGER) AS confirmado,
        TRY_CAST(paciente_codigo AS DOUBLE) AS paciente_codigo,
        CAST(paciente_nome AS VARCHAR) AS paciente_nome,
        CAST(medico_nome AS VARCHAR) AS medico_nome,
        CAST(medico_sobrenome AS VARCHAR) AS medico_sobrenome,
        CAST(medico2_nome AS VARCHAR) AS medico2_nome,
        CAST(centro_custos_nome AS VARCHAR) AS centro_custos_nome,
        CAST(agenda_nome AS VARCHAR) AS agenda_nome,
        CAST(procedimento_nome AS VARCHAR) AS procedimento_nome,
        hash,
        STRPTIME(extraction_timestamp, '%Y%m%d_%H%M%S') AS extraction_timestamp
    ''',
    'view_micromanipulacao': '''
        TRY_CAST(codigo_ficha AS INTEGER) AS codigo_ficha,
        CAST(numero_caso AS VARCHAR) AS numero_caso,
        TRY_CAST(prontuario AS INTEGER) AS prontuario,
        TRY_CAST(IdadeEsposa_DG AS DOUBLE) AS IdadeEsposa_DG,
        CAST(IdadeMarido_DG AS VARCHAR) AS IdadeMarido_DG,
        hash,
        STRPTIME(extraction_timestamp, '%Y%m%d_%H%M%S') AS extraction_timestamp
    ''',
    'view_micromanipulacao_oocitos': '''
        TRY_CAST(id AS INTEGER) AS id,
        TRY_CAST(id_micromanipulacao AS INTEGER) AS id_micromanipulacao,
        CAST(diaseguinte AS VARCHAR) AS diaseguinte,
        CAST(Maturidade AS VARCHAR) AS Maturidade,
        hash,
        STRPTIME(extraction_timestamp, '%Y%m%d_%H%M%S') AS extraction_timestamp
    ''',
    'view_orcamentos': '''
        CAST(id AS VARCHAR) AS id,
        CAST(prontuario AS VARCHAR) AS prontuario,
        CAST(paciente AS VARCHAR) AS paciente,
        CAST(clinica AS VARCHAR) AS clinica,
        CAST(tipo_cotacao AS VARCHAR) AS tipo_cotacao,
        CAST(profissional AS VARCHAR) AS profissional,
        CAST(status AS VARCHAR) AS status,
        CAST(status_entrega AS VARCHAR) AS status_entrega,
        CAST(nome_contato AS VARCHAR) AS nome_contato,
        CAST(telefone_contato AS VARCHAR) AS telefone_contato,
        CAST(email_contato AS VARCHAR) AS email_contato,
        CAST(comentario_para_paciente AS VARCHAR) AS comentario_para_paciente,
        CAST(comentario_do_paciente AS VARCHAR) AS comentario_do_paciente,
        CAST(orcamento_texto AS VARCHAR) AS orcamento_texto,
        CAST(descricao AS VARCHAR) AS descricao,
        CAST(fornecedor AS VARCHAR) AS fornecedor,
        CAST(qtd_cotada AS VARCHAR) AS qtd_cotada,
        CAST(unidade AS VARCHAR) AS unidade,
        CAST(valor_unidade AS VARCHAR) AS valor_unidade,
        CAST(total AS VARCHAR) AS total,
        CASE WHEN data_entrega_valid THEN TRY_CAST(STRPTIME(data_entrega, '%d/%m/%Y') AS DATE) ELSE NULL END AS data_entrega,
        CAST(centro_custos AS VARCHAR) AS centro_custos,
        CAST(valor_total AS VARCHAR) AS valor_total,
        CAST(forma AS VARCHAR) AS forma,
        CAST(parcelas AS VARCHAR) AS parcelas,
        CAST(comentarios AS VARCHAR) AS comentarios,
        CAST(forma_parcela AS VARCHAR) AS forma_parcela,
        CAST(valor AS VARCHAR) AS valor,
        CASE WHEN data_pagamento_valid THEN TRY_CAST(STRPTIME(data_pagamento, '%d/%m/%Y') AS DATE) ELSE NULL END AS data_pagamento,
        CAST(descricao_pagamento AS VARCHAR) AS descricao_pagamento,
        CASE WHEN data_valid THEN TRY_CAST(STRPTIME(data, '%d/%m/%Y') AS DATE) ELSE NULL END AS data,
        CAST(responsavel AS VARCHAR) AS responsavel,
        CASE WHEN data_entrega_orcamento_valid THEN TRY_CAST(STRPTIME(data_entrega_orcamento, '%d/%m/%Y') AS DATE) ELSE NULL END AS data_entrega_orcamento,
        CASE WHEN data_ultima_modificacao_valid THEN TRY_CAST(STRPTIME(data_ultima_modificacao, '%d/%m/%Y') AS DATE) ELSE NULL END AS data_ultima_modificacao,
        hash,
        STRPTIME(extraction_timestamp, '%Y%m%d_%H%M%S') AS extraction_timestamp
    ''',
    'view_procedimentos_financas': '''
        TRY_CAST(id AS INTEGER) AS id,
        CAST(procedimento AS VARCHAR) AS procedimento,
        CASE WHEN REGEXP_MATCHES(REPLACE(REPLACE(valor, '.', ''), ',', '.'), '^[0-9]+(\\.[0-9]+)?$') THEN TRY_CAST(REPLACE(REPLACE(valor, '.', ''), ',', '.') AS DOUBLE) ELSE NULL END AS valor,
        TRY_CAST(duracao AS INTEGER) AS duracao,
        hash,
        STRPTIME(extraction_timestamp, '%Y%m%d_%H%M%S') AS extraction_timestamp
    ''',
}

def deduplicate_and_format_sql(table, cast_sql):
    tables_with_dates = [
        'view_medicamentos_prescricoes',
        'view_congelamentos_embrioes',
        'view_congelamentos_ovulos',
        'view_descongelamentos_embrioes',
        'view_descongelamentos_ovulos',
        'view_extrato_atendimentos_central',
        'view_orcamentos'
    ]
    date_columns = {
        'view_medicamentos_prescricoes': ['data_inicial', 'data_final', 'hora'],
        'view_congelamentos_embrioes': ['Data', 'Hora', 'responsavel_recebimento_data', 'responsavel_armazenamento_data'],
        'view_congelamentos_ovulos': ['Data', 'Hora', 'responsavel_recebimento_data', 'responsavel_armazenamento_data'],
        'view_descongelamentos_embrioes': ['DataCongelamento', 'DataDescongelamento', 'DataTransferencia'],
        'view_descongelamentos_ovulos': ['DataCongelamento', 'DataDescongelamento'],
        'view_extrato_atendimentos_central': ['data', 'data_agendamento_original'],
        'view_orcamentos': ['data_entrega', 'data_pagamento', 'data', 'data_entrega_orcamento', 'data_ultima_modificacao'],
    }
    if table in tables_with_dates:
        cte_cols = []
        for col in date_columns.get(table, []):
            if 'hora' in col.lower():
                cte_cols.append(f"REGEXP_MATCHES({col}, '^[0-9]{{2}}:[0-9]{{2}}$') AS {col}_valid")
            else:
                cte_cols.append(f"REGEXP_MATCHES({col}, '^[0-9]{{2}}/[0-9]{{2}}/[0-9]{{4}}$') AS {col}_valid")
        cte_cols_str = ',\n                '.join(cte_cols)
        return f'''
        CREATE SCHEMA IF NOT EXISTS silver;
        CREATE OR REPLACE TABLE silver.{table} AS
        WITH cleaned AS (
            SELECT *,
                {cte_cols_str}
            FROM bronze.{table}
        )
        SELECT {cast_sql}
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY hash ORDER BY extraction_timestamp DESC) AS rn
            FROM cleaned
        )
        WHERE rn = 1;
        '''
    else:
        return f'''
        CREATE SCHEMA IF NOT EXISTS silver;
        CREATE OR REPLACE TABLE silver.{table} AS
        SELECT {cast_sql}
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY hash ORDER BY extraction_timestamp DESC) AS rn
            FROM bronze.{table}
        )
        WHERE rn = 1;
        '''

def main():
    logger.info('Starting final silver loader')
    with duckdb.connect(db_path) as con:
        for table, cast_sql in table_casts.items():
            logger.info(f'Processing {table}')
            try:
                sql = deduplicate_and_format_sql(table, cast_sql)
                logger.debug(f'Generated SQL for {table}: {sql}')
                con.execute(sql)
                logger.info(f'Created/updated silver.{table}')
            except Exception as e:
                logger.error(f'Error processing {table}: {e}', exc_info=True)
    logger.info('Final silver loader finished successfully')

if __name__ == '__main__':
    main() 