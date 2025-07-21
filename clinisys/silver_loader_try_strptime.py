import duckdb
import os
import logging
from datetime import datetime

# Setup logging
LOGS_DIR = os.path.join(os.path.dirname(__file__), 'logs')
os.makedirs(LOGS_DIR, exist_ok=True)
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
LOG_PATH = os.path.join(LOGS_DIR, f'silver_loader_try_strptime_{timestamp}.log')
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

def safe_date_parse_try_strptime(date_col):
    """Generate SQL for safe date parsing using try_strptime()"""
    return f"try_strptime({date_col}, '%d/%m/%Y')"

def safe_time_parse_try_strptime(time_col):
    """Generate SQL for safe time parsing using try_strptime()"""
    return f"try_strptime({time_col}, '%H:%M')"

def safe_numeric_parse_try_cast(col, target_type):
    """Generate SQL for safe numeric parsing using try_cast()"""
    return f"try_cast({col} AS {target_type})"

def safe_string_parse(col):
    """Generate SQL for safe string parsing"""
    return f"CAST({col} AS VARCHAR)"

# Define table transformations using try_strptime and try_cast
table_casts = {
    'view_medicamentos': '''
        try_cast(id AS INTEGER) AS id,
        CAST(tipo AS VARCHAR) AS tipo,
        CAST(tipo_ficha AS VARCHAR) AS tipo_ficha,
        CAST(medicamento AS VARCHAR) AS medicamento,
        CAST(observacoes AS VARCHAR) AS observacoes,
        hash,
        try_strptime(extraction_timestamp, '%Y%m%d_%H%M%S') AS extraction_timestamp
    ''',
    'view_medicamentos_prescricoes': f'''
        try_cast(id AS INTEGER) AS id,
        try_cast(prontuario AS INTEGER) AS prontuario,
        CAST(ficha_tipo AS VARCHAR) AS ficha_tipo,
        try_cast(ficha_id AS INTEGER) AS ficha_id,
        {safe_date_parse_try_strptime('data_inicial')} AS data_inicial,
        {safe_date_parse_try_strptime('data_final')} AS data_final,
        {safe_time_parse_try_strptime('hora')} AS hora,
        try_cast(medicamento AS INTEGER) AS medicamento,
        CAST(dose AS VARCHAR) AS dose,
        CAST(unidade AS VARCHAR) AS unidade,
        CAST(via AS VARCHAR) AS via,
        try_cast(intervalo AS DOUBLE) AS intervalo,
        CAST(observacoes AS VARCHAR) AS observacoes,
        try_cast(quantidade AS INTEGER) AS quantidade,
        CAST(forma AS VARCHAR) AS forma,
        try_cast(duracao AS INTEGER) AS duracao,
        hash,
        try_strptime(extraction_timestamp, '%Y%m%d_%H%M%S') AS extraction_timestamp
    ''',
    'view_medicos': '''
        try_cast(id AS INTEGER) AS id,
        CAST(nome AS VARCHAR) AS nome,
        CAST(tipo_medico AS VARCHAR) AS tipo_medico,
        try_cast(registro AS INTEGER) AS registro,
        CAST(especialidade AS VARCHAR) AS especialidade,
        try_cast(rqe AS DOUBLE) AS rqe,
        CAST(rqe_tipo AS VARCHAR) AS rqe_tipo,
        hash,
        try_strptime(extraction_timestamp, '%Y%m%d_%H%M%S') AS extraction_timestamp
    ''',
    'view_pacientes': '''
        try_cast(codigo AS INTEGER) AS codigo,
        try_cast(prontuario_esposa AS INTEGER) AS prontuario_esposa,
        try_cast(prontuario_marido AS INTEGER) AS prontuario_marido,
        try_cast(prontuario_responsavel1 AS INTEGER) AS prontuario_responsavel1,
        try_cast(prontuario_responsavel2 AS INTEGER) AS prontuario_responsavel2,
        try_cast(prontuario_esposa_pel AS INTEGER) AS prontuario_esposa_pel,
        try_cast(prontuario_marido_pel AS INTEGER) AS prontuario_marido_pel,
        try_cast(prontuario_esposa_pc AS INTEGER) AS prontuario_esposa_pc,
        try_cast(prontuario_marido_pc AS INTEGER) AS prontuario_marido_pc,
        try_cast(prontuario_responsavel1_pc AS INTEGER) AS prontuario_responsavel1_pc,
        try_cast(prontuario_responsavel2_pc AS INTEGER) AS prontuario_responsavel2_pc,
        try_cast(prontuario_esposa_fc AS INTEGER) AS prontuario_esposa_fc,
        try_cast(prontuario_marido_fc AS INTEGER) AS prontuario_marido_fc,
        try_cast(prontuario_esposa_ba AS INTEGER) AS prontuario_esposa_ba,
        try_cast(prontuario_marido_ba AS INTEGER) AS prontuario_marido_ba,
        CAST(esposa_nome AS VARCHAR) AS esposa_nome,
        CAST(marido_nome AS VARCHAR) AS marido_nome,
        try_cast(unidade_origem AS DOUBLE) AS unidade_origem,
        CAST(medico AS VARCHAR) AS medico,
        CAST(medico_encaminhante AS VARCHAR) AS medico_encaminhante,
        CAST(empresa_indicacao AS VARCHAR) AS empresa_indicacao,
        CAST(como_conheceu_huntington_outros AS VARCHAR) AS como_conheceu_huntington_outros,
        CAST(cidade AS VARCHAR) AS cidade,
        CAST(estado AS VARCHAR) AS estado,
        hash,
        try_strptime(extraction_timestamp, '%Y%m%d_%H%M%S') AS extraction_timestamp
    ''',
    'view_tratamentos': '''
        try_cast(id AS INTEGER) AS id,
        try_cast(prontuario AS INTEGER) AS prontuario,
        try_cast(unidade AS DOUBLE) AS unidade,
        try_cast(idade_esposa AS DOUBLE) AS idade_esposa,
        CAST(idade_marido AS VARCHAR) AS idade_marido,
        hash,
        try_strptime(extraction_timestamp, '%Y%m%d_%H%M%S') AS extraction_timestamp
    ''',
    'view_unidades': '''
        try_cast(id AS INTEGER) AS id,
        CAST(nome AS VARCHAR) AS nome,
        hash,
        try_strptime(extraction_timestamp, '%Y%m%d_%H%M%S') AS extraction_timestamp
    ''',
    'view_congelamentos_embrioes': f'''
        try_cast(id AS INTEGER) AS id,
        CAST(CodCongelamento AS VARCHAR) AS CodCongelamento,
        try_cast(Unidade AS INTEGER) AS Unidade,
        try_cast(prontuario AS INTEGER) AS prontuario,
        CAST(paciente AS VARCHAR) AS paciente,
        {safe_date_parse_try_strptime('Data')} AS Data,
        {safe_time_parse_try_strptime('Hora')} AS Hora,
        CAST(Ciclo AS VARCHAR) AS Ciclo,
        CAST(CicloRecongelamento AS VARCHAR) AS CicloRecongelamento,
        CAST(condicoes_amostra AS VARCHAR) AS condicoes_amostra,
        CAST(empresa_transporte AS VARCHAR) AS empresa_transporte,
        CAST(clinica_origem AS VARCHAR) AS clinica_origem,
        try_cast(responsavel_recebimento AS DOUBLE) AS responsavel_recebimento,
        {safe_date_parse_try_strptime('responsavel_recebimento_data')} AS responsavel_recebimento_data,
        try_cast(responsavel_armazenamento AS DOUBLE) AS responsavel_armazenamento,
        {safe_date_parse_try_strptime('responsavel_armazenamento_data')} AS responsavel_armazenamento_data,
        try_cast(NEmbrioes AS INTEGER) AS NEmbrioes,
        CAST(NPailletes AS VARCHAR) AS NPailletes,
        CAST(Identificacao AS VARCHAR) AS Identificacao,
        CAST(Tambor AS VARCHAR) AS Tambor,
        CAST(Cane AS VARCHAR) AS Cane,
        CAST(Cane2 AS VARCHAR) AS Cane2,
        CAST(Tecnica AS VARCHAR) AS Tecnica,
        try_cast(Ovulo AS DOUBLE) AS Ovulo,
        try_cast(D2 AS DOUBLE) AS D2,
        try_cast(D3 AS DOUBLE) AS D3,
        try_cast(D4 AS DOUBLE) AS D4,
        try_cast(D5 AS DOUBLE) AS D5,
        try_cast(D6 AS DOUBLE) AS D6,
        try_cast(D7 AS DOUBLE) AS D7,
        CAST(rack AS VARCHAR) AS rack,
        try_cast(rack2 AS DOUBLE) AS rack2,
        try_cast(rack3 AS DOUBLE) AS rack3,
        try_cast(rack4 AS DOUBLE) AS rack4,
        CAST(Observacoes AS VARCHAR) AS Observacoes,
        try_cast(BiologoResponsavel AS DOUBLE) AS BiologoResponsavel,
        CAST(BiologoFIV AS VARCHAR) AS BiologoFIV,
        CAST(BiologoFIV2 AS VARCHAR) AS BiologoFIV2,
        CAST(status_financeiro AS VARCHAR) AS status_financeiro,
        try_cast(responsavel_congelamento_d5 AS DOUBLE) AS responsavel_congelamento_d5,
        try_cast(responsavel_checagem_d5 AS DOUBLE) AS responsavel_checagem_d5,
        try_cast(responsavel_congelamento_d6 AS DOUBLE) AS responsavel_congelamento_d6,
        try_cast(responsavel_checagem_d6 AS DOUBLE) AS responsavel_checagem_d6,
        try_cast(responsavel_congelamento_d7 AS DOUBLE) AS responsavel_congelamento_d7,
        try_cast(responsavel_checagem_d7 AS DOUBLE) AS responsavel_checagem_d7,
        hash,
        try_strptime(extraction_timestamp, '%Y%m%d_%H%M%S') AS extraction_timestamp
    ''',
    'view_congelamentos_ovulos': f'''
        try_cast(id AS INTEGER) AS id,
        CAST(CodCongelamento AS VARCHAR) AS CodCongelamento,
        try_cast(Unidade AS INTEGER) AS Unidade,
        try_cast(prontuario AS INTEGER) AS prontuario,
        CAST(paciente AS VARCHAR) AS paciente,
        {safe_date_parse_try_strptime('Data')} AS Data,
        {safe_time_parse_try_strptime('Hora')} AS Hora,
        CAST(Ciclo AS VARCHAR) AS Ciclo,
        CAST(condicoes_amostra AS VARCHAR) AS condicoes_amostra,
        CAST(empresa_transporte AS VARCHAR) AS empresa_transporte,
        CAST(clinica_origem AS VARCHAR) AS clinica_origem,
        try_cast(responsavel_recebimento AS DOUBLE) AS responsavel_recebimento,
        {safe_date_parse_try_strptime('responsavel_recebimento_data')} AS responsavel_recebimento_data,
        try_cast(responsavel_armazenamento AS DOUBLE) AS responsavel_armazenamento,
        {safe_date_parse_try_strptime('responsavel_armazenamento_data')} AS responsavel_armazenamento_data,
        try_cast(NOvulos AS INTEGER) AS NOvulos,
        CAST(NPailletes AS VARCHAR) AS NPailletes,
        CAST(Identificacao AS VARCHAR) AS Identificacao,
        CAST(Tambor AS VARCHAR) AS Tambor,
        CAST(Cane AS VARCHAR) AS Cane,
        CAST(Cane2 AS VARCHAR) AS Cane2,
        CAST(Tecnica AS VARCHAR) AS Tecnica,
        CAST(Motivo AS VARCHAR) AS Motivo,
        CAST(Observacoes AS VARCHAR) AS Observacoes,
        try_cast(BiologoResponsavel AS DOUBLE) AS BiologoResponsavel,
        CAST(BiologoFIV AS VARCHAR) AS BiologoFIV,
        CAST(BiologoFIV2 AS VARCHAR) AS BiologoFIV2,
        CAST(status_financeiro AS VARCHAR) AS status_financeiro,
        hash,
        try_strptime(extraction_timestamp, '%Y%m%d_%H%M%S') AS extraction_timestamp
    ''',
    'view_descongelamentos_embrioes': f'''
        try_cast(id AS INTEGER) AS id,
        CAST(CodDescongelamento AS VARCHAR) AS CodDescongelamento,
        try_cast(Unidade AS DOUBLE) AS Unidade,
        try_cast(prontuario AS INTEGER) AS prontuario,
        try_cast(doadora AS INTEGER) AS doadora,
        {safe_date_parse_try_strptime('DataCongelamento')} AS DataCongelamento,
        {safe_date_parse_try_strptime('DataDescongelamento')} AS DataDescongelamento,
        CAST(Ciclo AS VARCHAR) AS Ciclo,
        CAST(Identificacao AS VARCHAR) AS Identificacao,
        CAST(CodCongelamento AS VARCHAR) AS CodCongelamento,
        CAST(Tambor AS VARCHAR) AS Tambor,
        CAST(Cane AS VARCHAR) AS Cane,
        CAST(PailletesDescongeladas AS VARCHAR) AS PailletesDescongeladas,
        CAST(Tecnica AS VARCHAR) AS Tecnica,
        try_cast(Transferencia AS DOUBLE) AS Transferencia,
        {safe_date_parse_try_strptime('DataTransferencia')} AS DataTransferencia,
        try_cast(Prateleira AS DOUBLE) AS Prateleira,
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
        try_cast(responsavel_transferencia AS DOUBLE) AS responsavel_transferencia,
        CAST(Observacoes AS VARCHAR) AS Observacoes,
        CAST(BiologoFIV AS VARCHAR) AS BiologoFIV,
        CAST(BiologoFIV2 AS VARCHAR) AS BiologoFIV2,
        hash,
        try_strptime(extraction_timestamp, '%Y%m%d_%H%M%S') AS extraction_timestamp
    ''',
    'view_descongelamentos_ovulos': f'''
        try_cast(id AS INTEGER) AS id,
        CAST(CodDescongelamento AS VARCHAR) AS CodDescongelamento,
        try_cast(Unidade AS DOUBLE) AS Unidade,
        try_cast(prontuario AS INTEGER) AS prontuario,
        try_cast(doadora AS INTEGER) AS doadora,
        {safe_date_parse_try_strptime('DataCongelamento')} AS DataCongelamento,
        {safe_date_parse_try_strptime('DataDescongelamento')} AS DataDescongelamento,
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
        try_strptime(extraction_timestamp, '%Y%m%d_%H%M%S') AS extraction_timestamp
    ''',
    'view_embrioes_congelados': '''
        try_cast(id AS INTEGER) AS id,
        try_cast(id_oocito AS INTEGER) AS id_oocito,
        try_cast(id_congelamento AS INTEGER) AS id_congelamento,
        try_cast(id_descongelamento AS INTEGER) AS id_descongelamento,
        try_cast(prontuario AS INTEGER) AS prontuario,
        hash,
        try_strptime(extraction_timestamp, '%Y%m%d_%H%M%S') AS extraction_timestamp
    ''',
    'view_extrato_atendimentos_central': f'''
        try_cast(agendamento_id AS INTEGER) AS agendamento_id,
        {safe_date_parse_try_strptime('data')} AS data,
        CAST(inicio AS VARCHAR) AS inicio,
        {safe_date_parse_try_strptime('data_agendamento_original')} AS data_agendamento_original,
        try_cast(medico AS DOUBLE) AS medico,
        try_cast(medico2 AS DOUBLE) AS medico2,
        try_cast(prontuario AS BIGINT) AS prontuario,
        try_cast(evento AS BIGINT) AS evento,
        CAST(evento2 AS VARCHAR) AS evento2,
        try_cast(centro_custos AS INTEGER) AS centro_custos,
        try_cast(agenda AS INTEGER) AS agenda,
        CAST(chegou AS VARCHAR) AS chegou,
        try_cast(confirmado AS INTEGER) AS confirmado,
        try_cast(paciente_codigo AS DOUBLE) AS paciente_codigo,
        CAST(paciente_nome AS VARCHAR) AS paciente_nome,
        CAST(medico_nome AS VARCHAR) AS medico_nome,
        CAST(medico_sobrenome AS VARCHAR) AS medico_sobrenome,
        CAST(medico2_nome AS VARCHAR) AS medico2_nome,
        CAST(centro_custos_nome AS VARCHAR) AS centro_custos_nome,
        CAST(agenda_nome AS VARCHAR) AS agenda_nome,
        CAST(procedimento_nome AS VARCHAR) AS procedimento_nome,
        hash,
        try_strptime(extraction_timestamp, '%Y%m%d_%H%M%S') AS extraction_timestamp
    ''',
    'view_micromanipulacao': '''
        try_cast(codigo_ficha AS INTEGER) AS codigo_ficha,
        CAST(numero_caso AS VARCHAR) AS numero_caso,
        try_cast(prontuario AS INTEGER) AS prontuario,
        try_cast(IdadeEsposa_DG AS DOUBLE) AS IdadeEsposa_DG,
        CAST(IdadeMarido_DG AS VARCHAR) AS IdadeMarido_DG,
        hash,
        try_strptime(extraction_timestamp, '%Y%m%d_%H%M%S') AS extraction_timestamp
    ''',
    'view_micromanipulacao_oocitos': '''
        try_cast(id AS INTEGER) AS id,
        try_cast(id_micromanipulacao AS INTEGER) AS id_micromanipulacao,
        CAST(diaseguinte AS VARCHAR) AS diaseguinte,
        CAST(Maturidade AS VARCHAR) AS Maturidade,
        hash,
        try_strptime(extraction_timestamp, '%Y%m%d_%H%M%S') AS extraction_timestamp
    ''',
    'view_orcamentos': f'''
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
        {safe_date_parse_try_strptime('data_entrega')} AS data_entrega,
        CAST(centro_custos AS VARCHAR) AS centro_custos,
        CAST(valor_total AS VARCHAR) AS valor_total,
        CAST(forma AS VARCHAR) AS forma,
        CAST(parcelas AS VARCHAR) AS parcelas,
        CAST(comentarios AS VARCHAR) AS comentarios,
        CAST(forma_parcela AS VARCHAR) AS forma_parcela,
        CAST(valor AS VARCHAR) AS valor,
        {safe_date_parse_try_strptime('data_pagamento')} AS data_pagamento,
        CAST(descricao_pagamento AS VARCHAR) AS descricao_pagamento,
        {safe_date_parse_try_strptime('data')} AS data,
        CAST(responsavel AS VARCHAR) AS responsavel,
        {safe_date_parse_try_strptime('data_entrega_orcamento')} AS data_entrega_orcamento,
        {safe_date_parse_try_strptime('data_ultima_modificacao')} AS data_ultima_modificacao,
        hash,
        try_strptime(extraction_timestamp, '%Y%m%d_%H%M%S') AS extraction_timestamp
    ''',
    'view_procedimentos_financas': '''
        try_cast(id AS INTEGER) AS id,
        CAST(procedimento AS VARCHAR) AS procedimento,
        try_cast(REPLACE(REPLACE(valor, '.', ''), ',', '.') AS DOUBLE) AS valor,
        try_cast(duracao AS INTEGER) AS duracao,
        hash,
        try_strptime(extraction_timestamp, '%Y%m%d_%H%M%S') AS extraction_timestamp
    ''',
}

def deduplicate_and_format_sql(table, cast_sql):
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
    logger.info('Starting try_strptime silver loader')
    logger.info(f'Database path: {db_path}')
    
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
    
    logger.info('try_strptime silver loader finished successfully')

if __name__ == '__main__':
    main() 