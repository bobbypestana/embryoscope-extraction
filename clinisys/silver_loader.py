import duckdb
import os
import logging
from datetime import datetime

# Setup logging
LOGS_DIR = os.path.join(os.path.dirname(__file__), 'logs')
os.makedirs(LOGS_DIR, exist_ok=True)
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
LOG_PATH = os.path.join(LOGS_DIR, f'silver_loader_{timestamp}.log')
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

table_casts = {
    'view_medicamentos': '''
        CAST(id AS INTEGER) AS id,
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
        CASE WHEN data_inicial_clean IS NOT NULL THEN TRY_CAST(STRPTIME(data_inicial_clean, '%d/%m/%Y') AS DATE) ELSE NULL END AS data_inicial,
        CASE WHEN data_final_clean IS NOT NULL THEN TRY_CAST(STRPTIME(data_final_clean, '%d/%m/%Y') AS DATE) ELSE NULL END AS data_final,
        CASE WHEN hora_clean IS NOT NULL THEN TRY_CAST(STRPTIME(hora_clean, '%H:%M') AS TIME) ELSE NULL END AS hora,
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
        CAST(id AS INTEGER) AS id,
        CAST(nome AS VARCHAR) AS nome,
        CAST(tipo_medico AS VARCHAR) AS tipo_medico,
        CAST(registro AS INTEGER) AS registro,
        CAST(especialidade AS VARCHAR) AS especialidade,
        CAST(rqe AS DOUBLE) AS rqe,
        CAST(rqe_tipo AS VARCHAR) AS rqe_tipo,
        hash,
        STRPTIME(extraction_timestamp, '%Y%m%d_%H%M%S') AS extraction_timestamp
    ''',
    'view_pacientes': '''
        CAST(codigo AS INTEGER) AS codigo,
        CAST(prontuario_esposa AS INTEGER) AS prontuario_esposa,
        CAST(prontuario_marido AS INTEGER) AS prontuario_marido,
        CAST(prontuario_responsavel1 AS INTEGER) AS prontuario_responsavel1,
        CAST(prontuario_responsavel2 AS INTEGER) AS prontuario_responsavel2,
        CAST(prontuario_esposa_pel AS INTEGER) AS prontuario_esposa_pel,
        CAST(prontuario_marido_pel AS INTEGER) AS prontuario_marido_pel,
        CAST(prontuario_esposa_pc AS INTEGER) AS prontuario_esposa_pc,
        CAST(prontuario_marido_pc AS INTEGER) AS prontuario_marido_pc,
        CAST(prontuario_responsavel1_pc AS INTEGER) AS prontuario_responsavel1_pc,
        CAST(prontuario_responsavel2_pc AS INTEGER) AS prontuario_responsavel2_pc,
        CAST(prontuario_esposa_fc AS INTEGER) AS prontuario_esposa_fc,
        CAST(prontuario_marido_fc AS INTEGER) AS prontuario_marido_fc,
        CAST(prontuario_esposa_ba AS INTEGER) AS prontuario_esposa_ba,
        CAST(prontuario_marido_ba AS INTEGER) AS prontuario_marido_ba,
        CAST(esposa_nome AS VARCHAR) AS esposa_nome,
        CAST(marido_nome AS VARCHAR) AS marido_nome,
        CAST(unidade_origem AS DOUBLE) AS unidade_origem,
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
        CAST(id AS INTEGER) AS id,
        CAST(prontuario AS INTEGER) AS prontuario,
        CAST(unidade AS DOUBLE) AS unidade,
        CAST(idade_esposa AS DOUBLE) AS idade_esposa,
        CAST(idade_marido AS VARCHAR) AS idade_marido,
        -- Add more casts as needed for all columns
        hash,
        STRPTIME(extraction_timestamp, '%Y%m%d_%H%M%S') AS extraction_timestamp
    ''',
    'view_unidades': '''
        CAST(id AS INTEGER) AS id,
        CAST(nome AS VARCHAR) AS nome,
        hash,
        STRPTIME(extraction_timestamp, '%Y%m%d_%H%M%S') AS extraction_timestamp
    ''',
    'view_congelamentos_embrioes': '''
        CAST(id AS INTEGER) AS id,
        CAST(CodCongelamento AS VARCHAR) AS CodCongelamento,
        CAST(Unidade AS INTEGER) AS Unidade,
        CAST(prontuario AS INTEGER) AS prontuario,
        CAST(paciente AS VARCHAR) AS paciente,
        CASE WHEN REGEXP_MATCHES(Data, '^[0-9]{2}/[0-9]{2}/[0-9]{4}$') THEN STRPTIME(Data, '%d/%m/%Y') ELSE NULL END AS Data,
        CASE WHEN REGEXP_MATCHES(Hora, '^[0-9]{2}:[0-9]{2}$') THEN STRPTIME(Hora, '%H:%M')::TIME ELSE NULL END AS Hora,
        CAST(Ciclo AS VARCHAR) AS Ciclo,
        CAST(CicloRecongelamento AS VARCHAR) AS CicloRecongelamento,
        CAST(condicoes_amostra AS VARCHAR) AS condicoes_amostra,
        CAST(empresa_transporte AS VARCHAR) AS empresa_transporte,
        CAST(clinica_origem AS VARCHAR) AS clinica_origem,
        CAST(responsavel_recebimento AS DOUBLE) AS responsavel_recebimento,
        CASE WHEN REGEXP_MATCHES(responsavel_recebimento_data, '^[0-9]{2}/[0-9]{2}/[0-9]{4}$') THEN STRPTIME(responsavel_recebimento_data, '%d/%m/%Y') ELSE NULL END AS responsavel_recebimento_data,
        CAST(responsavel_armazenamento AS DOUBLE) AS responsavel_armazenamento,
        CASE WHEN REGEXP_MATCHES(responsavel_armazenamento_data, '^[0-9]{2}/[0-9]{2}/[0-9]{4}$') THEN STRPTIME(responsavel_armazenamento_data, '%d/%m/%Y') ELSE NULL END AS responsavel_armazenamento_data,
        CAST(NEmbrioes AS INTEGER) AS NEmbrioes,
        CAST(NPailletes AS VARCHAR) AS NPailletes,
        CAST(Identificacao AS VARCHAR) AS Identificacao,
        CAST(Tambor AS VARCHAR) AS Tambor,
        CAST(Cane AS VARCHAR) AS Cane,
        CAST(Cane2 AS VARCHAR) AS Cane2,
        CAST(Tecnica AS VARCHAR) AS Tecnica,
        CAST(Ovulo AS DOUBLE) AS Ovulo,
        CAST(D2 AS DOUBLE) AS D2,
        CAST(D3 AS DOUBLE) AS D3,
        CAST(D4 AS DOUBLE) AS D4,
        CAST(D5 AS DOUBLE) AS D5,
        CAST(D6 AS DOUBLE) AS D6,
        CAST(D7 AS DOUBLE) AS D7,
        CAST(rack AS VARCHAR) AS rack,
        CAST(rack2 AS DOUBLE) AS rack2,
        CAST(rack3 AS DOUBLE) AS rack3,
        CAST(rack4 AS DOUBLE) AS rack4,
        CAST(Observacoes AS VARCHAR) AS Observacoes,
        CAST(BiologoResponsavel AS DOUBLE) AS BiologoResponsavel,
        CAST(BiologoFIV AS VARCHAR) AS BiologoFIV,
        CAST(BiologoFIV2 AS VARCHAR) AS BiologoFIV2,
        CAST(status_financeiro AS VARCHAR) AS status_financeiro,
        CAST(responsavel_congelamento_d5 AS DOUBLE) AS responsavel_congelamento_d5,
        CAST(responsavel_checagem_d5 AS DOUBLE) AS responsavel_checagem_d5,
        CAST(responsavel_congelamento_d6 AS DOUBLE) AS responsavel_congelamento_d6,
        CAST(responsavel_checagem_d6 AS DOUBLE) AS responsavel_checagem_d6,
        CAST(responsavel_congelamento_d7 AS DOUBLE) AS responsavel_congelamento_d7,
        CAST(responsavel_checagem_d7 AS DOUBLE) AS responsavel_checagem_d7,
        hash,
        STRPTIME(extraction_timestamp, '%Y%m%d_%H%M%S') AS extraction_timestamp
    ''',
    'view_congelamentos_ovulos': '''
        CAST(id AS INTEGER) AS id,
        CAST(CodCongelamento AS VARCHAR) AS CodCongelamento,
        CAST(Unidade AS INTEGER) AS Unidade,
        CAST(prontuario AS INTEGER) AS prontuario,
        CAST(paciente AS VARCHAR) AS paciente,
        CASE WHEN REGEXP_MATCHES(Data, '^[0-9]{2}/[0-9]{2}/[0-9]{4}$') THEN STRPTIME(Data, '%d/%m/%Y') ELSE NULL END AS Data,
        CASE WHEN REGEXP_MATCHES(Hora, '^[0-9]{2}:[0-9]{2}$') THEN STRPTIME(Hora, '%H:%M')::TIME ELSE NULL END AS Hora,
        CAST(Ciclo AS VARCHAR) AS Ciclo,
        CAST(condicoes_amostra AS VARCHAR) AS condicoes_amostra,
        CAST(empresa_transporte AS VARCHAR) AS empresa_transporte,
        CAST(clinica_origem AS VARCHAR) AS clinica_origem,
        CAST(responsavel_recebimento AS DOUBLE) AS responsavel_recebimento,
        CASE WHEN REGEXP_MATCHES(responsavel_recebimento_data, '^[0-9]{2}/[0-9]{2}/[0-9]{4}$') THEN STRPTIME(responsavel_recebimento_data, '%d/%m/%Y') ELSE NULL END AS responsavel_recebimento_data,
        CAST(responsavel_armazenamento AS DOUBLE) AS responsavel_armazenamento,
        CASE WHEN REGEXP_MATCHES(responsavel_armazenamento_data, '^[0-9]{2}/[0-9]{2}/[0-9]{4}$') THEN STRPTIME(responsavel_armazenamento_data, '%d/%m/%Y') ELSE NULL END AS responsavel_armazenamento_data,
        CAST(NOvulos AS INTEGER) AS NOvulos,
        CAST(NPailletes AS VARCHAR) AS NPailletes,
        CAST(Identificacao AS VARCHAR) AS Identificacao,
        CAST(Tambor AS VARCHAR) AS Tambor,
        CAST(Cane AS VARCHAR) AS Cane,
        CAST(Cane2 AS VARCHAR) AS Cane2,
        CAST(Tecnica AS VARCHAR) AS Tecnica,
        CAST(Motivo AS VARCHAR) AS Motivo,
        CAST(Observacoes AS VARCHAR) AS Observacoes,
        CAST(BiologoResponsavel AS DOUBLE) AS BiologoResponsavel,
        CAST(BiologoFIV AS VARCHAR) AS BiologoFIV,
        CAST(BiologoFIV2 AS VARCHAR) AS BiologoFIV2,
        CAST(status_financeiro AS VARCHAR) AS status_financeiro,
        hash,
        STRPTIME(extraction_timestamp, '%Y%m%d_%H%M%S') AS extraction_timestamp
    ''',
    'view_descongelamentos_embrioes': '''
        CAST(id AS INTEGER) AS id,
        CAST(CodDescongelamento AS VARCHAR) AS CodDescongelamento,
        CAST(Unidade AS DOUBLE) AS Unidade,
        CAST(prontuario AS INTEGER) AS prontuario,
        CAST(doadora AS INTEGER) AS doadora,
        CASE WHEN REGEXP_MATCHES(DataCongelamento, '^[0-9]{2}/[0-9]{2}/[0-9]{4}$') THEN STRPTIME(DataCongelamento, '%d/%m/%Y') ELSE NULL END AS DataCongelamento,
        CASE WHEN REGEXP_MATCHES(DataDescongelamento, '^[0-9]{2}/[0-9]{2}/[0-9]{4}$') THEN STRPTIME(DataDescongelamento, '%d/%m/%Y') ELSE NULL END AS DataDescongelamento,
        CAST(Ciclo AS VARCHAR) AS Ciclo,
        CAST(Identificacao AS VARCHAR) AS Identificacao,
        CAST(CodCongelamento AS VARCHAR) AS CodCongelamento,
        CAST(Tambor AS VARCHAR) AS Tambor,
        CAST(Cane AS VARCHAR) AS Cane,
        CAST(PailletesDescongeladas AS VARCHAR) AS PailletesDescongeladas,
        CAST(Tecnica AS VARCHAR) AS Tecnica,
        CAST(Transferencia AS DOUBLE) AS Transferencia,
        CASE WHEN REGEXP_MATCHES(DataTransferencia, '^[0-9]{2}/[0-9]{2}/[0-9]{4}$') THEN STRPTIME(DataTransferencia, '%d/%m/%Y') ELSE NULL END AS DataTransferencia,
        CAST(Prateleira AS DOUBLE) AS Prateleira,
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
        CAST(responsavel_transferencia AS DOUBLE) AS responsavel_transferencia,
        CAST(Observacoes AS VARCHAR) AS Observacoes,
        CAST(BiologoFIV AS VARCHAR) AS BiologoFIV,
        CAST(BiologoFIV2 AS VARCHAR) AS BiologoFIV2,
        hash,
        STRPTIME(extraction_timestamp, '%Y%m%d_%H%M%S') AS extraction_timestamp
    ''',
    'view_descongelamentos_ovulos': '''
        CAST(id AS INTEGER) AS id,
        CAST(CodDescongelamento AS VARCHAR) AS CodDescongelamento,
        CAST(Unidade AS DOUBLE) AS Unidade,
        CAST(prontuario AS INTEGER) AS prontuario,
        CAST(doadora AS INTEGER) AS doadora,
        CASE WHEN REGEXP_MATCHES(DataCongelamento, '^[0-9]{2}/[0-9]{2}/[0-9]{4}$') THEN STRPTIME(DataCongelamento, '%d/%m/%Y') ELSE NULL END AS DataCongelamento,
        CASE WHEN REGEXP_MATCHES(DataDescongelamento, '^[0-9]{2}/[0-9]{2}/[0-9]{4}$') THEN STRPTIME(DataDescongelamento, '%d/%m/%Y') ELSE NULL END AS DataDescongelamento,
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
        CAST(id AS INTEGER) AS id,
        CAST(id_oocito AS INTEGER) AS id_oocito,
        CAST(id_congelamento AS INTEGER) AS id_congelamento,
        CAST(id_descongelamento AS INTEGER) AS id_descongelamento,
        CAST(prontuario AS INTEGER) AS prontuario,
        -- Add more casts as needed for all columns
        hash,
        STRPTIME(extraction_timestamp, '%Y%m%d_%H%M%S') AS extraction_timestamp
    ''',
    'view_extrato_atendimentos_central': '''
        TRY_CAST(agendamento_id AS INTEGER) AS agendamento_id,
        CASE WHEN REGEXP_MATCHES(data, '^[0-9]{2}/[0-9]{2}/[0-9]{4}$') THEN STRPTIME(data, '%d/%m/%Y') ELSE NULL END AS data,
        CAST(inicio AS VARCHAR) AS inicio,
        CASE WHEN REGEXP_MATCHES(data_agendamento_original, '^[0-9]{2}/[0-9]{2}/[0-9]{4}$') THEN STRPTIME(data_agendamento_original, '%d/%m/%Y') ELSE NULL END AS data_agendamento_original,
        CAST(medico AS DOUBLE) AS medico,
        CAST(medico2 AS DOUBLE) AS medico2,
        TRY_CAST(prontuario AS BIGINT) AS prontuario,
        TRY_CAST(evento AS BIGINT) AS evento,
        CAST(evento2 AS VARCHAR) AS evento2,
        TRY_CAST(centro_custos AS INTEGER) AS centro_custos,
        TRY_CAST(agenda AS INTEGER) AS agenda,
        CAST(chegou AS VARCHAR) AS chegou,
        TRY_CAST(confirmado AS INTEGER) AS confirmado,
        CAST(paciente_codigo AS DOUBLE) AS paciente_codigo,
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
        CAST(codigo_ficha AS INTEGER) AS codigo_ficha,
        CAST(numero_caso AS VARCHAR) AS numero_caso,
        CAST(prontuario AS INTEGER) AS prontuario,
        CAST(IdadeEsposa_DG AS DOUBLE) AS IdadeEsposa_DG,
        CAST(IdadeMarido_DG AS VARCHAR) AS IdadeMarido_DG,
        -- Add more casts as needed for all columns
        hash,
        STRPTIME(extraction_timestamp, '%Y%m%d_%H%M%S') AS extraction_timestamp
    ''',
    'view_micromanipulacao_oocitos': '''
        CAST(id AS INTEGER) AS id,
        CAST(id_micromanipulacao AS INTEGER) AS id_micromanipulacao,
        CAST(diaseguinte AS VARCHAR) AS diaseguinte,
        CAST(Maturidade AS VARCHAR) AS Maturidade,
        -- Add more casts as needed for all columns
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
        CASE WHEN REGEXP_MATCHES(data_entrega, '^[0-9]{2}/[0-9]{2}/[0-9]{4}$') THEN STRPTIME(data_entrega, '%d/%m/%Y') ELSE NULL END AS data_entrega,
        CAST(centro_custos AS VARCHAR) AS centro_custos,
        CAST(valor_total AS VARCHAR) AS valor_total,
        CAST(forma AS VARCHAR) AS forma,
        CAST(parcelas AS VARCHAR) AS parcelas,
        CAST(comentarios AS VARCHAR) AS comentarios,
        CAST(forma_parcela AS VARCHAR) AS forma_parcela,
        CAST(valor AS VARCHAR) AS valor,
        CASE WHEN REGEXP_MATCHES(data_pagamento, '^[0-9]{2}/[0-9]{2}/[0-9]{4}$') THEN STRPTIME(data_pagamento, '%d/%m/%Y') ELSE NULL END AS data_pagamento,
        CAST(descricao_pagamento AS VARCHAR) AS descricao_pagamento,
        CASE WHEN REGEXP_MATCHES(data, '^[0-9]{2}/[0-9]{2}/[0-9]{4}$') THEN STRPTIME(data, '%d/%m/%Y') ELSE NULL END AS data,
        CAST(responsavel AS VARCHAR) AS responsavel,
        CASE WHEN REGEXP_MATCHES(data_entrega_orcamento, '^[0-9]{2}/[0-9]{2}/[0-9]{4}$') THEN STRPTIME(data_entrega_orcamento, '%d/%m/%Y') ELSE NULL END AS data_entrega_orcamento,
        CASE WHEN REGEXP_MATCHES(data_ultima_modificacao, '^[0-9]{2}/[0-9]{2}/[0-9]{4}$') THEN STRPTIME(data_ultima_modificacao, '%d/%m/%Y') ELSE NULL END AS data_ultima_modificacao,
        hash,
        STRPTIME(extraction_timestamp, '%Y%m%d_%H%M%S') AS extraction_timestamp
    ''',
    'view_procedimentos_financas': '''
        CAST(id AS INTEGER) AS id,
        CAST(procedimento AS VARCHAR) AS procedimento,
        CASE WHEN REGEXP_MATCHES(REPLACE(REPLACE(valor, '.', ''), ',', '.'), '^[0-9]+(\\.[0-9]+)?$') THEN CAST(REPLACE(REPLACE(valor, '.', ''), ',', '.') AS DOUBLE) ELSE NULL END AS valor,
        CAST(duracao AS INTEGER) AS duracao,
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
                cte_cols.append(f"CASE WHEN REGEXP_MATCHES({col}, '^[0-9]{{2}}:[0-9]{{2}}$') THEN {col} ELSE NULL END AS {col}_clean")
            else:
                cte_cols.append(f"CASE WHEN REGEXP_MATCHES({col}, '^[0-9]{{2}}/[0-9]{{2}}/[0-9]{{4}}$') THEN {col} ELSE NULL END AS {col}_clean")
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
    logger.info('Starting silver loader')
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
    logger.info('Silver loader finished successfully')

if __name__ == '__main__':
    main() 