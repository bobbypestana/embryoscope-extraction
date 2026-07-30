import duckdb
import os
import logging
import time

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)
logger = logging.getLogger(__name__)

# Database paths
BASE_DIR = os.path.dirname(__file__)
source_db_path = os.path.abspath(os.path.join(BASE_DIR, '..', 'database', 'clinisys_all.duckdb'))
target_db_path = os.path.abspath(os.path.join(BASE_DIR, '..', 'database', 'huntington_data_lake.duckdb'))

def main():
    logger.info("Starting gold loader for extrato_atendimento_central")
    logger.info(f"Source DB: {source_db_path}")
    logger.info(f"Target DB: {target_db_path}")

    start_time = time.time()

    # Highly optimized query: pre-casting prontuario to INTEGER in a CTE and using simple equality join conditions to enable HASH_JOINs on all joins
    query = """
    CREATE TABLE gold.extrato_atendimento_central AS
    WITH agenda_prepared AS (
        SELECT 
            id, data, inicio, data_agendamento_original, medico, medico2, 
            prontuario, CAST(prontuario AS INTEGER) AS prontuario_int, 
            evento, evento2, centro_custos, agenda, chegou, confirmado, oculto
        FROM source_db.silver.view_agenda
        WHERE oculto = '0' 
          AND CAST(data AS DATE) >= CAST('2019-01-01' AS DATE)
    )
    SELECT
        a.id AS agendamento_id,
        a.data AS data,
        a.inicio AS inicio,
        a.data_agendamento_original AS data_agendamento_original,
        a.medico AS medico,
        a.medico2 AS medico2,
        a.prontuario AS prontuario,
        a.evento AS evento,
        a.evento2 AS evento2,
        a.centro_custos AS centro_custos,
        a.agenda AS agenda,
        a.chegou AS chegou,
        a.confirmado AS confirmado,
        p.codigo AS paciente_codigo,
        p.esposa_nome AS paciente_nome,
        m1.nome AS medico_nome,
        m1.sobrenome AS medico_sobrenome,
        m2.nome AS medico2_nome,
        cc.nome AS centro_custos_nome,
        ag.nome AS agenda_nome,
        pr.procedimento AS procedimento_nome
    FROM
        agenda_prepared a
    LEFT JOIN source_db.silver.view_usuarios m1 
        ON a.medico = m1.id
    LEFT JOIN source_db.silver.view_medicos m2 
        ON a.medico2 = m2.id
    LEFT JOIN source_db.silver.view_pacientes p 
        ON a.prontuario_int = p.codigo
    LEFT JOIN source_db.silver.view_unidades cc 
        ON a.centro_custos = cc.id
    LEFT JOIN source_db.silver.view_agendas ag 
        ON a.agenda = ag.id
    LEFT JOIN source_db.silver.view_procedimentos pr 
        ON a.evento = pr.id
    ORDER BY
        a.id DESC
    """

    try:
        logger.info("Executing optimized DuckDB ATTACH and query...")
        with duckdb.connect(target_db_path) as target_con:
            target_con.execute(f"ATTACH '{source_db_path}' AS source_db (READ_ONLY);")
            
            target_con.execute("CREATE SCHEMA IF NOT EXISTS gold;")
            target_con.execute("DROP TABLE IF EXISTS gold.extrato_atendimento_central;")
            
            # Execute optimized query (runs Hash Joins)
            target_con.execute(query)
            
            gold_count = target_con.execute("SELECT count(*) FROM gold.extrato_atendimento_central").fetchone()[0]
            logger.info(f"Successfully created gold.extrato_atendimento_central table with {gold_count:,} rows.")

            # Fast reconciliation comparison
            logger.info("Performing native comparison/reconciliation...")
            overlapping_count = target_con.execute("""
                SELECT count(*) 
                FROM gold.extrato_atendimento_central g
                JOIN source_db.silver.view_extrato_atendimentos_central s
                  ON g.agendamento_id = s.agendamento_id
                WHERE CAST(g.data AS DATE) >= CAST('2019-01-01' AS DATE)
            """).fetchone()[0]
            
            logger.info(f"Number of overlapping agendamento_ids: {overlapping_count:,}")

            # Verify schema
            gold_cols = set(d[0] for d in target_con.execute("DESCRIBE gold.extrato_atendimento_central").fetchall())
            ingested_cols = set(d[0] for d in target_con.execute("DESCRIBE source_db.silver.view_extrato_atendimentos_central").fetchall())
            
            logger.info("=" * 80)
            logger.info("RECONCILIATION REPORT (FULLY OPTIMIZED HASH JOINS)")
            logger.info("=" * 80)
            if gold_cols == ingested_cols:
                logger.info("[PASS] Column schemas match exactly!")
            else:
                logger.warning(f"[FAIL] Column mismatch! Missing: {ingested_cols - gold_cols}")
            logger.info(f"Execution time: {time.time() - start_time:.2f} seconds")
            logger.info("=" * 80)

            target_con.execute("DETACH source_db;")

    except Exception as e:
        logger.error(f"Failed during gold loader execution: {e}", exc_info=True)
        return

if __name__ == "__main__":
    main()
