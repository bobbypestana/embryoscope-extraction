#!/usr/bin/env python3
"""
Script para listar todas as tabelas da camada silver_embryoscope do huntington_data_lake.duckdb
"""

import duckdb
import pandas as pd
import os

def main():
    # Conectar ao banco de dados huntington_data_lake
    db_path = os.path.join('..','..', 'database', 'huntington_data_lake.duckdb')
    con = duckdb.connect(db_path)

    # Obter lista de todas as tabelas da camada silver_embryoscope
    silver_embryoscope_tables = con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'silver_embryoscope'").fetchdf()['table_name'].tolist()

    print("=" * 70)
    print("LISTA COMPLETA DAS TABELAS DA CAMADA SILVER_EMBRYOSCOPE - HUNTINGTON_DATA_LAKE")
    print("=" * 70)
    print(f"Total de tabelas: {len(silver_embryoscope_tables)}")
    print()

    # Listar todas as tabelas numeradas
    for i, table in enumerate(sorted(silver_embryoscope_tables), 1):
        print(f"{i:2d}. {table}")

    print()
    print("=" * 70)
    print("DETALHES ADICIONAIS:")
    print("=" * 70)

    # Mostrar contagem de registros para cada tabela
    for table in sorted(silver_embryoscope_tables):
        try:
            count = con.execute(f"SELECT COUNT(*) FROM silver_embryoscope.{table}").fetchone()[0]
            print(f"{table:<40} | {count:>10,} registros")
        except Exception as e:
            print(f"{table:<40} | ERRO: {str(e)}")

    con.close()

if __name__ == "__main__":
    main()
