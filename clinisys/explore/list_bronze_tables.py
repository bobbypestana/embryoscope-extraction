#!/usr/bin/env python3
"""
Script para listar todas as tabelas da camada bronze do clinisys_all.duckdb
"""

import duckdb
import pandas as pd
import os

def main():
    # Conectar ao banco de dados
    db_path = os.path.join('..','..', 'database', 'clinisys_all.duckdb') if not os.path.exists('database/clinisys_all.duckdb') else 'database/clinisys_all.duckdb'
    con = duckdb.connect(db_path)

    # Obter lista de todas as tabelas da camada bronze
    bronze_tables = con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'bronze'").fetchdf()['table_name'].tolist()

    print("=" * 60)
    print("LISTA COMPLETA DAS TABELAS DA CAMADA BRONZE - CLINISYS_ALL")
    print("=" * 60)
    print(f"Total de tabelas: {len(bronze_tables)}")
    print()

    # Listar todas as tabelas numeradas
    for i, table in enumerate(sorted(bronze_tables), 1):
        print(f"{i:2d}. {table}")

    print()
    print("=" * 60)
    print("DETALHES ADICIONAIS:")
    print("=" * 60)

    # Mostrar contagem de registros para cada tabela
    for table in sorted(bronze_tables):
        try:
            count = con.execute(f"SELECT COUNT(*) FROM bronze.{table}").fetchone()[0]
            print(f"{table:<35} | {count:>10,} registros")
        except Exception as e:
            print(f"{table:<35} | ERRO: {str(e)}")

    con.close()

if __name__ == "__main__":
    main()

