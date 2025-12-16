import os
import duckdb


def main() -> None:
    repo_root = os.path.dirname(os.path.dirname(__file__))
    db_path = os.path.join(repo_root, "database", "huntington_data_lake.duckdb")

    con = duckdb.connect(db_path)

    print("--- Count for patient 138475 in gold.data_ploidia ---")
    print(
        con.execute(
            'SELECT COUNT(*) AS rows_138475_dp '
            'FROM gold.data_ploidia '
            'WHERE "Patient ID" = 138475'
        ).df()
    )

    print("\n--- Sample rows for patient 138475 ---")
    df = con.execute(
        'SELECT * '
        'FROM gold.data_ploidia '
        'WHERE "Patient ID" = 138475 '
        'ORDER BY "Slide ID", "Embryo ID" '
        "LIMIT 10"
    ).df()
    if df.empty:
        print("No rows found for Patient ID = 138475")
    else:
        print(df.to_string(index=False))


if __name__ == "__main__":
    main()


