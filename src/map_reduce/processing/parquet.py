import time
from pathlib import Path

from ..infrastructure.duckdb import input_query, s3_connection, sql_literal
from ..infrastructure.files import write_json


def convert_to_parquet(storage, output: Path) -> dict:
    output.mkdir(parents=True, exist_ok=True)
    path = output / "java_vacancies.parquet"
    started = time.perf_counter()
    with s3_connection(storage.settings) as connection:
        connection.execute("CREATE TEMP TABLE original AS " + input_query(storage.settings.uri))
        connection.execute(
            f"COPY original TO {sql_literal(str(path))} (FORMAT PARQUET, COMPRESSION ZSTD)"
        )
        rows = connection.execute("SELECT count(*) FROM original").fetchone()[0]
        storage.upload(path, storage.settings.parquet_key)
        parquet = input_query(storage.settings.parquet_uri, "parquet")
        difference = connection.execute(f"""
            SELECT count(*) FROM (
                (SELECT * FROM original EXCEPT ALL {parquet})
                UNION ALL ({parquet} EXCEPT ALL SELECT * FROM original)
            )
        """).fetchone()[0]
        if difference:
            raise ValueError("Parquet differs from the source CSV")
    result = {
        "s3_uri": storage.settings.parquet_uri,
        "rows": rows,
        "compression": "zstd",
        "csv_bytes": storage.head()["ContentLength"],
        "parquet_bytes": storage.head(storage.settings.parquet_key)["ContentLength"],
        "conversion_seconds": time.perf_counter() - started,
        "exact_match": True,
    }
    write_json(output / "parquet.json", result)
    return result
