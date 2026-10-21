from pathlib import Path

import duckdb

from ..config import CountOptions, S3Settings
from ..infrastructure.duckdb import SQL, export_query, vacancies_connection
from ..infrastructure.files import write_json

EXPECTED_FORMULAS = {
    1: "len(skills)",
    2: "len(skills)*(len(skills)-1)/2",
    3: "len(skills)*(len(skills)-1)*(len(skills)-2)/6",
}


def count_duckdb(
    settings: S3Settings, output: Path, options: CountOptions, file_format: str = "csv"
) -> dict:
    controls = []
    with vacancies_connection(settings, file_format) as connection:
        for size in options.sizes:
            counts = export_query(
                connection,
                (SQL / f"counts_k{size}.sql").read_text(),
                output / f"counts_k{size}.csv",
            )
            expected = connection.execute(
                f"SELECT coalesce(sum({EXPECTED_FORMULAS[size]}),0)::BIGINT FROM vacancies"
            ).fetchone()[0]
            total = sum(row["frequency"] for row in counts)
            if total != expected:
                raise ValueError("Frequency total differs from sum of binomial coefficients")
            controls.append(
                {
                    "k": size,
                    "unique_combinations": len(counts),
                    "total_occurrences": total,
                    "expected_sum_binomial": expected,
                }
            )
        rows, with_skills = connection.execute(
            "SELECT count(*),count(*) FILTER(WHERE len(skills)>0) FROM vacancies"
        ).fetchone()
        if connection.execute("SELECT count(*) FROM input_rows").fetchone()[0] != rows:
            raise ValueError("Selected input contains rows outside the SQL variant condition")
    result = {
        "engine": "duckdb",
        "version": duckdb.__version__,
        "format": file_format,
        "s3_uri": settings.parquet_uri if file_format == "parquet" else settings.uri,
        "rows": rows,
        "with_skills": with_skills,
        "threads": 2,
        "controls": controls,
    }
    write_json(output / "run.json", result)
    return result
