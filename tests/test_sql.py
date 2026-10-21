import csv
import tempfile
import unittest
from pathlib import Path

import duckdb
from test_processing import MemoryStorage

from map_reduce.config import CountOptions
from map_reduce.infrastructure.duckdb import SQL, export_query, prepare_vacancy_tables, sql_literal
from map_reduce.processing.mapreduce import count_mapreduce
from map_reduce.verification.counts import verify_counts


class SQLTests(unittest.TestCase):
    def test_csv_parquet_and_both_mapreduce_modes_match_every_frequency(self):
        stacks = ["Java;JAVA;SQL;Docker", "Java;SQL", "", "C++;C#;ООП", "Java;Jаva"]
        rows = [
            {
                "source_row": str(i),
                "key_skills": value,
                "name": "Java Developer",
                "role": "Программист, разработчик",
                "area": "Москва",
                "description": "текст\nстрока",
            }
            for i, value in enumerate(stacks, 1)
        ]
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "source.csv"
            with source.open("w", encoding="utf-8", newline="") as stream:
                writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
                writer.writeheader()
                writer.writerows(rows)
            parquet = root / "source.parquet"
            with duckdb.connect() as connection:
                connection.execute(
                    f"COPY (SELECT * FROM read_csv({sql_literal(str(source))}, all_varchar=true)) "
                    f"TO {sql_literal(str(parquet))} (FORMAT PARQUET, COMPRESSION ZSTD)"
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT * FROM read_csv(?, all_varchar=true)", [str(source)]
                    ).fetchall(),
                    connection.execute("SELECT * FROM read_parquet(?)", [str(parquet)]).fetchall(),
                )
            for mode in ("baseline", "optimized"):
                count_mapreduce(
                    MemoryStorage(rows),
                    root / mode,
                    CountOptions(mode=mode, workers=1, chunk_size=1),
                )
            for file_format, path in (("csv", source), ("parquet", parquet)):
                with duckdb.connect() as connection:
                    prepare_vacancy_tables(connection, str(path), file_format)
                    for size in (1, 2, 3):
                        export_query(
                            connection,
                            (SQL / f"counts_k{size}.sql").read_text(),
                            root / file_format / f"counts_k{size}.csv",
                        )
                self.assertTrue(verify_counts(root / "optimized", root / file_format)["passed"])
            self.assertTrue(verify_counts(root / "baseline", root / "optimized")["passed"])

    def test_sql_variant_is_independent_of_extraction(self):
        with tempfile.TemporaryDirectory() as temporary, duckdb.connect() as connection:
            source = Path(temporary) / "source.csv"
            source.write_text(
                "source_row,name,role,area,key_skills\n"
                '1,Java Developer,"Программист, разработчик",Москва,Java\n'
                '2,JavaScript Developer,"Программист, разработчик",Москва,JS\n'
                "3,Java QA,Тестировщик,Москва,Java\n",
                encoding="utf-8",
            )
            prepare_vacancy_tables(connection, str(source))
            self.assertEqual(
                connection.execute("SELECT source_row FROM vacancies").fetchall(), [("1",)]
            )
