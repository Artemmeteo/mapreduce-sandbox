from contextlib import contextmanager
from pathlib import Path
from urllib.parse import urlparse

import duckdb

from ..config import ROOT, S3Settings
from .files import write_csv

SQL = Path(__file__).parent / "sql"


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def local_connection():
    connection = duckdb.connect()
    connection.execute("SET threads=2")
    connection.execute("SET memory_limit='2GB'")
    connection.execute(f"SET extension_directory={sql_literal(str(ROOT / '.duckdb_extensions'))}")
    return connection


def install_httpfs() -> str:
    with local_connection() as connection:
        connection.execute("INSTALL httpfs FROM 'https://extensions.duckdb.org'")
        connection.execute("LOAD httpfs")
    return duckdb.__version__


def input_query(source_uri: str, file_format: str = "csv") -> str:
    source = sql_literal(source_uri)
    if file_format == "csv":
        return f"SELECT * FROM read_csv({source}, header=true, all_varchar=true, delim=',')"
    if file_format == "parquet":
        return f"SELECT * FROM read_parquet({source}, hive_partitioning=false)"
    raise ValueError("Input format must be csv or parquet")


def prepare_vacancy_tables(connection, source_uri, file_format="csv", analysis=False):
    fields = "source_row, name, role, area, key_skills"
    if analysis:
        fields += ", salary_from, salary_to, currency, gross, experience, schedule"
    connection.execute(
        f"CREATE TEMP VIEW input_rows AS SELECT {fields} FROM ({input_query(source_uri, file_format)})"
    )
    connection.execute((SQL / "prepare.sql").read_text())


@contextmanager
def s3_connection(settings: S3Settings):
    connection = local_connection()
    try:
        connection.execute("LOAD httpfs")
        endpoint = urlparse(settings.endpoint)
        connection.execute(f"""CREATE SECRET local_s3 (
            TYPE S3, KEY_ID {sql_literal(settings.access_key)},
            SECRET {sql_literal(settings.secret_key)},
            REGION 'us-east-1', ENDPOINT {sql_literal(endpoint.netloc)}, URL_STYLE 'path',
            USE_SSL {"true" if endpoint.scheme == "https" else "false"}
        )""")
        yield connection
    finally:
        connection.close()


@contextmanager
def vacancies_connection(settings, file_format="csv", analysis=False):
    with s3_connection(settings) as connection:
        uri = settings.parquet_uri if file_format == "parquet" else settings.uri
        prepare_vacancy_tables(connection, uri, file_format, analysis)
        yield connection


def export_query(connection, query: str, path: Path, params=None) -> list[dict]:
    result = connection.execute(query, params or [])
    fields = [column[0] for column in result.description]
    rows = [dict(zip(fields, row)) for row in result.fetchall()]
    write_csv(path, rows, fields)
    return rows
