import os
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RESULTS = ROOT / "results"
REGIONS = ("Москва", "Санкт-Петербург", "Новосибирск")


@dataclass(frozen=True)
class S3Settings:
    access_key: str = field(repr=False)
    secret_key: str = field(repr=False)
    endpoint: str = "http://127.0.0.1:9010"
    bucket: str = "hh-lake"
    object_key: str = "variant=3/java_vacancies.csv"

    @property
    def uri(self) -> str:
        return f"s3://{self.bucket}/{self.object_key}"

    @property
    def parquet_key(self) -> str:
        return str(Path(self.object_key).with_suffix(".parquet"))

    @property
    def parquet_uri(self) -> str:
        return f"s3://{self.bucket}/{self.parquet_key}"


def load_settings() -> S3Settings:
    values = {}
    env_file = ROOT / ".env"
    if env_file.exists():
        for line in env_file.read_text(encoding="utf-8").splitlines():
            if line.strip() and not line.lstrip().startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                values[key.strip()] = value.strip().strip('"').strip("'")
    values.update(os.environ)
    return S3Settings(
        endpoint=values.get("S3_ENDPOINT", "http://127.0.0.1:9010"),
        access_key=values["S3_ACCESS_KEY"],
        secret_key=values["S3_SECRET_KEY"],
    )


@dataclass(frozen=True)
class CountOptions:
    sizes: tuple[int, ...] = (1, 2, 3)
    mode: str = "optimized"
    chunk_size: int = 250
    workers: int = 2
    retries: int = 2
    failure_chunk: int | None = None

    def __post_init__(self):
        if not self.sizes or len(set(self.sizes)) != len(self.sizes):
            raise ValueError("Combination sizes must be nonempty and unique")
        if any(size not in (1, 2, 3) for size in self.sizes):
            raise ValueError("Combination sizes must be 1, 2 or 3")
        if self.mode not in ("baseline", "optimized"):
            raise ValueError("Unknown MapReduce mode")
        if self.chunk_size < 1 or self.workers < 1 or self.retries < 0:
            raise ValueError("Positive chunk size/workers and nonnegative retries required")
        if self.failure_chunk is not None and self.failure_chunk < 0:
            raise ValueError("Failure chunk must be nonnegative")
