import sys
from pathlib import Path
from tempfile import TemporaryDirectory

from map_reduce.config import load_settings
from map_reduce.infrastructure.files import sha256
from map_reduce.infrastructure.s3 import S3Storage

RESULT_FILES = ("counts_k2.csv", "top10.csv")


def download_results(prefix: str, output: Path) -> None:
    storage = S3Storage(load_settings())
    output.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(dir=output) as staging:
        for name in RESULT_FILES:
            storage.client.download_file(
                storage.settings.bucket, f"{prefix.rstrip('/')}/{name}", str(Path(staging) / name)
            )
        for name in RESULT_FILES:
            file = output / name
            (Path(staging) / name).replace(file)
            print(f"{sha256(file)}  {file}")


if __name__ == "__main__":
    if len(sys.argv) != 3 or not sys.argv[1].strip("/"):
        raise SystemExit(f"Usage: {Path(sys.argv[0]).name} <s3-prefix> <output-directory>")
    download_results(sys.argv[1], Path(sys.argv[2]))
