import csv
import io
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from ..config import S3Settings


class S3Storage:
    def __init__(self, settings: S3Settings):
        self.settings = settings
        self.client = boto3.client(
            "s3",
            endpoint_url=settings.endpoint,
            aws_access_key_id=settings.access_key,
            aws_secret_access_key=settings.secret_key,
            region_name="us-east-1",
            config=Config(
                signature_version="s3v4",
                s3={"addressing_style": "path"},
                retries={"max_attempts": 3, "mode": "standard"},
            ),
        )

    def ensure_bucket(self) -> None:
        try:
            self.client.head_bucket(Bucket=self.settings.bucket)
        except ClientError as error:
            if error.response["Error"]["Code"] not in {"404", "NoSuchBucket"}:
                raise
            self.client.create_bucket(Bucket=self.settings.bucket)

    def head(self, key: str | None = None) -> dict:
        return self.client.head_object(
            Bucket=self.settings.bucket, Key=key or self.settings.object_key
        )

    @contextmanager
    def rows(self) -> Iterator[csv.DictReader]:
        response = self.client.get_object(Bucket=self.settings.bucket, Key=self.settings.object_key)
        stream = io.TextIOWrapper(response["Body"], encoding="utf-8-sig", newline="")
        try:
            yield csv.DictReader(stream)
        finally:
            stream.close()

    def upload(self, path: Path, key: str) -> None:
        self.client.upload_file(str(path), self.settings.bucket, key)

    def publish(self, results: Path) -> int:
        count = 0
        for path in sorted(results.rglob("*")):
            if path.is_file():
                self.upload(path, "results/variant=3/" + path.relative_to(results).as_posix())
                count += 1
        return count
