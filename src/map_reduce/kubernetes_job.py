import argparse
import hashlib
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

from .config import CountOptions, load_settings
from .infrastructure.files import sha256, write_json
from .infrastructure.s3 import S3Storage
from .processing.mapreduce import count_mapreduce


def memory_snapshot(root=Path("/sys/fs/cgroup")):
    result = {"method": "cgroup-v2 memory.peak", "unit": "bytes"}
    for name in ("memory.peak", "memory.current", "memory.max", "memory.swap.max"):
        value = (root / name).read_text().strip()
        result[name] = int(value) if value != "max" else value
    result["memory.events"] = {
        key: int(value) for key, value in
        (line.split() for line in (root / "memory.events").read_text().splitlines())
    }
    return result


def validate_input(storage, expected_sha256, expected_rows):
    digest = hashlib.sha256()
    response = storage.client.get_object(
        Bucket=storage.settings.bucket, Key=storage.settings.object_key,
    )
    body = response["Body"]
    try:
        for block in body.iter_chunks(chunk_size=1024 * 1024):
            digest.update(block)
    finally:
        body.close()
    with storage.rows() as rows:
        row_count = sum(1 for _ in rows)
    result = {"sha256": digest.hexdigest(), "rows": row_count,
              "bytes": response["ContentLength"], "s3_uri": storage.settings.uri}
    if result["sha256"] != expected_sha256 or row_count != expected_rows:
        raise ValueError(f"Input differs from extraction.json: {result}")
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-prefix", required=True)
    parser.add_argument("--expected-sha256", required=True)
    parser.add_argument("--expected-rows", type=int, required=True)
    parser.add_argument("--reference-sha256", required=True)
    parser.add_argument("--preflight", action="store_true")
    args = parser.parse_args()
    storage = S3Storage(load_settings())
    output = Path("/tmp/pair-results")
    started = time.monotonic()

    def progress(stage, details=None):
        print(json.dumps({"utc": datetime.now(timezone.utc).isoformat(), "stage": stage,
                          **(details or {}), "memory": memory_snapshot()}), flush=True)

    progress("input-validation")
    input_info = validate_input(storage, args.expected_sha256, args.expected_rows)
    write_json(output / "input-verification.json", input_info)
    progress("input-validated", input_info)
    if not args.preflight:
        options = CountOptions(sizes=(2,), workers=1, execution="inline", retries=0)
        result = count_mapreduce(storage, output, options, progress=progress)
        if result["rows"] != args.expected_rows:
            raise ValueError("Unexpected computation population")
    progress("publishing")
    prefix = args.output_prefix.strip("/")
    for path in sorted(output.iterdir()):
        if path.is_file():
            # Single PUT: no transfer thread pool; 15k pair rows fit in <1 MiB.
            with path.open("rb") as body:
                storage.client.put_object(Bucket=storage.settings.bucket,
                                          Key=f"{prefix}/{path.name}", Body=body)
    metrics = {"elapsed_seconds": time.monotonic() - started, "input": input_info,
               "reference_sha256": args.reference_sha256,
               "source_id": os.environ.get("SOURCE_ID"),
               "image_tag": os.environ.get("IMAGE_TAG"),
               "output_prefix": prefix, "memory": memory_snapshot()}
    if not args.preflight:
        metrics["counts_sha256"] = sha256(output / "counts_k2.csv")
    write_json(output / "metrics.json", metrics)
    storage.client.put_object(Bucket=storage.settings.bucket, Key=f"{prefix}/metrics.json",
                              Body=(output / "metrics.json").read_bytes())
    progress("complete", metrics)


if __name__ == "__main__":
    main()
