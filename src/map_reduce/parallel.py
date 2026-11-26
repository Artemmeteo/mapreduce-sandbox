import csv
import hashlib
import json
import os
import sys
import time
from collections import Counter
from contextlib import contextmanager
from datetime import datetime, timezone
from itertools import chain, islice, zip_longest
from pathlib import Path
from tempfile import TemporaryDirectory

from botocore.exceptions import ClientError

from .config import CountOptions, load_settings
from .infrastructure.files import read_csv, sha256
from .infrastructure.s3 import S3Storage
from .kubernetes_job import memory_snapshot
from .processing.mapreduce import count_mapreduce
from .verification.counts import load_counts


def timestamp():
    return datetime.now(timezone.utc).isoformat()


def split_csv(source, directory, total, n):
    if not 1 <= n <= total:
        raise ValueError("Invalid number of parts")
    parts = []
    with source.open(encoding="utf-8-sig", newline="") as stream:
        rows = csv.reader(stream, strict=True)
        header = next(rows)
        for index in range(n):
            size = total // n + int(index < total % n)
            path = directory / f"part-{index}.csv"
            with path.open("w", encoding="utf-8", newline="") as output:
                writer = csv.writer(output)
                writer.writerow(header)
                writer.writerows(islice(rows, size))
            parts.append({"id": index, "rows": size, "file": path.name, "sha256": sha256(path)})
        if next(rows, None) is not None:
            raise ValueError("Input exceeds expected row count")
    # Audit ordered records including every field, duplicate row and embedded newline.
    actual = chain.from_iterable(read_csv(directory / p["file"]) for p in parts)
    count = 0
    for left, right in zip_longest(read_csv(source), actual):
        if left != right:
            raise ValueError("Split changed a record")
        count += 1
    if count != total:
        raise ValueError("Wrong input population")
    return parts


def check_manifest(manifest, prefix):
    parts = manifest["parts"]
    if not parts or [p["id"] for p in parts] != list(range(manifest["n"])):
        raise ValueError("Missing or duplicate part ID")
    if any(p["rows"] < 1 or p["key"] != f"{prefix}/parts/{p['id']}.csv" for p in parts):
        raise ValueError("Unexpected part assignment")
    if sum(p["rows"] for p in parts) != manifest["rows"]:
        raise ValueError("Part row totals differ")


class Objects:
    def __init__(self):
        s = S3Storage(load_settings())
        self.client, self.bucket, self.settings = s.client, s.settings.bucket, s.settings

    def put(self, key, value):
        self.client.put_object(Bucket=self.bucket, Key=key, Body=value)

    def json(self, key, value):
        self.put(key, json.dumps(value, sort_keys=True).encode())

    def get(self, key):
        body = self.client.get_object(Bucket=self.bucket, Key=key)["Body"]
        try:
            return json.loads(body.read())
        finally:
            body.close()

    def fetch(self, key, path, digest):
        self.client.download_file(self.bucket, key, str(path))
        if sha256(path) != digest:
            raise ValueError(f"Checksum mismatch for {key}")


def rendezvous(store, prefix, index):
    # Both first-attempt workers start mapping on the same node clock
    ready = f"{prefix}/ready"
    store.json(f"{ready}/{index}", time.time())
    for _ in range(300):
        try:
            other = store.get(f"{ready}/{1-index}")
            start = max(other, store.get(f"{ready}/{index}")) + 0.5
            time.sleep(max(0, start-time.time()))
            return
        except ClientError as error:
            if error.response["Error"]["Code"] not in {"NoSuchKey", "404"}:
                raise
            time.sleep(0.1)
    raise TimeoutError("Peer did not become ready")


def worker(store, prefix, part, folder, synchronize):
    index = part["id"]
    source = folder / "input.csv"
    store.fetch(part["key"], source, part["sha256"])

    class AssignedCSV:
        settings = store.settings

        @contextmanager
        def rows(self):
            yield read_csv(source)

    if synchronize:
        rendezvous(store, prefix, index)
    started = timestamp()
    result = count_mapreduce(AssignedCSV(), folder, CountOptions(sizes=(2,), execution="inline",
                                                                workers=1, retries=0))
    ended = timestamp()
    if result["rows"] != part["rows"]:
        raise ValueError("Worker population mismatch")
    path = folder / "counts_k2.csv"
    digest = sha256(path)
    key = f"{prefix}/mapped/{index}/{digest}.csv"
    store.put(key, path.read_bytes())
    marker = {"id": index, "part_sha256": part["sha256"], "sha256": digest, "key": key,
              "rows": result["rows"], "with_skills": result["with_skills"],
              "occurrences": result["controls"][0]["expected_sum_binomial"]}
    store.json(f"{prefix}/done/{index}.json", marker)
    return {**marker, "compute_start": started, "compute_end": ended}


def merge_part(counts, part, marker, path, prefix):
    if (marker["id"] != part["id"] or marker["rows"] != part["rows"]
            or marker["part_sha256"] != part["sha256"] or marker["sha256"] != sha256(path)
            or marker["key"] != f"{prefix}/mapped/{part['id']}/{marker['sha256']}.csv"):
        raise ValueError("Completion marker does not match assigned part")
    contribution = load_counts(path, 2)
    if sum(contribution.values()) != marker["occurrences"]:
        raise ValueError("Partial frequencies differ from mapper control")
    counts.update(contribution)


def reduce(store, prefix, manifest, folder):
    counts = Counter()
    with_skills = 0
    for part in manifest["parts"]:
        marker = store.get(f"{prefix}/done/{part['id']}.json")
        path = folder / "partial.csv"
        store.fetch(marker["key"], path, marker["sha256"])
        merge_part(counts, part, marker, path, prefix)
        with_skills += marker["with_skills"]
    ordered = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    files = {}
    for name, pairs in (("counts_k2.csv", ordered), ("top10.csv", ordered[:10])):
        path = folder / name
        with path.open("w", newline="", encoding="utf-8") as output:
            writer = csv.writer(output)
            writer.writerow(("skill_1", "skill_2", "frequency"))
            writer.writerows((*pair, count) for pair, count in pairs)
        store.put(f"{prefix}/final/{name}", path.read_bytes())
        files[name] = sha256(path)
    return {"rows": manifest["rows"], "with_skills": with_skills, "unique": len(counts),
            "occurrences": sum(counts.values()), "files": files}


def main():
    role, prefix, expected, population = sys.argv[1:5]
    store = Objects()
    key = prefix + "/manifest.json"
    with TemporaryDirectory() as tmp:
        folder = Path(tmp)
        if role == "split":
            source = folder / "source.csv"
            store.fetch(store.settings.object_key, source, expected)
            parts = split_csv(source, folder, int(population), 2)
            for p in parts:
                p["key"] = f"{prefix}/parts/{p['id']}.csv"
                with (folder/p["file"]).open("rb") as stream:
                    store.put(p["key"], stream)
            result = {"parts": parts, "rows": int(population), "n": 2, "input_sha256": expected,
                      "ordered_records_verified": True}
            check_manifest(result, prefix)
            store.json(key, result)
        else:
            manifest = store.get(key)
            check_manifest(manifest, prefix)
            if (manifest["input_sha256"] != expected or manifest["rows"] != int(population)
                    or hashlib.sha256(json.dumps(manifest, sort_keys=True).encode()).hexdigest()
                    != sys.argv[5]):
                raise ValueError("Unexpected manifest")
            if role in {"worker", "retry"}:
                index = int(os.environ["JOB_COMPLETION_INDEX"]) if role == "worker" else 0
                result = worker(store, prefix, manifest["parts"][index], folder, role == "worker")
            elif role == "reduce":
                result = reduce(store, prefix, manifest, folder)
            else:
                raise ValueError(role)
        print(json.dumps({"result": result, "pod": os.environ["HOSTNAME"], "memory": memory_snapshot()}))


if __name__ == "__main__":
    main()
