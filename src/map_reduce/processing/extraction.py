import csv
from collections import Counter
from pathlib import Path

from ..domain.vacancy import belongs_to_variant
from ..infrastructure.files import sha256, write_json
from ..infrastructure.s3 import S3Storage


def extract_variant(source: Path, output: Path, storage: S3Storage) -> dict:
    latest: dict[tuple[str, str], tuple[tuple, dict | None]] = {}
    source_rows = 0
    matching_rows = 0
    with source.open(encoding="utf-8-sig", newline="") as stream:
        reader = csv.DictReader(stream)
        if not reader.fieldnames:
            raise ValueError("Source CSV has no header")
        fields = ["source_row", *reader.fieldnames]
        for source_rows, raw in enumerate(reader, start=1):
            identity = (raw["source"], raw["vacancy_id"])
            matches = belongs_to_variant(raw)
            matching_rows += matches
            rank = (raw["snapshot_date"], raw["published_at"], source_rows)
            previous = latest.get(identity)
            if previous is None or rank > previous[0]:
                # Retain only dates/identity for other variants, not their descriptions.
                selected = {"source_row": str(source_rows), **raw} if matches else None
                latest[identity] = rank, selected

    selected_rows = sorted(
        (row for _, row in latest.values() if row is not None),
        key=lambda row: int(row["source_row"]),
    )
    output.mkdir(parents=True, exist_ok=True)
    path = output / "java_vacancies.csv"
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        writer.writerows(selected_rows)

    storage.ensure_bucket()
    storage.upload(path, storage.settings.object_key)
    remote = storage.head()
    if remote["ContentLength"] != path.stat().st_size:
        raise ValueError("Uploaded object size differs from extracted CSV")
    result = {
        "source_path": str(source.resolve()),
        "source_sha256": sha256(source),
        "source_bytes": source.stat().st_size,
        "source_rows": source_rows,
        "source_unique_vacancies": len(latest),
        "source_duplicate_rows": source_rows - len(latest),
        "matching_source_rows": matching_rows,
        "selected_vacancies": len(selected_rows),
        "selected_sha256": sha256(path),
        "selected_bytes": path.stat().st_size,
        "s3_uri": storage.settings.uri,
        "s3_etag": remote["ETag"],
        "s3_bytes": remote["ContentLength"],
        "areas": Counter(row.get("area", "") for row in selected_rows).most_common(15),
        "currency": dict(Counter(row.get("currency", "") for row in selected_rows)),
        "gross": dict(Counter(row.get("gross", "") for row in selected_rows)),
    }
    write_json(output / "extraction.json", result)
    return result
