import json
from pathlib import Path

from ..infrastructure.files import read_csv, write_json


def load_counts(path: Path, size: int) -> dict[tuple[str, ...], int]:
    counts = {}
    for row in read_csv(path):
        key = tuple(row[f"skill_{position}"] for position in range(1, size + 1))
        if key in counts:
            raise ValueError(f"Duplicate key in {path}: {key}")
        counts[key] = int(row["frequency"])
    return counts


def verify_counts(left: Path, right: Path, output: Path | None = None, sizes=(1, 2, 3)) -> dict:
    checks = []
    for size in sizes:
        first = load_counts(left / f"counts_k{size}.csv", size)
        second = load_counts(right / f"counts_k{size}.csv", size)
        mismatches = [
            {"skills": key, "left": first.get(key), "right": second.get(key)}
            for key in sorted(first.keys() | second.keys())
            if first.get(key) != second.get(key)
        ]
        checks.append(
            {
                "k": size,
                "left_keys": len(first),
                "right_keys": len(second),
                "left_total": sum(first.values()),
                "right_total": sum(second.values()),
                "mismatch_count": len(mismatches),
                "examples": mismatches[:10],
            }
        )
    population_matches = True
    if (left / "run.json").exists() and (right / "run.json").exists():
        first_run = json.loads((left / "run.json").read_text())
        second_run = json.loads((right / "run.json").read_text())
        population_matches = all(
            first_run.get(key) == second_run.get(key) for key in ("rows", "with_skills")
        )
    result = {
        "passed": all(check["mismatch_count"] == 0 for check in checks) and population_matches,
        "left": str(left),
        "right": str(right),
        "checks": checks,
        "population_matches": population_matches,
    }
    if output is not None:
        write_json(output, result)
    if not result["passed"]:
        raise ValueError("MapReduce and DuckDB results differ; inspect verification checks")
    return result
