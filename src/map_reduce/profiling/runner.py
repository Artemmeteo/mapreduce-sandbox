import platform
import statistics
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory

import psutil

from ..infrastructure.files import write_csv, write_json
from ..verification.counts import verify_counts

METHODS = ("baseline", "optimized", "duckdb_csv", "duckdb_parquet")


def job_command(method: str, output: Path, sizes: tuple[int, ...]) -> list[str]:
    sql = method.startswith("duckdb_")
    command = [
        sys.executable,
        "-m",
        "map_reduce.cli",
        "run-duckdb" if sql else "run-mr",
        "--out",
        str(output),
        "--sizes",
        ",".join(map(str, sizes)),
    ]
    command += ["--format", method.removeprefix("duckdb_")] if sql else ["--mode", method]
    return command


def tree_rss(process: psutil.Process) -> int:
    try:
        processes = [process, *process.children(recursive=True)]
    except psutil.NoSuchProcess:
        return 0
    total = 0
    for child in processes:
        try:
            total += child.memory_info().rss
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
    return total


def profile_process(command: list[str], log: Path, check=True) -> dict:
    peak_rss = 0
    started = time.perf_counter()
    with log.open("w", encoding="utf-8") as stream:
        process = subprocess.Popen(command, stdout=stream, stderr=subprocess.STDOUT)
        observed = psutil.Process(process.pid)
        while process.poll() is None:
            peak_rss = max(peak_rss, tree_rss(observed))
            time.sleep(0.01)
        exit_code = process.wait()
    result = {
        "wall_seconds": time.perf_counter() - started,
        "peak_tree_rss_mib": peak_rss / 1024**2,
        "exit_code": exit_code,
    }
    if check and exit_code != 0:
        raise RuntimeError(f"Job exited with {exit_code}:\n{log.read_text()}")
    return result


def aggregate_runs(runs: list[dict]):
    summary = []
    for method in METHODS:
        for size in (1, 2, 3):
            selected = [run for run in runs if run["method"] == method and run["k"] == size]
            elapsed = [run["wall_seconds"] for run in selected]
            peaks = [run["peak_tree_rss_mib"] for run in selected]
            summary.append(
                {
                    "method": method,
                    "k": size,
                    "attempts": len(selected),
                    "median_seconds": statistics.median(elapsed),
                    "min_seconds": min(elapsed),
                    "max_seconds": max(elapsed),
                    "median_peak_rss_mib": statistics.median(peaks),
                    "min_peak_rss_mib": min(peaks),
                    "max_peak_rss_mib": max(peaks),
                }
            )
    improvements = []
    for before_method, after_method in (
        ("baseline", "optimized"),
        ("duckdb_csv", "duckdb_parquet"),
    ):
        for size in (1, 2, 3):
            before = next(r for r in summary if r["method"] == before_method and r["k"] == size)
            after = next(r for r in summary if r["method"] == after_method and r["k"] == size)
            improvements.append(
                {
                    "before": before_method,
                    "after": after_method,
                    "k": size,
                    "speedup": before["median_seconds"] / after["median_seconds"],
                    "time_reduction_pct": 100
                    * (1 - after["median_seconds"] / before["median_seconds"]),
                    "memory_reduction_pct": 100
                    * (1 - after["median_peak_rss_mib"] / before["median_peak_rss_mib"]),
                }
            )
    return summary, improvements


def benchmark(settings, results: Path, repeats=3) -> list[dict]:
    if repeats < 3:
        raise ValueError("At least three measured attempts are required")
    runs = []
    with TemporaryDirectory(prefix="java-lab-bench-") as temporary:
        root = Path(temporary)
        log = root / "job.log"

        def execute(method, sizes, attempt):
            directory = root / f"{method}-k{sizes[0]}-run{attempt}"
            profile = profile_process(job_command(method, directory, sizes), log)
            verify_counts(results / "mapreduce", directory, sizes=sizes)
            if attempt:
                runs.append(dict(method=method, k=sizes[0], attempt=attempt, **profile))
                print(
                    f"BENCH {method} k={sizes[0]} run={attempt}: "
                    f"{profile['wall_seconds']:.3f}s / {profile['peak_tree_rss_mib']:.1f} MiB",
                    flush=True,
                )

        for method in METHODS:
            execute(method, (1, 2, 3), 0)
        for attempt in range(1, repeats + 1):
            shift = (attempt - 1) % len(METHODS)
            for size in (1, 2, 3):
                for method in METHODS[shift:] + METHODS[:shift]:
                    execute(method, (size,), attempt)
    summary, improvements = aggregate_runs(runs)
    output = results / "benchmarks"
    for name, rows in (("runs", runs), ("summary", summary), ("optimization", improvements)):
        write_csv(output / f"{name}.csv", rows, list(rows[0]))
    write_json(
        output / "environment.json",
        {
            "timestamp_utc": datetime.now(UTC).isoformat(),
            "python": sys.version,
            "platform": platform.platform(),
            "attempts": repeats,
            "chunk_size": 250,
            "workers": 2,
            "duckdb_threads": 2,
            "s3_csv": settings.uri,
            "s3_parquet": settings.parquet_uri,
        },
    )
    return summary
