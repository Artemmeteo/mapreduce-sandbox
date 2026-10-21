import argparse
import json
from pathlib import Path

from .config import RESULTS, ROOT, CountOptions, load_settings


def parser() -> argparse.ArgumentParser:
    arguments = argparse.ArgumentParser(description="Java vacancies: laboratory 3, variant 3")
    commands = arguments.add_subparsers(dest="command", required=True)
    commands.add_parser("install-httpfs")
    commands.add_parser("storage-check")
    extract = commands.add_parser("extract")
    extract.add_argument("--source", type=Path, default=ROOT.parent / "it_vacancies_2024.csv")
    extract.add_argument("--out", type=Path, default=RESULTS / "data")
    parquet = commands.add_parser("convert-parquet")
    parquet.add_argument("--out", type=Path, default=RESULTS / "data")
    for name, directory in (("run-mr", "mapreduce"), ("run-duckdb", "duckdb")):
        count = commands.add_parser(name)
        count.add_argument("--out", type=Path, default=RESULTS / directory)
        count.add_argument("--sizes", default="1,2,3")
        if name == "run-duckdb":
            count.add_argument("--format", choices=("csv", "parquet"), default="csv")
        if name == "run-mr":
            count.add_argument("--mode", choices=("baseline", "optimized"), default="optimized")
            count.add_argument("--chunk-size", type=int, default=250)
            count.add_argument("--workers", type=int, default=2)
            count.add_argument("--retries", type=int, default=2)
            count.add_argument("--failure-chunk", type=int)
    verify = commands.add_parser("verify")
    verify.add_argument("left", type=Path, nargs="?", default=RESULTS / "mapreduce")
    verify.add_argument("right", type=Path, nargs="?", default=RESULTS / "duckdb")
    verify.add_argument("--out", type=Path, default=RESULTS / "checks" / "counts.json")
    analysis = commands.add_parser("analyze")
    analysis.add_argument("--out", type=Path, default=RESULTS / "analysis")
    analysis.add_argument("--min-support", type=int, default=20)
    check = commands.add_parser("validate-analysis")
    check.add_argument("--analysis", type=Path, default=RESULTS / "analysis")
    check.add_argument("--out", type=Path, default=RESULTS / "checks" / "analysis.json")
    benchmark = commands.add_parser("benchmark")
    benchmark.add_argument("--repeats", type=int, default=3)
    commands.add_parser("fault-test")
    commands.add_parser("publish")
    return arguments


def dispatch(args: argparse.Namespace):
    if args.command == "verify":
        from .verification.counts import verify_counts

        return verify_counts(args.left, args.right, args.out)
    if args.command == "install-httpfs":
        from .infrastructure.duckdb import install_httpfs

        return {"httpfs_installed": True, "duckdb_version": install_httpfs()}

    settings = load_settings()
    if args.command in {"run-mr", "run-duckdb"}:
        sizes = tuple(int(size) for size in args.sizes.split(","))
        options = CountOptions(
            sizes=sizes,
            **{
                key: getattr(args, key)
                for key in ("mode", "chunk_size", "workers", "retries", "failure_chunk")
                if hasattr(args, key)
            },
        )
        if args.command == "run-duckdb":
            from .processing.duckdb import count_duckdb

            return count_duckdb(settings, args.out, options, args.format)
        from .infrastructure.s3 import S3Storage
        from .processing.mapreduce import count_mapreduce

        return count_mapreduce(S3Storage(settings), args.out, options)
    if args.command == "analyze":
        from .analytics.analysis import analyze_vacancies

        return analyze_vacancies(settings, args.out, args.min_support)
    if args.command == "benchmark":
        from .profiling.runner import benchmark

        return benchmark(settings, RESULTS, args.repeats)
    if args.command == "fault-test":
        from .verification.failure import test_mapper_failure

        return test_mapper_failure(RESULTS)

    from .infrastructure.s3 import S3Storage

    storage = S3Storage(settings)
    if args.command == "convert-parquet":
        from .processing.parquet import convert_to_parquet

        return convert_to_parquet(storage, args.out)
    if args.command == "extract":
        from .processing.extraction import extract_variant

        return extract_variant(args.source, args.out, storage)
    if args.command == "validate-analysis":
        from .verification.analysis import verify_analysis

        return verify_analysis(storage, args.analysis, args.out)
    if args.command == "storage-check":
        storage.ensure_bucket()
        remote = storage.head()
        return {
            "bucket": settings.bucket,
            "object": settings.object_key,
            "bytes": remote["ContentLength"],
            "etag": remote["ETag"],
        }
    if args.command == "publish":
        return {"bucket": settings.bucket, "uploaded_results": storage.publish(RESULTS)}
    raise ValueError(f"Unknown command: {args.command}")


def main() -> None:
    result = dispatch(parser().parse_args())
    print(json.dumps(result, ensure_ascii=False, indent=2, allow_nan=False))


if __name__ == "__main__":
    main()
