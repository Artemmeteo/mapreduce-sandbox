import multiprocessing
from collections import Counter
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, wait
from itertools import combinations, groupby, islice
from math import comb
from pathlib import Path
from typing import NamedTuple

from ..config import CountOptions
from ..domain.skills import normalize_skills
from ..infrastructure.files import write_csv, write_json
from ..infrastructure.s3 import S3Storage


class MapperOutput(NamedTuple):
    emitted: Counter | list
    expected: Counter
    rows: int
    with_skills: int


def map_chunk(values, sizes, combine, task_id=0, attempt=0, failure_chunk=None):
    if task_id == failure_chunk and attempt == 0:
        raise RuntimeError(f"Injected mapper failure: chunk {task_id}")
    emitted = Counter() if combine else []
    expected = Counter()
    with_skills = 0
    for value in values:
        stack = normalize_skills(value)
        with_skills += bool(stack)
        for size in sizes:
            expected[size] += comb(len(stack), size)
            for skills in combinations(stack, size):
                key = (size, skills)
                if combine:
                    emitted[key] += 1
                else:
                    emitted.append(key)
    return MapperOutput(emitted, expected, len(values), with_skills)


def execute_mappers(rows, options: CountOptions):
    values = (row["key_skills"] for row in rows)
    chunks = iter(lambda: tuple(islice(values, options.chunk_size)), ())
    chunks = enumerate(chunks)
    pending = {}
    exhausted = False
    max_inflight = 0
    with ProcessPoolExecutor(
        max_workers=options.workers, mp_context=multiprocessing.get_context("spawn")
    ) as pool:

        def submit(task_id, chunk, attempt):
            future = pool.submit(
                map_chunk,
                chunk,
                options.sizes,
                options.mode == "optimized",
                task_id,
                attempt,
                options.failure_chunk,
            )
            pending[future] = (task_id, chunk, attempt)

        while not exhausted or pending:
            while not exhausted and len(pending) < 2 * options.workers:
                try:
                    task_id, chunk = next(chunks)
                except StopIteration:
                    exhausted = True
                    break
                submit(task_id, chunk, 0)
            max_inflight = max(max_inflight, len(pending))
            if not pending:
                break
            completed, _ = wait(pending, return_when=FIRST_COMPLETED)
            for future in completed:
                task_id, chunk, attempt = pending.pop(future)
                try:
                    output = future.result()
                except Exception as error:
                    if attempt >= options.retries:
                        raise RuntimeError(
                            f"Mapper {task_id} failed after {attempt + 1} attempt(s)"
                        ) from error
                    print(f"RETRY mapper={task_id} attempt={attempt + 2}", flush=True)
                    submit(task_id, chunk, attempt + 1)
                    continue
                yield output, attempt, max_inflight


def count_mapreduce(storage: S3Storage, output: Path, options: CountOptions) -> dict:
    reduced = Counter()
    intermediate = []
    expected = Counter()
    rows = with_skills = retries = max_inflight = shuffle_items = tasks = 0
    with storage.rows() as source:
        for result, attempt, inflight in execute_mappers(source, options):
            tasks += 1
            rows += result.rows
            with_skills += result.with_skills
            retries += attempt
            max_inflight = max(max_inflight, inflight)
            shuffle_items += len(result.emitted)
            expected.update(result.expected)
            if options.mode == "optimized":
                reduced.update(result.emitted)
            else:
                intermediate.extend(result.emitted)
    if options.mode == "baseline":
        intermediate.sort()
        for key, occurrences in groupby(intermediate):
            reduced[key] = sum(1 for _ in occurrences)
        del intermediate

    controls = []
    for size in options.sizes:
        counts = sorted(
            ((skills, count) for (k, skills), count in reduced.items() if k == size),
            key=lambda item: (-item[1], item[0]),
        )
        total = sum(count for _, count in counts)
        if total != expected[size]:
            raise ValueError("Frequency total differs from sum of binomial coefficients")
        controls.append(
            {
                "k": size,
                "unique_combinations": len(counts),
                "total_occurrences": total,
                "expected_sum_binomial": expected[size],
            }
        )
        fields = [f"skill_{position}" for position in range(1, size + 1)] + ["frequency"]
        write_csv(
            output / f"counts_k{size}.csv",
            (dict(zip(fields, (*skills, count))) for skills, count in counts),
            fields,
        )
    result = {
        "engine": "mapreduce",
        "mode": options.mode,
        "s3_uri": storage.settings.uri,
        "rows": rows,
        "with_skills": with_skills,
        "controls": controls,
        "chunk_size": options.chunk_size,
        "workers": options.workers,
        "tasks": tasks,
        "max_inflight": max_inflight,
        "retries": retries,
        "shuffle_items": shuffle_items,
    }
    write_json(output / "run.json", result)
    return result
