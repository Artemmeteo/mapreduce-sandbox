import json
from pathlib import Path
from tempfile import TemporaryDirectory

from ..infrastructure.files import write_json
from ..profiling.runner import job_command, profile_process
from .counts import verify_counts


def test_mapper_failure(results: Path) -> dict:
    with TemporaryDirectory(prefix="java-lab-failure-") as temporary:
        root = Path(temporary)
        failed = root / "failed"
        log = root / "failure.log"
        command = job_command("optimized", failed, (1, 2, 3))
        failure = profile_process(
            command + ["--failure-chunk", "1", "--retries", "0"], log, check=False
        )
        if failure["exit_code"] == 0 or list(failed.glob("counts_k*.csv")):
            raise ValueError("Failed mapper unexpectedly published successful results")
        if "Injected mapper failure: chunk 1" not in log.read_text():
            raise ValueError("Job failed for a reason other than the injected mapper exception")
        recovered = root / "recovered"
        command = job_command("optimized", recovered, (1, 2, 3))
        profile_process(command + ["--failure-chunk", "1"], log)
        verification = verify_counts(results / "mapreduce", recovered)
        run = json.loads((recovered / "run.json").read_text())
        if run["retries"] != 1:
            raise ValueError("The injected failure must cause exactly one retry")
        result = {
            "passed": True,
            "injected_chunk": 1,
            "no_retry_exit_code": failure["exit_code"],
            "partial_results_published": False,
            "recovery_retries": run["retries"],
            "exact_match_after_retry": verification["passed"],
            "failure_model": "mapper task exception; worker/host termination not simulated",
        }
    write_json(results / "checks" / "mapper_failure.json", result)
    return result
