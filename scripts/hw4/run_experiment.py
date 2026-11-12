import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

from map_reduce.config import RESULTS, ROOT, load_environment
from map_reduce.coordination.coordinator import settings_from_env
from map_reduce.coordination.publication import SharedStore
from map_reduce.infrastructure.files import write_json
from map_reduce.verification.counts import verify_counts


def records(path: Path) -> list[dict]:
    if not path.exists():
        return []
    values = []
    for line in path.read_text(encoding="utf-8").splitlines():
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue  # The other process may currently be appending its last line.
        values.append(value)
    return values


def await_event(path: Path, name: str, process: subprocess.Popen, timeout: float) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        matches = [item for item in records(path) if item["event"] == name]
        if matches:
            return matches[-1]
        if process.poll() is not None:
            raise RuntimeError(f"Coordinator exited before {name}: {process.returncode}; {path}")
        time.sleep(0.05)
    raise TimeoutError(f"No {name} event in {path} after {timeout} seconds")


def main() -> None:
    os.chdir(ROOT)
    os.environ.setdefault("COORDINATOR_ID", "c1")
    settings = settings_from_env()
    if settings.chunk_delay <= 0:
        raise ValueError("CHUNK_DELAY must be positive for the pause experiment")
    pause = float(load_environment().get("PAUSE_SECONDS", "40"))
    if not 30 <= pause <= 300:
        raise ValueError("PAUSE_SECONDS must be between 30 and 300")
    settings.output.mkdir(parents=True, exist_ok=False)
    log_root = ROOT / "logs/hw4" / settings.run_id
    log_root.mkdir(parents=True, exist_ok=False)
    processes: list[subprocess.Popen] = []
    frozen: subprocess.Popen | None = None
    try:
        first = subprocess.Popen(
            [sys.executable, "-m", "map_reduce.coordination.coordinator"],
            env={**os.environ, "COORDINATOR_ID": "c1"},
        )
        processes.append(first)
        first_log = log_root / "c1.jsonl"
        await_event(first_log, "CHECKPOINT", first, 30)
        first.send_signal(signal.SIGSTOP)
        frozen = first
        stopped = time.monotonic()
        checkpoint = [r for r in records(first_log) if r["event"] == "CHECKPOINT"][-1]
        print(f"SIGSTOP pid={first.pid} checkpoint_rows={checkpoint['rows']}", flush=True)
        second = subprocess.Popen(
            [sys.executable, "-m", "map_reduce.coordination.coordinator"],
            env={**os.environ, "COORDINATOR_ID": "c2"},
        )
        processes.append(second)
        second_log = log_root / "c2.jsonl"
        takeover = await_event(second_log, "LEADER", second, 30)
        restored = await_event(second_log, "RESTORED", second, 5)
        if restored["rows"] != checkpoint["rows"] or restored["rows"] == 0:
            raise ValueError("Standby did not restore the committed nonempty checkpoint")
        await_event(second_log, "PUBLICATION_ACCEPTED", second, 180)
        second.wait(timeout=15)
        if second.returncode:
            raise RuntimeError(f"Standby exited with {second.returncode}")
        while time.monotonic() - stopped < pause:
            time.sleep(max(0, min(0.2, pause - (time.monotonic() - stopped))))
        actual_pause = time.monotonic() - stopped
        first.send_signal(signal.SIGCONT)
        frozen = None
        print(f"SIGCONT pid={first.pid} pause_seconds={actual_pause:.3f}", flush=True)
        first.wait(timeout=180)
        if first.returncode:
            raise RuntimeError(f"Resumed coordinator exited with {first.returncode}")
        store = SharedStore(settings.database)
        publications = store.publications()
        expected = 1 if settings.fencing else 2
        if len(publications) != expected:
            raise ValueError(f"Expected {expected} publications, got {len(publications)}")
        expected_event = "PUBLICATION_REJECTED" if settings.fencing else "PUBLICATION_ACCEPTED"
        if not any(r["event"] == expected_event for r in records(first_log)):
            raise ValueError("Missing evidence of the resumed coordinator's write")
        for publication in publications:
            folder = settings.output / f"publication-{publication.identifier:02d}"
            # Rebuild exports from committed receiver state if an earlier export was interrupted.
            publication.progress.export(folder)
            verify_counts(RESULTS / "mapreduce", folder, folder / "verification.json", sizes=(2,))
        result = {
            "run_id": settings.run_id,
            "fencing": settings.fencing,
            "session_timeout_seconds": 10,
            "pause_seconds": actual_pause,
            "failover_seconds": takeover["monotonic"] - stopped,
            "failover_measurement": "SIGSTOP sent -> standby registered receiver epoch",
            "checkpoint_rows": checkpoint["rows"],
            "restored_rows": restored["rows"],
            "accepted_publications": len(publications),
            "resumed_write": expected_event,
            "exact_match_hw3_pairs": True,
        }
        write_json(settings.output / "experiment.json", result)
        print(json.dumps(result, ensure_ascii=False, indent=2))
    finally:
        if frozen is not None:
            frozen.send_signal(signal.SIGCONT)
        for process in processes:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()


if __name__ == "__main__":
    main()
