import json
import logging
import os
import re
import time
from collections.abc import Mapping
from dataclasses import dataclass
from itertools import islice
from pathlib import Path

from kazoo.client import KazooClient
from kazoo.retry import KazooRetry

from ..config import ROOT, load_environment, load_settings
from ..infrastructure.s3 import S3Storage
from .election import LeaderElection
from .publication import SharedStore

LOGGER = logging.getLogger(__name__)
ZK_PORTS = (22181, 22182, 22183)


def zookeeper_hosts(values: Mapping[str, str]) -> str:
    if "ZK_HOSTS" in values:
        return values["ZK_HOSTS"]
    return ",".join(
        f"127.0.0.1:{values.get(f'ZK_PORT_{number}', str(default))}"
        for number, default in enumerate(ZK_PORTS, start=1)
    )


@dataclass(frozen=True)
class CoordinatorSettings:
    run_id: str
    identifier: str
    hosts: str = zookeeper_hosts({})
    namespace: str = "/mapreduce-hw4"
    chunk_size: int = 250
    chunk_delay: float = 0.5
    fencing: int = 1

    def __post_init__(self) -> None:
        for value in (self.run_id, self.identifier):
            if not re.fullmatch(r"[a-zA-Z0-9_-]+", value):
                raise ValueError("Run/coordinator names must contain only letters, digits, _ or -")
        if self.chunk_size < 1 or not 0 <= self.chunk_delay <= 60:
            raise ValueError("Positive CHUNK_SIZE and CHUNK_DELAY between 0 and 60 required")
        if self.fencing not in (0, 1):
            raise ValueError("FENCING must be 0 or 1")
        if not self.namespace.startswith("/") or self.namespace.endswith("/"):
            raise ValueError("ZK_NAMESPACE must be an absolute path without trailing slash")

    @property
    def election_path(self) -> str:
        return f"{self.namespace}/{self.run_id}/election"

    @property
    def output(self) -> Path:
        return ROOT / "results/hw4/experiments" / self.run_id

    @property
    def database(self) -> Path:
        return self.output / "shared.sqlite3"


def settings_from_env() -> CoordinatorSettings:
    values = load_environment()
    return CoordinatorSettings(
        run_id=values["RUN_ID"],
        identifier=values["COORDINATOR_ID"],
        hosts=zookeeper_hosts(values),
        namespace=values.get("ZK_NAMESPACE", CoordinatorSettings.namespace),
        chunk_size=int(values.get("CHUNK_SIZE", str(CoordinatorSettings.chunk_size))),
        chunk_delay=float(values.get("CHUNK_DELAY", str(CoordinatorSettings.chunk_delay))),
        fencing=int(values.get("FENCING", str(CoordinatorSettings.fencing))),
    )


def event(name: str, **values: str | int | float) -> None:
    LOGGER.info(
        json.dumps(
            {"event": name, "time": time.time(), "monotonic": time.monotonic(), **values},
            ensure_ascii=False,
        )
    )


def input_identity(storage: S3Storage, settings: CoordinatorSettings) -> str:
    head = storage.head()
    return json.dumps(
        {
            "uri": storage.settings.uri,
            "endpoint": storage.settings.endpoint,
            "etag": head["ETag"],
            "bytes": head["ContentLength"],
            "version": head.get("VersionId"),
            "chunk_size": settings.chunk_size,
            "election_path": settings.election_path,
        },
        sort_keys=True,
    )


def calculate(
    storage: S3Storage, store: SharedStore, settings: CoordinatorSettings, epoch: int
) -> None:
    identity = input_identity(storage, settings)
    store.initialize(identity, settings.fencing)
    progress = store.load()
    event("RESTORED", epoch=epoch, rows=progress.rows, chunks=progress.chunks)
    with storage.rows(etag=json.loads(identity)["etag"]) as rows:
        for _ in range(progress.rows):
            if next(rows, None) is None:
                raise ValueError("Checkpoint row offset exceeds input length")
        values = (row["key_skills"] for row in rows)
        for chunk in iter(lambda: tuple(islice(values, settings.chunk_size)), ()):
            progress.add(chunk)
            saved = store.save(epoch, progress)
            event(
                "CHECKPOINT" if saved else "CHECKPOINT_REJECTED",
                epoch=epoch,
                rows=progress.rows,
                chunks=progress.chunks,
            )
            # Keep obsolete in-flight work running in both educational experiments.
            # Only the receiver may decide whether its final write is allowed.
            time.sleep(settings.chunk_delay)
    store.initialize(input_identity(storage, settings), settings.fencing)
    identifier = store.publish(epoch, settings.identifier, progress)
    if identifier is None:
        event("PUBLICATION_REJECTED", epoch=epoch, rows=progress.rows)
        return
    event("PUBLICATION_ACCEPTED", epoch=epoch, publication=identifier, rows=progress.rows)
    progress.export(settings.output / f"publication-{identifier:02d}")


def run(settings: CoordinatorSettings) -> None:
    log_path = ROOT / "logs/hw4" / settings.run_id / f"{settings.identifier}.jsonl"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    handler = logging.FileHandler(log_path, mode="x", encoding="utf-8")
    LOGGER.addHandler(handler)
    LOGGER.setLevel(logging.INFO)
    client = KazooClient(
        hosts=settings.hosts,
        timeout=10.0,
        connection_retry=KazooRetry(max_tries=3, max_delay=1),
        command_retry=KazooRetry(max_tries=3, max_delay=1),
    )
    election = LeaderElection(client, settings.election_path, settings.identifier)
    client.add_listener(lambda state: event("SESSION_STATE", state=state))
    event("STARTED", coordinator=settings.identifier, pid=os.getpid(), session_timeout=10)
    try:
        storage = S3Storage(load_settings())
        store = SharedStore(settings.database)
        store.initialize(input_identity(storage, settings), settings.fencing)
        client.start(timeout=15)
        election.join()
        while not store.completed():
            epoch = election.leader_epoch()
            if epoch is None:
                time.sleep(0.2)
                continue
            store.claim(epoch)
            event("LEADER", epoch=epoch, pid=os.getpid())
            calculate(storage, store, settings, epoch)
            return
        event("ALREADY_COMPLETED")
    except Exception as error:
        event("FAILED", error_type=type(error).__name__)
        raise
    finally:
        client.stop()
        client.close()
        handler.close()
        LOGGER.removeHandler(handler)


if __name__ == "__main__":
    run(settings_from_env())
