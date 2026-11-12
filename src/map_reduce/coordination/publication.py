import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from .checkpoints import PairProgress


@dataclass(frozen=True)
class Publication:
    identifier: int
    epoch: int
    coordinator: str
    progress: PairProgress


class SharedStore:
    """Общий получатель и чекпоинт для двух процессов на одном компьютере.

    Проверка эпохи и запись выполняются под одной блокировкой SQLite.
    Для разных компьютеров потребуется сетевой получатель/общая СУБД.
    """

    def __init__(self, path: Path):
        self.path = path.resolve()

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        try:
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            connection.commit()
        finally:
            connection.close()

    def initialize(self, identity: str, fencing: int) -> None:
        if fencing not in (0, 1):
            raise ValueError("FENCING must be 0 or 1")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.transaction() as connection:
            connection.execute(
                "CREATE TABLE IF NOT EXISTS job ("
                "id INTEGER PRIMARY KEY CHECK(id=1), identity TEXT NOT NULL, "
                "fencing INTEGER NOT NULL, epoch INTEGER NOT NULL, "
                "checkpoint TEXT NOT NULL, done INTEGER NOT NULL)"
            )
            connection.execute(
                "CREATE TABLE IF NOT EXISTS publications ("
                "id INTEGER PRIMARY KEY, epoch INTEGER UNIQUE NOT NULL, "
                "coordinator TEXT NOT NULL, payload TEXT NOT NULL)"
            )
            connection.execute(
                "INSERT OR IGNORE INTO job VALUES (1, ?, ?, 0, ?, 0)",
                (identity, fencing, PairProgress().encode()),
            )
            stored = connection.execute("SELECT identity, fencing FROM job").fetchone()
            if stored != (identity, fencing):
                raise ValueError("Shared job input, chunk size, ZooKeeper path or FENCING differs")

    def claim(self, epoch: int) -> None:
        with self.transaction() as connection:
            changed = connection.execute(
                "UPDATE job SET epoch=? WHERE epoch < ? AND done=0", (epoch, epoch)
            ).rowcount
            if not changed:
                raise ValueError("Leadership epoch is obsolete or job is already complete")

    def load(self) -> PairProgress:
        with self.transaction() as connection:
            payload = connection.execute("SELECT checkpoint FROM job").fetchone()[0]
        return PairProgress.decode(payload)

    def completed(self) -> bool:
        with self.transaction() as connection:
            return bool(connection.execute("SELECT done FROM job").fetchone()[0])

    def save(self, epoch: int, progress: PairProgress) -> bool:
        with self.transaction() as connection:
            current_epoch, payload, done = connection.execute(
                "SELECT epoch, checkpoint, done FROM job"
            ).fetchone()
            if epoch != current_epoch or done:
                return False
            previous = PairProgress.decode(payload)
            if progress.rows <= previous.rows:
                raise ValueError("Checkpoint must advance the committed row offset")
            connection.execute("UPDATE job SET checkpoint=?", (progress.encode(),))
        return True

    def publish(self, epoch: int, coordinator: str, progress: PairProgress) -> int | None:
        with self.transaction() as connection:
            current_epoch, fencing = connection.execute("SELECT epoch, fencing FROM job").fetchone()
            if fencing and epoch != current_epoch:
                return None
            connection.execute(
                "INSERT OR IGNORE INTO publications(epoch, coordinator, payload) VALUES (?, ?, ?)",
                (epoch, coordinator, progress.encode()),
            )
            identifier = connection.execute(
                "SELECT id FROM publications WHERE epoch=?", (epoch,)
            ).fetchone()[0]
            connection.execute("UPDATE job SET done=1")
        return identifier

    def publications(self) -> list[Publication]:
        with self.transaction() as connection:
            rows = connection.execute(
                "SELECT id, epoch, coordinator, payload FROM publications ORDER BY id"
            ).fetchall()
        return [Publication(i, e, c, PairProgress.decode(p)) for i, e, c, p in rows]
