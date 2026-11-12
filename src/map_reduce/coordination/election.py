import threading
from pathlib import PurePosixPath

from kazoo.client import KazooClient, KazooState


class LeadershipLost(RuntimeError):
    pass


class LeaderElection:
    """Выбрать минимальный ephemeral sequential узел, czxid служит эпохой"""

    def __init__(self, client: KazooClient, path: str, identifier: str):
        self.client = client
        self.path = path
        self.identifier = identifier
        self.candidate: str | None = None
        self.lost = threading.Event()
        client.add_listener(self.session_changed)

    def session_changed(self, state: str) -> None:
        if state == KazooState.LOST:
            self.lost.set()

    def join(self) -> None:
        self.client.ensure_path(self.path)
        self.candidate = self.client.create(
            self.path + "/candidate-",
            self.identifier.encode(),
            ephemeral=True,
            sequence=True,
        )

    def leader_epoch(self) -> int | None:
        if self.lost.is_set() or self.candidate is None:
            raise LeadershipLost("Coordinator session expired")
        children = sorted(
            name for name in self.client.get_children(self.path) if name.startswith("candidate-")
        )
        name = PurePosixPath(self.candidate).name
        if name not in children:
            raise LeadershipLost("Election node disappeared")
        if children[0] != name:
            return None
        stat = self.client.exists(self.candidate)
        if stat is None:
            raise LeadershipLost("Election node disappeared")
        return stat.czxid
