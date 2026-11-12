import socket
import subprocess
import time
import uuid

from kazoo.client import KazooClient
from kazoo.handlers.threading import KazooTimeoutError

from map_reduce.config import ROOT, load_environment
from map_reduce.coordination.coordinator import zookeeper_hosts

COMPOSE = ["docker", "compose", "-f", str(ROOT / "compose.zookeeper.yaml")]


def compose(action: str, *services: str) -> None:
    subprocess.run([*COMPOSE, action, *services], check=True)


def probe(hosts: str, expected: bool, timeout: float = 40) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        client = KazooClient(hosts=hosts, timeout=10, read_only=False)
        try:
            client.start(timeout=3)
            path = f"/hw4-quorum-probe-{uuid.uuid4().hex}"
            client.create(path, b"quorum")
            client.delete(path)
            available = True
        except KazooTimeoutError:
            available = False
        finally:
            client.stop()
            client.close()
        if available == expected:
            print(f"quorum_write_available={available}", flush=True)
            return
        time.sleep(0.2)
    raise TimeoutError(f"Quorum availability did not become {expected}")


def main() -> None:
    hosts = zookeeper_hosts(load_environment())
    probe(hosts, True)
    roles = []
    for address in hosts.split(","):
        host, port = address.rsplit(":", 1)
        with socket.create_connection((host, int(port)), timeout=3) as stream:
            stream.sendall(b"srvr")
            response = b""
            while block := stream.recv(4096):
                response += block
        role = next(line for line in response.decode().splitlines() if line.startswith("Mode:"))
        roles.append(role)
        print(f"{address} {role}", flush=True)
    if roles.count("Mode: leader") != 1 or roles.count("Mode: follower") != 2:
        raise ValueError("Expected exactly one leader and two followers")
    try:
        compose("stop", "zoo3")
        probe(hosts, True)
        compose("stop", "zoo2")
        probe(hosts, False)
    finally:
        compose("start", "zoo2", "zoo3")
    probe(hosts, True)
    owner = KazooClient(hosts=hosts, timeout=10)
    observer = KazooClient(hosts=hosts, timeout=10)
    try:
        owner.start(timeout=15)
        observer.start(timeout=15)
        path = f"/hw4-ephemeral-{uuid.uuid4().hex}"
        owner.create(path, b"temporary", ephemeral=True)
        if observer.exists(path) is None:
            raise ValueError("Ephemeral node was not created")
        print(f"ephemeral_created={path}", flush=True)
        owner.stop()
        deadline = time.monotonic() + 15
        while observer.exists(path) is not None:
            if time.monotonic() >= deadline:
                raise TimeoutError("Ephemeral node did not disappear")
            time.sleep(0.1)
        print("ephemeral_disappeared=True", flush=True)
    finally:
        owner.stop()
        owner.close()
        observer.stop()
        observer.close()


if __name__ == "__main__":
    main()
