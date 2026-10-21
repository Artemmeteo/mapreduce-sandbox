from pathlib import Path
import os
import secrets

project = Path(__file__).resolve().parents[1]
base = project.parent
path = project / ".env"
if not path.exists():
    path.write_text(
        f"LOCAL_UID={os.getuid()}\nLOCAL_GID={os.getgid()}\n"
        f"S3_ACCESS_KEY=HW3JAVA{secrets.token_hex(6).upper()}\n"
        f"S3_SECRET_KEY={secrets.token_hex(24)}\n"
        "S3_ENDPOINT=http://127.0.0.1:9010\n"
    )
    path.chmod(0o600)
for rel in ("storage/vol1", "storage/vol2", "storage/vol3", "storage/vol4", "logs", "downloads"):
    (base / rel).mkdir(parents=True, exist_ok=True)
print("Local settings and directories are ready")
