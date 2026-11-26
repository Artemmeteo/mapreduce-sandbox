import hashlib
import json
import sys
from dataclasses import replace

from map_reduce.config import load_settings
from map_reduce.infrastructure.s3 import S3Storage

mode, run_id, expected = sys.argv[1:]
settings = load_settings()
target = S3Storage(settings)
bucket, key = settings.bucket, settings.object_key
control_key = f"kubernetes/storage/{run_id}/control"
control = run_id.encode()


def read(store, key):
    body = store.client.get_object(Bucket=bucket, Key=key)["Body"]
    try:
        return body.read()
    finally:
        body.close()


def digest(data):
    return hashlib.sha256(data).hexdigest()


if mode == "copy":
    source = S3Storage(replace(settings, endpoint="http://host.docker.internal:9010"))
    data = read(source, key)
    if digest(data) != expected:
        raise ValueError("Source differs from HW3 extraction")
    target.ensure_bucket()
    target.client.put_object(Bucket=bucket, Key=key, Body=data)
    target.client.put_object(Bucket=bucket, Key=control_key, Body=control)
data = read(target, key)
if digest(data) != expected or read(target, control_key) != control:
    raise ValueError("Content verification failed")
print(json.dumps(dict(mode=mode, source_endpoint="http://host.docker.internal:9010",
                      target_endpoint=settings.endpoint, bucket=bucket, key=key,
                      bytes=len(data), sha256=digest(data), control_key=control_key,
                      control_sha256=digest(control), control_bytes=len(control))))
