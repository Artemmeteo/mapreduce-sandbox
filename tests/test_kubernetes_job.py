import io
import tempfile
import unittest
from contextlib import contextmanager
from hashlib import sha256
from pathlib import Path
from types import SimpleNamespace

from map_reduce.kubernetes_job import memory_snapshot, validate_input


class JobTests(unittest.TestCase):
    def test_cgroup_peak_is_read_in_bytes_without_sampling(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name, value in {"memory.peak": "104857600", "memory.current": "62914560",
                                "memory.max": "134217728", "memory.swap.max": "0",
                                "memory.events": "oom 1\noom_kill 1\n"}.items():
                (root / name).write_text(value)
            metrics = memory_snapshot(root)
            self.assertEqual(metrics["memory.peak"], 100 * 1024**2)
            self.assertEqual(metrics["memory.events"]["oom_kill"], 1)

    def test_input_checks_hash_and_csv_records_with_embedded_newline(self):
        data = b'key_skills,name\njava,"line one\nline two"\nsql,job\n'

        class Storage:
            settings = SimpleNamespace(bucket="test", object_key="input", uri="s3://test/input")

            @property
            def client(self):
                return self

            def get_object(self, **kwargs):
                from botocore.response import StreamingBody
                return {"Body": StreamingBody(io.BytesIO(data), len(data)),
                        "ContentLength": len(data)}

            @contextmanager
            def rows(self):
                import csv
                yield csv.DictReader(io.StringIO(data.decode()))

        storage = Storage()
        self.assertEqual(validate_input(storage, sha256(data).hexdigest(), 2)["rows"], 2)
        with self.assertRaisesRegex(ValueError, "Input differs"):
            validate_input(storage, "0" * 64, 2)
        with self.assertRaisesRegex(ValueError, "Input differs"):
            validate_input(storage, sha256(data).hexdigest(), 3)
