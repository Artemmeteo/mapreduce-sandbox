import csv
import tempfile
import unittest
from collections import Counter
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

from map_reduce.config import CountOptions
from map_reduce.infrastructure.files import read_csv
from map_reduce.processing.extraction import extract_variant
from map_reduce.processing.mapreduce import count_mapreduce, map_chunk
from map_reduce.verification.counts import load_counts


class MemoryStorage:
    settings = SimpleNamespace(uri="s3://test/data.csv", object_key="data.csv")

    def __init__(self, rows=()):
        self.data = rows
        self.uploaded = None

    @contextmanager
    def rows(self):
        yield iter(self.data)

    def ensure_bucket(self):
        pass

    def upload(self, path, key):
        self.uploaded = path

    def head(self):
        return {"ContentLength": self.uploaded.stat().st_size, "ETag": "test"}


class ProcessingTests(unittest.TestCase):
    def test_combiner_preserves_every_frequency(self):
        values = ("Java; JAVA; SQL; Docker", "java; SQL", "")
        baseline = map_chunk(values, (1, 2, 3), False)
        optimized = map_chunk(values, (1, 2, 3), True)
        self.assertEqual(Counter(baseline.emitted), optimized.emitted)
        self.assertEqual(optimized.expected, Counter({1: 5, 2: 4, 3: 1}))
        self.assertEqual((optimized.rows, optimized.with_skills), (3, 2))

    def test_failure_is_injected_only_on_first_attempt(self):
        with self.assertRaisesRegex(RuntimeError, "Injected mapper failure"):
            map_chunk(("java",), (1,), True, failure_chunk=0)
        self.assertEqual(map_chunk(("java",), (1,), True, attempt=1, failure_chunk=0).rows, 1)

    def test_process_retry_commits_once_and_respects_chunk_limit(self):
        rows = [
            {"key_skills": text}
            for text in ("java;sql", "sql;docker", "java", "", "java;sql;docker")
        ]
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary)
            result = count_mapreduce(
                MemoryStorage(rows), output, CountOptions(workers=1, chunk_size=2, failure_chunk=1)
            )
            self.assertEqual(result["retries"], 1)
            self.assertEqual(result["tasks"], 3)
            self.assertLessEqual(result["max_inflight"], 2)
            self.assertEqual(
                load_counts(output / "counts_k1.csv", 1),
                {("java",): 3, ("sql",): 3, ("docker",): 2},
            )
            self.assertEqual(
                load_counts(output / "counts_k3.csv", 3), {("docker", "java", "sql"): 1}
            )

    def test_latest_snapshot_is_chosen_before_variant_filter(self):
        fields = [
            "source",
            "vacancy_id",
            "name",
            "role",
            "area",
            "snapshot_date",
            "published_at",
            "key_skills",
        ]
        values = [
            ("a", "Java Developer", "2024-08-01", "java"),
            ("a", "JavaScript Developer", "2024-09-01", "javascript"),
            ("b", "Java\nDeveloper", "2024-09-01", "Java; SQL"),
            ("b", "Java Developer", "2024-08-01", "old"),
            ("c", "Java Developer", "2024-09-01", ""),
        ]
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "source.csv"
            with source.open("w", encoding="utf-8", newline="") as stream:
                writer = csv.DictWriter(stream, fieldnames=fields)
                writer.writeheader()
                for identity, title, snapshot, skills in values:
                    writer.writerow(
                        {
                            "source": "hh",
                            "vacancy_id": identity,
                            "name": title,
                            "role": "Программист, разработчик",
                            "area": "Москва",
                            "snapshot_date": snapshot,
                            "published_at": "2024-01-01",
                            "key_skills": skills,
                        }
                    )
            result = extract_variant(source, root / "output", MemoryStorage())
            selected = list(read_csv(root / "output/java_vacancies.csv"))
            self.assertEqual(result["selected_vacancies"], 2)
            self.assertEqual([row["vacancy_id"] for row in selected], ["b", "c"])
            self.assertEqual(selected[0]["name"], "Java\nDeveloper")
