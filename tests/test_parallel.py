import csv
import tempfile
import unittest
from collections import Counter
from pathlib import Path

from map_reduce.infrastructure.files import read_csv, sha256
from map_reduce.parallel import check_manifest, merge_part, split_csv


class ParallelTests(unittest.TestCase):
    def test_split_preserves_order_quotes_newlines_and_duplicate_records(self):
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp)
            source = folder / "source.csv"
            rows = [["id", "text"], ["3", 'quotes, "double"'], ["1", "a\nb"],
                    ["1", "a\nb"], ["4", ""]]
            with source.open("w", newline="") as output:
                csv.writer(output).writerows(rows)
            parts = split_csv(source, folder, 4, 3)
            self.assertEqual([p["rows"] for p in parts], [2, 1, 1])
            self.assertEqual([r for p in parts for r in read_csv(folder / p["file"])],
                             list(read_csv(source)))
            for p in parts:
                self.assertEqual(p["sha256"], sha256(folder / p["file"]))
            for wrong_population in (3, 5):
                with self.assertRaises(ValueError):
                    split_csv(source, folder, wrong_population, 2)

    def test_reducer_requires_distinct_ids_and_combines_shared_pairs(self):
        with tempfile.TemporaryDirectory() as tmp:
            counts = Counter()
            parts = []
            for index, frequency in enumerate([2, 7]):
                path = Path(tmp) / str(index)
                path.write_text(f"skill_1,skill_2,frequency\njava,sql,{frequency}\n")
                part = {"id": index, "rows": frequency, "sha256": str(index),
                        "key": f"test/parts/{index}.csv"}
                marker = dict(id=index, rows=frequency, part_sha256=str(index), sha256=sha256(path),
                              key=f"test/mapped/{index}/{sha256(path)}.csv", occurrences=frequency)
                merge_part(counts, part, marker, path, "test")
                with self.assertRaises(ValueError):
                    merge_part(counts, part, dict(marker, part_sha256="stale"), path, "test")
                parts.append(part)
            self.assertEqual(counts, {("java", "sql"): 9})
            check_manifest(dict(n=2, parts=parts, rows=9), "test")
            for invalid in ([parts[0], parts[0]], [parts[1]]):
                with self.assertRaises(ValueError):
                    check_manifest(dict(n=2, parts=invalid, rows=9), "test")
