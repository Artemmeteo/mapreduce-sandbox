import json
import tempfile
import unittest
from pathlib import Path

from map_reduce.verification.counts import verify_counts


class VerificationTests(unittest.TestCase):
    def test_missing_skill_rows_cannot_hide_different_populations(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for name, population in (("first", 2), ("second", 3)):
                folder = root / name
                folder.mkdir()
                (folder / "counts_k1.csv").write_text("skill_1,frequency\njava,2\n")
                (folder / "run.json").write_text(json.dumps({"rows": population, "with_skills": 2}))
            with self.assertRaisesRegex(ValueError, "results differ"):
                verify_counts(root / "first", root / "second", sizes=(1,))

    def test_same_totals_do_not_hide_different_keys(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for name, content in (
                ("first", "skill_1,frequency\njava,2\n"),
                ("second", "skill_1,frequency\npython,2\n"),
            ):
                (root / name).mkdir()
                (root / name / "counts_k1.csv").write_text(content)
            with self.assertRaisesRegex(ValueError, "results differ"):
                verify_counts(root / "first", root / "second", root / "check.json", sizes=(1,))
            self.assertTrue((root / "check.json").exists())
