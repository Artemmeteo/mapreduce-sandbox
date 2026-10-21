import unittest

from map_reduce.config import CountOptions, S3Settings
from map_reduce.domain.skills import normalize_skills
from map_reduce.domain.vacancy import belongs_to_variant, net_salary


class DomainTests(unittest.TestCase):
    def test_java_title_and_role_boundaries(self):
        for title in ("Java-разработчик", "Senior JAVA Developer", "Java 11", "Java/Kotlin"):
            self.assertTrue(belongs_to_variant({"name": title, "role": "Программист, разработчик"}))
        for title, role in (
            ("JavaScript Developer", "Программист, разработчик"),
            ("QA Java", "Тестировщик"),
        ):
            self.assertFalse(belongs_to_variant({"name": title, "role": role}))

    def test_unique_skills_keep_punctuation_and_distinct_names(self):
        self.assertEqual(
            normalize_skills(" Java ; JAVA; SQL;; C++; C# "), ("c#", "c++", "java", "sql")
        )
        self.assertEqual(normalize_skills(" ; "), ())
        self.assertEqual(len(normalize_skills("Spring;Spring Framework;ООП;OOP")), 4)

    def test_salary_policy(self):
        base = {"salary_from": "100", "salary_to": "200", "currency": "RUR", "gross": "false"}
        self.assertEqual(net_salary(base), 150)
        self.assertEqual(net_salary({**base, "salary_from": ""}), 200)
        for changes in (
            {"salary_from": "", "salary_to": ""},
            {"salary_from": "300"},
            {"salary_from": "-1"},
            {"salary_from": "inf"},
            {"salary_from": "nan"},
            {"gross": "true"},
            {"currency": "USD"},
        ):
            self.assertIsNone(net_salary({**base, **changes}))

    def test_invalid_calculation_options(self):
        for changes in (
            {"sizes": ()},
            {"sizes": (1, 1)},
            {"sizes": (4,)},
            {"chunk_size": 0},
            {"workers": 0},
            {"retries": -1},
            {"failure_chunk": -1},
        ):
            with self.assertRaises(ValueError):
                CountOptions(**changes)

    def test_credentials_are_not_printed(self):
        settings = S3Settings(access_key="test-access", secret_key="test-secret")
        self.assertNotIn("test-secret", repr(settings))
        self.assertNotIn("test-access", repr(settings))
