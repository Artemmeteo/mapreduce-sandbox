import ast
from collections import Counter, defaultdict
from itertools import combinations
from pathlib import Path
from statistics import median

from ..config import REGIONS
from ..domain.skills import normalize_skills
from ..domain.vacancy import net_salary
from ..infrastructure.files import read_csv, write_json
from ..infrastructure.s3 import S3Storage


def require_equal(actual, expected, label: str) -> None:
    if actual != expected:
        raise ValueError(f"{label}: expected {expected}, got {actual}")


def verify_salary_effect(row: dict, vacancies: list[tuple[frozenset, float | None]]) -> None:
    stack = set(ast.literal_eval(row["stack"]))
    base = set(ast.literal_eval(row["base_stack"]))
    prices = [price for skills, price in vacancies if stack <= skills and price is not None]
    base_prices = [price for skills, price in vacancies if base <= skills and price is not None]
    without = [
        price
        for skills, price in vacancies
        if base <= skills and not stack <= skills and price is not None
    ]
    require_equal(len(prices), int(row["salary_n"]), "Salary sample size")
    require_equal(median(prices), float(row["median_net_rub"]), "Combination median")
    require_equal(median(base_prices), float(row["base_median_net_rub"]), "Base median")
    require_equal(
        median(prices) - median(base_prices), float(row["delta_rub"]), "Salary difference"
    )
    require_equal(
        median(without) if without else None,
        float(row["base_without_added_median"]) if row["base_without_added_median"] else None,
        "Exclusive base median",
    )
    candidates = list(combinations(sorted(stack), int(row["k"]) - 1))
    frequency = {
        candidate: sum(skills.issuperset(candidate) for skills, _ in vacancies)
        for candidate in candidates
    }
    chosen = min(candidates, key=lambda candidate: (-frequency[candidate], candidate))
    require_equal(tuple(sorted(base)), chosen, "Most frequent base")


def verify_analysis(storage: S3Storage, analysis: Path, output: Path) -> dict:
    frequency = Counter()
    prices = defaultdict(list)
    region_count = Counter()
    region_prices = defaultdict(list)
    vacancies = []
    with storage.rows() as source:
        for raw in source:
            skills = normalize_skills(raw["key_skills"])
            price = net_salary(raw)
            area = raw["area"]
            vacancies.append((frozenset(skills), price))
            region_count[area] += 1
            if price is not None:
                region_prices[area].append(price)
            if area not in REGIONS:
                continue
            for size in (1, 2, 3):
                for combination in combinations(skills, size):
                    key = (area, size, combination)
                    frequency[key] += 1
                    if price is not None:
                        prices[key].append(price)

    regional_checks = 0
    for row in read_csv(analysis / "regions_all_combinations.csv"):
        key = (row["area"], int(row["k"]), tuple(ast.literal_eval(row["stack"])))
        known = prices[key]
        require_equal(frequency[key], int(row["frequency"]), f"Regional frequency: {key}")
        require_equal(len(known), int(row["salary_n"]), f"Regional salary sample: {key}")
        require_equal(
            median(known) if known else None,
            float(row["median_net_rub"]) if row["median_net_rub"] else None,
            f"Regional median: {key}",
        )
        regional_checks += 1
    require_equal(regional_checks, len(frequency), "Regional combination coverage")
    checked_regions = set()
    for row in read_csv(analysis / "regions_summary.csv"):
        area = row["area"]
        checked_regions.add(area)
        require_equal(int(row["vacancies"]), region_count[area], f"Regional vacancy count: {area}")
        require_equal(
            int(row["salary_n"]), len(region_prices[area]), f"Regional salary count: {area}"
        )
        expected = median(region_prices[area]) if region_prices[area] else None
        require_equal(
            float(row["median_net_rub"]) if row["median_net_rub"] else None,
            expected,
            f"Regional median: {area}",
        )

    require_equal(checked_regions, set(REGIONS), "Regional summary coverage")

    salary_checks = 0
    for size in (2, 3):
        for direction in ("increase", "decrease"):
            for raw in read_csv(analysis / f"salary_k{size}_{direction}.csv"):
                verify_salary_effect(raw, vacancies)
                salary_checks += 1
    result = {
        "passed": True,
        "regional_combinations_checked": regional_checks,
        "regions_checked": len(REGIONS),
        "salary_top_rows_checked": salary_checks,
    }
    write_json(output, result)
    return result
