import math
import re

JAVA_TITLE = re.compile(r"(^|[^a-z])java([^a-z]|$)", re.IGNORECASE | re.ASCII)
DEVELOPER_ROLE = "Программист, разработчик"


def belongs_to_variant(row: dict) -> bool:
    return row["role"].strip() == DEVELOPER_ROLE and bool(JAVA_TITLE.search(row["name"]))


def net_salary(row: dict) -> float | None:
    if row.get("currency") not in {"RUR", "RUB"} or row.get("gross", "").lower() != "false":
        return None
    bounds = []
    for key in ("salary_from", "salary_to"):
        try:
            value = float(row.get(key, ""))
        except (ValueError, TypeError):
            continue
        if not math.isfinite(value) or value <= 0:
            return None
        bounds.append(value)
    if not bounds or (len(bounds) == 2 and bounds[0] > bounds[1]):
        return None
    return sum(bounds) / len(bounds)
