def normalize_skills(value: str) -> tuple[str, ...]:
    return tuple(sorted({part.strip().lower() for part in value.split(";") if part.strip()}))
