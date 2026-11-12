import json
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path

from ..infrastructure.files import write_json
from ..processing.mapreduce import map_chunk, write_counts


@dataclass
class PairProgress:
    rows: int = 0
    with_skills: int = 0
    chunks: int = 0
    expected: int = 0
    counts: Counter[tuple[str, ...]] = field(default_factory=Counter)

    def add(self, values: tuple[str, ...]) -> None:
        mapped = map_chunk(values, (2,), True)
        self.rows += mapped.rows
        self.with_skills += mapped.with_skills
        self.chunks += 1
        self.expected += mapped.expected[2]
        self.counts.update({skills: count for (_, skills), count in mapped.emitted.items()})

    def encode(self) -> str:
        return json.dumps(
            {
                "rows": self.rows,
                "with_skills": self.with_skills,
                "chunks": self.chunks,
                "expected": self.expected,
                "counts": [[*skills, count] for skills, count in sorted(self.counts.items())],
            },
            ensure_ascii=False,
        )

    @classmethod
    def decode(cls, payload: str) -> "PairProgress":
        value = json.loads(payload)
        return cls(
            rows=value["rows"],
            with_skills=value["with_skills"],
            chunks=value["chunks"],
            expected=value["expected"],
            counts=Counter({tuple(row[:2]): row[2] for row in value["counts"]}),
        )

    def export(self, output: Path) -> None:
        controls = write_counts(
            output,
            Counter({(2, skills): count for skills, count in self.counts.items()}),
            Counter({2: self.expected}),
            (2,),
        )
        write_json(
            output / "run.json",
            {
                "engine": "mapreduce-zookeeper",
                "rows": self.rows,
                "with_skills": self.with_skills,
                "tasks": self.chunks,
                "controls": controls,
            },
        )
