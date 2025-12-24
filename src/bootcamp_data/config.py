from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Paths:
    root: Path
    raw: Path
    processed: Path
    cache: Path
    external: Path
    reports: Path
    figures: Path


def make_paths(root: Path) -> Paths:
    data = root / "data"
    reports = data / "reports"
    return Paths(
        root=root,
        raw=data / "raw",
        processed=data / "processed",
        cache=data / "cache",
        external=data / "external",
        reports=reports,
        figures=reports / "figures",
    )
