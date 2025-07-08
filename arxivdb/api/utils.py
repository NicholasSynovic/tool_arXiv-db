from pathlib import Path


def resolve_path(fp: Path) -> Path:
    return fp.resolve()
