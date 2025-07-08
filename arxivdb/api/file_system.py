from pathlib import Path
from typing import Iterator

import pandas as pd
from pandas import DataFrame


def resolve_path(fp: str) -> Path:
    return Path(fp).resolve()


def is_file(fp: Path) -> bool:
    return fp.is_file(follow_symlinks=False)


def count_lines(fp: Path) -> int:
    count: int = 0
    for count, _ in enumerate(iterable=open(file=fp, mode="r")):
        pass

    return count + 1


def read_json(fp: Path, chunksize: int = 10000) -> Iterator[DataFrame]:
    """
    Read a JSON file into chunks of DataFrames.

    This function reads a JSON file into chunks of DataFrames, which can be
    useful for large files or memory-constrained environments.

    Args:
        fp (Path): The path to the JSON file to read.
        chunksize (int, optional): The number of rows to include in each chunk.
            Defaults to 10,000.

    Returns:
        Iterator[DataFrame]: A generator that yields chunks of DataFrames.

    """
    return pd.read_json(
        path_or_buf=fp,
        lines=True,
        chunksize=chunksize,
        engine="ujson",
    )
