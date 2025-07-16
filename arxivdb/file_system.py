"""
File system related methods.

Copyright (C) 2025 Nicholas M. Synovic.

"""

from collections.abc import Iterator
from pathlib import Path

import pandas as pd
from pandas import DataFrame


def resolve_path(fp: str) -> Path:
    """
    Resolve the given file path to an absolute path.

    Args:
        fp (str): The file path to resolve.

    Returns:
        Path: The resolved absolute path.

    """
    return Path(fp).resolve()


def is_file(fp: Path) -> bool:
    """
    Check if the given path is a file.

    Args:
        fp (Path): The path to check.

    Returns:
        bool: True if the path is a file, False otherwise.

    """
    return fp.is_file(follow_symlinks=False)


def count_lines(fp: Path) -> int:
    """
    Count the number of lines in a file.

    Args:
        fp (Path): The path to the file.

    Returns:
        int: The number of lines in the file.

    """
    count: int = 0
    with fp.open(encoding="utf-8") as json_file:
        for count, _ in enumerate(iterable=json_file):  # noqa: B007
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
