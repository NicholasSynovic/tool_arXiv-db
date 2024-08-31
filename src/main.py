from pathlib import Path
from typing import Iterator

import pandas
from pandas import DataFrame

from src.db import DB


def readJSON(fp: Path, chunksize: int = 10000) -> Iterator[DataFrame]:
    return pandas.read_json(
        path_or_buf=fp,
        lines=True,
        chunksize=chunksize,
        engine="ujson",
    )


def main() -> None:
    fp: Path = Path("../data/arxiv.jsonlines")
    jr: Iterator[DataFrame] = readJSON(fp=fp)

    db: DB = DB(path=Path("test.db"))


if __name__ == "__main__":
    main()
