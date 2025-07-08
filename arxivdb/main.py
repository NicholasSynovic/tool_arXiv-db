from pathlib import Path
from typing import Iterator

from pandas import DataFrame

from arxivdb import cli
from arxivdb.api import file_system


def main() -> None:
    args: dict[str, list[Path]] = cli.CLI().parse_args().__dict__
    dfs: Iterator[DataFrame] = file_system.read_json(fp=args["input"][0])


if __name__ == "__main__":
    main()
