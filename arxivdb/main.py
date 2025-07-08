import math
from pathlib import Path
from typing import Iterator

from pandas import DataFrame
from progress.bar import Bar

from arxivdb import cli
from arxivdb.api import data, file_system

JSON_CHUNKSIZE: int = 10000


def main() -> None:
    args: dict[str, list[Path]] = cli.CLI().parse_args().__dict__
    input_fp: Path = args["input"][0]

    line_count: int = file_system.count_lines(fp=input_fp)
    dfs_count: int = math.ceil(line_count / JSON_CHUNKSIZE)

    dfs: Iterator[DataFrame] = file_system.read_json(
        fp=args["input"][0],
        chunksize=JSON_CHUNKSIZE,
    )

    with Bar("Iterating JSON document in chunks...", max=dfs_count) as bar:
        df: DataFrame
        for df in dfs:
            author_data: DataFrame = data.get_authors(df=df)
            document_data: DataFrame = data.get_documents(df=df)
            document_version_data: DataFrame = data.get_document_versions(df=df)
            bar.next()


if __name__ == "__main__":
    main()
