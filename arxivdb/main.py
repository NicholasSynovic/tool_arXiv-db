"""
Main entrypoint to `arxivdb`.

Copyright (C) 2025 Nicholas M. Synovic.

"""

import math
from collections.abc import Iterator
from pathlib import Path

from pandas import DataFrame
from progress.bar import Bar

import arxivdb.cli as arxivdb_cli
import arxivdb.data as arxivdb_data
import arxivdb.db as arxivdb_db
import arxivdb.file_system as arxivdb_fs


def main() -> None:
    """
    Read JSON data, process it, and write to a database.

    This function parses command-line arguments to obtain input and output file
    paths, and the chunk size for processing. It then reads the input JSON file
    in chunks, processes the data to extract documents, authors, and versions,
    and writes the processed data to the corresponding tables in the SQLite
    database.

    """
    cli: arxivdb_cli.CLI = arxivdb_cli.CLI()
    args: dict = cli.parse_args().__dict__

    input_fp: Path = args["input"][0]
    output_fp: Path = args["output"][0]
    chunksize: int = args["chunksize"]

    db: arxivdb_db.DB = arxivdb_db.DB(path=output_fp)

    line_count: int = arxivdb_fs.count_lines(fp=input_fp)
    dfs_count: int = math.ceil(line_count / chunksize)

    dfs: Iterator[DataFrame] = arxivdb_fs.read_json(
        fp=input_fp,
        chunksize=chunksize,
    )

    with Bar("Iterating JSON document in chunks...", max=dfs_count) as bar:
        df: DataFrame
        for df in dfs:
            document_data: DataFrame = arxivdb_data.get_documents(df=df)
            author_data: DataFrame = arxivdb_data.get_authors(df=df)
            document_version_data: DataFrame = arxivdb_data.get_document_versions(df=df)

            db.write_table(table_name="documents", df=document_data)
            db.write_table(table_name="authors", df=author_data)
            db.write_table(table_name="versions", df=document_version_data)

            bar.next()


if __name__ == "__main__":
    main()
