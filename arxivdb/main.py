"""
Main entrypoint to `arxivdb`.

Copyright (C) 2025 Nicholas M. Synovic.

"""

import math
from collections import defaultdict
from collections.abc import Iterator
from pathlib import Path

import pandas as pd
from pandas import DataFrame, Series
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
    json_chunk_count: int = math.ceil(line_count / chunksize)

    json_chunks: Iterator[DataFrame] = arxivdb_fs.read_json(
        fp=input_fp,
        chunksize=chunksize,
    )

    unique_attributes: dict[str, list[Series]] = defaultdict(list)
    documents: list[DataFrame] = []
    document_versions: list[DataFrame] = []

    with Bar(
        "Iterating JSON document in chunks...",
        max=json_chunk_count,
    ) as bar:
        json_chunk: DataFrame
        for json_chunk in json_chunks:
            # Get the unique categories in this JSON chunk
            unique_attributes["categories"].append(
                arxivdb_data.get_unique_categories(
                    json_chunk=json_chunk,
                )
            )

            # Get the unique submitters in this JSON chunk
            unique_attributes["submitters"].append(
                arxivdb_data.get_unique_submitters(
                    json_chunk=json_chunk,
                )
            )

            # Get the unique authors in this JSON chunk
            unique_attributes["authors"].append(
                arxivdb_data.get_unique_authors(
                    json_chunk=json_chunk,
                )
            )

            # Get the versions per document in this JSON chunk
            document_versions.append(
                arxivdb_data.get_document_versions(json_chunk=json_chunk)
            )

            # Get the documents in this JSON chunk
            documents.append(
                arxivdb_data.get_documents(
                    json_chunk=json_chunk,
                )
            )

            bar.next()


if __name__ == "__main__":
    main()
