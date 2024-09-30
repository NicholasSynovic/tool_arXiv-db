from pathlib import Path
from typing import Iterator

import click
import pandas
from pandas import DataFrame
from progress.spinner import Spinner

from arxiv_db.db import DB


def readJSON(fp: Path, chunksize: int = 10000) -> Iterator[DataFrame]:
    """
    Read a JSON file into chunks of DataFrames.

    This function reads a JSON file into chunks of DataFrames, which can be useful for large files or memory-constrained environments.

    :param fp: The path to the JSON file to read.
    :type fp: Path
    :param chunksize: The number of rows to include in each chunk. Defaults to 10,000.
    :type chunksize: int, optional
    :return: A generator that yields chunks of DataFrames.
    :rtype: Iterator[DataFrame]
    """  # noqa: E501
    return pandas.read_json(
        path_or_buf=fp,
        lines=True,
        chunksize=chunksize,
        engine="ujson",
    )


def getDocuments(df: DataFrame) -> DataFrame:
    """
    Extract a subset of relevant columns from a given DataFrame.

    This function takes a DataFrame as input and returns a new DataFrame containing only the specified columns.

    :param df: The input DataFrame to process.
    :type df: DataFrame
    :return: A new DataFrame with the selected columns, including conversion of 'update_date' to datetime format.
    :rtype: DataFrame
    """  # noqa: E501
    documentsDF: DataFrame = df[
        [
            "id",
            "title",
            "submitter",
            "comments",
            "journal-ref",
            "doi",
            "report-no",
            "categories",
            "license",
            "abstract",
            "update_date",
        ]
    ].copy()

    documentsDF["update_date"] = pandas.to_datetime(
        arg=documentsDF["update_date"],
    )

    return documentsDF


def getAuthors(df: DataFrame, idIncrement: int = 0) -> DataFrame:
    """
    Extract author information from a given DataFrame and assign unique IDs.

    This function takes a DataFrame as input, extracts the 'authors_parsed' column,
    explodes it into separate rows for each author, renames the columns, and assigns
    a new ID to each row based on the specified increment.

    :param df: The input DataFrame containing author information.
    :type df: DataFrame
    :param idIncrement: An integer to increment the IDs by. Defaults to 0.
    :type idIncrement: int, optional
    :return: A new DataFrame with the extracted author information and unique IDs.
    :rtype: DataFrame
    """  # noqa: E501
    authorsDF: DataFrame = df[["id", "authors_parsed"]]

    authorsDF = authorsDF.explode(
        column="authors_parsed",
        ignore_index=True,
    )

    authorsDF["author"] = authorsDF["authors_parsed"].apply(
        lambda x: ", ".join(x),
    )

    authorsDF = authorsDF.drop(columns="authors_parsed")
    authorsDF.index += idIncrement
    authorsDF = authorsDF.reset_index()

    authorsDF = authorsDF.rename(
        columns={
            "id": "document_id",
            "index": "id",
        }
    )

    return authorsDF


def getVersions(df: DataFrame, idIncrement: int = 0) -> DataFrame:
    """
    Extract version information from a given DataFrame and assign unique IDs.

    This function takes a DataFrame as input, extracts the 'versions' column,
    explodes it into separate rows for each version, extracts relevant fields ('version', 'created'),
    converts the 'created' field to datetime format, renames the columns, and assigns
    a new ID to each row based on the specified increment.

    :param df: The input DataFrame containing version information.
    :type df: DataFrame
    :param idIncrement: An integer to increment the IDs by. Defaults to 0.
    :type idIncrement: int, optional
    :return: A new DataFrame with the extracted version information and unique IDs.
    :rtype: DataFrame
    """  # noqa: E501
    versionsDF: DataFrame = df[["id", "versions"]]
    versionsDF = versionsDF.explode(column="versions", ignore_index=True)

    versionsDF["version"] = versionsDF["versions"].apply(
        lambda x: x["version"],
    )

    versionsDF["created"] = versionsDF["versions"].apply(
        lambda x: x["created"],
    )

    versionsDF["created"] = pandas.to_datetime(
        arg=versionsDF["created"],
        format=r"%a, %d %b %Y %H:%M:%S %Z",
    )

    versionsDF = versionsDF.drop(columns="versions")
    versionsDF.index += idIncrement
    versionsDF = versionsDF.reset_index()

    versionsDF = versionsDF.rename(
        columns={
            "id": "document_id",
            "index": "id",
        },
    )

    return versionsDF


def loadData(dfs: Iterator[DataFrame], db: DB) -> None:
    """
    Load data from an iterator of DataFrames into a database.

    This function takes an iterator of DataFrames and a database object as input,
    processes each DataFrame to extract author, version, and document information,
    and loads the data into the corresponding tables in the database.

    :param dfs: An iterator yielding DataFrames containing document metadata.
    :type dfs: Iterator[DataFrame]
    :param db: A database object with methods for loading data into SQL tables.
    :type db: DB
    """  # noqa: E501
    authorIDIncrement: int = 0
    versionsIDIncrement: int = 0

    with Spinner(f"Loading data into {db.path}... ") as spinner:
        df: DataFrame
        for df in dfs:
            authorsDF: DataFrame = getAuthors(
                df=df,
                idIncrement=authorIDIncrement,
            )
            versionsDF: DataFrame = getVersions(
                df=df,
                idIncrement=versionsIDIncrement,
            )
            documentsDF: DataFrame = getDocuments(df=df)

            db.toSQL(tableName=db.documentTable, df=documentsDF)
            authorRows: int = db.toSQL(tableName=db.authorTable, df=authorsDF)

            versionRows: int = db.toSQL(
                tableName=db.versionTable,
                df=versionsDF,
            )

            authorIDIncrement += authorRows
            versionsIDIncrement += versionRows

            spinner.next()


@click.command()
@click.option(
    "-i",
    "--input",
    "inputPath",
    type=click.Path(
        exists=True,
        file_okay=True,
        readable=True,
        resolve_path=True,
        path_type=Path,
    ),
    required=True,
    help="Path to a JSON Lines arXiv metadata file",
)
@click.option(
    "-o",
    "--output",
    "outputPath",
    type=click.Path(
        exists=False,
        file_okay=True,
        writable=True,
        resolve_path=True,
        path_type=Path,
    ),
    required=True,
    help="Path to store SQLite3 database",
)
def main(inputPath: Path, outputPath: Path) -> None:
    jr: Iterator[DataFrame] = readJSON(fp=inputPath)

    db: DB = DB(path=outputPath)

    loadData(dfs=jr, db=db)


if __name__ == "__main__":
    main()
