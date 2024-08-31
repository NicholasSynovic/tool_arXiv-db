from pathlib import Path
from typing import Iterator

import click
import pandas
from pandas import DataFrame
from progress.spinner import Spinner

from src.db import DB


def readJSON(fp: Path, chunksize: int = 10000) -> Iterator[DataFrame]:
    return pandas.read_json(
        path_or_buf=fp,
        lines=True,
        chunksize=chunksize,
        engine="ujson",
    )


def getDocuments(df: DataFrame) -> DataFrame:
    documentsDF: DataFrame = df[
        [
            "id",
            "title",
            "submitter",
            "comments",
            "journal-ref",
            "doi",
            "report-no",
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
