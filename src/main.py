from pathlib import Path
from typing import Iterator

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
    return df[
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
    ]


def getAuthors(df: DataFrame, idIncrement: int = 0) -> DataFrame:
    authorsDF: DataFrame = df[["id", "authors_parsed"]]
    authorsDF = authorsDF.explode(column="authors_parsed", ignore_index=True)
    authorsDF["author"] = authorsDF["authors_parsed"].apply(
        lambda x: ", ".join(x),
    )

    authorsDF = authorsDF.drop(columns="authors_parsed")
    authorsDF.index += idIncrement
    authorsDF = authorsDF.reset_index()
    authorsDF = authorsDF.rename(columns={"id": "document_id", "index": "id"})

    return authorsDF


def loadData(dfs: Iterator[DataFrame], db: DB) -> None:
    authorIDIncrement: int = 0

    with Spinner(f"Loading data into {db.path}... ") as spinner:
        df: DataFrame
        for df in dfs:
            authorsDF: DataFrame = getAuthors(
                df=df,
                idIncrement=authorIDIncrement,
            )
            documentsDF: DataFrame = getDocuments(df=df)
            db.toSQL(tableName=db.documentTable, df=documentsDF)
            authorRows: int = db.toSQL(tableName=db.authorTable, df=authorsDF)

            authorIDIncrement += authorRows

            spinner.next()


def main() -> None:
    fp: Path = Path("../data/arxiv.jsonlines")
    jr: Iterator[DataFrame] = readJSON(fp=fp)

    db: DB = DB(path=Path("test.db"))

    loadData(dfs=jr, db=db)


if __name__ == "__main__":
    main()
