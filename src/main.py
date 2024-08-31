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


def getDocuments(df: DataFrame) -> DataFrame:
    return df[
        [
            "id",
            "title",
            "comments",
            "journal-ref",
            "doi",
            "report-no",
            "license",
            "abstract",
            "update_date",
        ]
    ]


def loadData(dfs: Iterator[DataFrame], db: DB) -> None:
    df: DataFrame
    for df in dfs:
        documentsDF: DataFrame = getDocuments(df=df)
        documentsDF.to_sql(
            name=db.documentTable,
            con=db.engine,
            if_exists="append",
            index=False,
        )
        quit()


def main() -> None:
    fp: Path = Path("../data/arxiv.jsonlines")
    jr: Iterator[DataFrame] = readJSON(fp=fp)

    db: DB = DB(path=Path("test.db"))

    loadData(dfs=jr, db=db)


if __name__ == "__main__":
    main()
