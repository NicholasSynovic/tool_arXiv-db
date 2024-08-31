from pathlib import Path
from typing import List

from pandas import DataFrame
from sqlalchemy import (
    Column,
    Engine,
    MetaData,
    PrimaryKeyConstraint,
    String,
    Table,
    create_engine,
)
from sqlalchemy.exc import IntegrityError


class DB:
    def __init__(self, path: Path) -> None:
        self.path: Path = path
        self.engine: Engine = create_engine(url=f"sqlite:///{path}")
        self.metadata: MetaData = MetaData()

        self.documentTable: str = "documents"

        self.createTables()

    def createTables(self) -> None:
        _: Table = Table(
            self.documentTable,
            self.metadata,
            Column("id", String),
            Column("title", String),
            Column("comments", String),
            Column("journal-ref", String),
            Column("doi", String),
            Column("report-no", String),
            Column("license", String),
            Column("abstract", String),
            Column("update_date", String),
            PrimaryKeyConstraint("id"),
        )

        self.metadata.create_all(bind=self.engine, checkfirst=True)

    def toSQL(self, tableName: str, df: DataFrame) -> None:
        try:
            df.to_sql(
                name=tableName,
                con=self.engine,
                if_exists="append",
                index=False,
            )
        except IntegrityError as error:
            ids: List[str] = [param[0] for param in error.params]
            uniqueDF: DataFrame = df[~df["id"].isin(values=ids)]

            uniqueDF.to_sql(
                name=tableName,
                con=self.engine,
                if_exists="append",
                index=False,
            )
