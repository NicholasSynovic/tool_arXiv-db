from pathlib import Path
from typing import List

from pandas import DataFrame
from sqlalchemy import (
    Column,
    DateTime,
    Engine,
    ForeignKeyConstraint,
    Integer,
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
        self.authorTable: str = "authors"
        self.versionTable: str = "versions"

        self.createTables()

    def createTables(self) -> None:
        _: Table = Table(
            self.documentTable,
            self.metadata,
            Column("id", String),
            Column("title", String),
            Column("submitter", String),
            Column("comments", String),
            Column("journal-ref", String),
            Column("doi", String),
            Column("report-no", String),
            Column("license", String),
            Column("abstract", String),
            Column("update_date", DateTime),
            PrimaryKeyConstraint("id"),
        )

        _: Table = Table(
            self.authorTable,
            self.metadata,
            Column("id", Integer),
            Column("document_id", String),
            Column("author", String),
            PrimaryKeyConstraint("id"),
            ForeignKeyConstraint(
                columns=["document_id"],
                refcolumns=["documents.id"],
            ),
        )

        _: Table = Table(
            self.versionTable,
            self.metadata,
            Column("id", Integer),
            Column("document_id", String),
            Column("version", String),
            Column("created", String),
            PrimaryKeyConstraint("id"),
            ForeignKeyConstraint(
                columns=["document_id"],
                refcolumns=["documents.id"],
            ),
        )

        self.metadata.create_all(bind=self.engine, checkfirst=True)

    def toSQL(self, tableName: str, df: DataFrame) -> int:
        try:
            df.to_sql(
                name=tableName,
                con=self.engine,
                if_exists="append",
                index=False,
            )

        except IntegrityError as error:
            ids: List[str] = [param[0] for param in error.params]
            df = df[~df["id"].isin(values=ids)]

            df.to_sql(
                name=tableName,
                con=self.engine,
                if_exists="append",
                index=False,
            )

        return df.shape[0]
