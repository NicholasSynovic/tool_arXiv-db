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
        """
        Initialize an instance of the class.

        This method sets up the necessary attributes for the class, including:

        * The database connection engine.
        * The metadata object used to create tables.
        * The names of the three tables to be created (documents, authors, and versions).

        :param path: The path to the SQLite database file.
        :type path: Path
        """  # noqa: E501
        self.path: Path = path
        self.engine: Engine = create_engine(url=f"sqlite:///{path}")
        self.metadata: MetaData = MetaData()

        self.createTables()

    def createTables(self) -> None:
        """
        Create SQL tables for document metadata and related information.

        This method creates three SQL tables using the SQLAlchemy library:
        - The `documents` table to store metadata about individual documents.
        - The `authors` table to store information about authors associated with each document.
        - The `versions` table to store versions of each document.

        Each table is created with a primary key and, where applicable, foreign key constraints
        to establish relationships between the tables.

        :return: None
        """  # noqa: E501
        _: Table = Table(
            "documents",
            self.metadata,
            Column("id", String),
            Column("title", String),
            Column("submitter", String),
            Column("comments", String),
            Column("journal-ref", String),
            Column("doi", String),
            Column("report-no", String),
            Column("categories", String),
            Column("license", String),
            Column("abstract", String),
            Column("update_date", DateTime),
            PrimaryKeyConstraint("id"),
        )

        _: Table = Table(
            "authors",
            self.metadata,
            Column("index", Integer, primary_key=True, autoincrement=True),
            Column("id", String),
            Column("author", String),
            ForeignKeyConstraint(
                columns=["id"],
                refcolumns=["documents.id"],
            ),
        )

        _: Table = Table(
            "versions",
            self.metadata,
            Column("index", Integer, primary_key=True, autoincrement=True),
            Column("id", String),
            Column("version", String),
            Column("created", DateTime),
            ForeignKeyConstraint(
                columns=["id"],
                refcolumns=["documents.id"],
            ),
        )

        self.metadata.create_all(bind=self.engine, checkfirst=True)

    def write_table(self, tableName: str, df: DataFrame) -> None:
        df.to_sql(
            name=tableName,
            con=self.engine,
            if_exists="append",
            index=False,
        )
