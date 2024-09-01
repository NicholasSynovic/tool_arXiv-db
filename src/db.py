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

        self.documentTable: str = "documents"
        self.authorTable: str = "authors"
        self.versionTable: str = "versions"

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
        """
        Load a DataFrame into a SQL table.

        This method takes a DataFrame and loads it into a specified SQL table.
        If the table already exists, it appends new data without overwriting existing rows.

        :param tableName: The name of the SQL table to load into.
        :type tableName: str
        :param df: The DataFrame containing the data to be loaded.
        :type df: DataFrame
        :return: The number of rows successfully inserted into the table.
        :rtype: int
        """  # noqa: E501
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
