"""
SQLite3 Database Class.

Copyright (C) 2025 Nicholas M. Synovic.

"""

from pathlib import Path

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
    """
    A class to manage interactions with an SQLite database.

    This class provides methods for creating tables, writing data to tables, and
    managing database connections using SQLAlchemy. It is designed to handle
    document metadata and related information, including authors and versions.

    Attributes:
        path (Path): The path to the SQLite database file.
        engine (Engine): The SQLAlchemy engine for database connections.
        metadata (MetaData): The SQLAlchemy MetaData object for managing table
            schemas.

    """

    def __init__(self, path: Path) -> None:
        """
        Initialize an instance of the class.

        Set up the necessary attributes for the class, including the database
        connection engine, and the metadata object used to create tables.

        Args:
            path (Path): The path to the SQLite database file.

        """
        self.path: Path = path
        self.engine: Engine = create_engine(url=f"sqlite:///{path}")
        self.metadata: MetaData = MetaData()

        self.create_tables()

    def create_tables(self) -> None:
        """
        Create SQL tables for document metadata and related information.

        This method creates three SQL tables using the SQLAlchemy library:

        - The `documents` table to store metadata about individual documents.
        - The `authors` table to store information about authors associated with
            each document.
        - The `versions` table to store versions of each document.

        Each table is created with a primary key and, where applicable, foreign
        key constraints to establish relationships between the tables.

        """
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

    def write_table(self, table_name: str, df: DataFrame) -> None:
        """
        Write a DataFrame to a SQL table, handling potential integrity errors.

        This method attempts to append the DataFrame to the specified SQL table.
        If an IntegrityError occurs (e.g., due to duplicate primary keys), it
        filters out the conflicting rows and retries the operation.

        Args:
            table_name (str): The name of the table to write the DataFrame to.
            df (DataFrame): The DataFrame containing data to be written to the
                SQL table.

        """
        try:
            df.to_sql(
                name=table_name,
                con=self.engine,
                if_exists="append",
                index=False,
            )

        except IntegrityError as error:
            ids: list[str] = [param[0] for param in error.params]
            df = df[~df["id"].isin(values=ids)]

            df.to_sql(
                name=table_name,
                con=self.engine,
                if_exists="append",
                index=False,
            )
