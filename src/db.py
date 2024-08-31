from pathlib import Path

from sqlalchemy import (
    Column,
    Engine,
    MetaData,
    PrimaryKeyConstraint,
    String,
    Table,
    create_engine,
)


class DB:
    def __init__(self, path: Path) -> None:
        self.path: Path = path
        self.engine: Engine = create_engine(url=f"sqlite:///{path}")
        self.metadata: MetaData = MetaData()

        self.createTables()

    def createTables(self) -> None:
        _: Table = Table(
            "documents",
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
