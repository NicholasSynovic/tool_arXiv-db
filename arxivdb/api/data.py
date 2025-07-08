import pandas as pd
from pandas import DataFrame


def get_documents(df: DataFrame) -> DataFrame:
    data: DataFrame = df[
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

    data["update_date"] = data["update_date"].apply(func=pd.Timestamp)

    return data


def get_authors(df: DataFrame) -> DataFrame:
    data: DataFrame = df[["id", "authors_parsed"]].copy()

    data = data.explode(
        column="authors_parsed",
        ignore_index=True,
    )

    data["author"] = data["authors_parsed"].apply(
        lambda x: ", ".join(x),
    )

    return data.drop(columns="authors_parsed")


def get_document_versions(df: DataFrame) -> DataFrame:
    data: DataFrame = df[["id", "versions"]].copy()
    data = data.explode(column="versions", ignore_index=True)

    data["version"] = data["versions"].apply(
        lambda x: x["version"],
    )

    data["created"] = data["versions"].apply(
        lambda x: x["created"],
    )

    data["created"] = data["created"].apply(func=pd.Timestamp)

    return data.drop(columns="versions")
