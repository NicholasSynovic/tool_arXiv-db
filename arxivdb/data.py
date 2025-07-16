"""
Data manipulation functions.

Copyright (C) 2025 Nicholas M. Synovic.

"""

from operator import itemgetter

import pandas as pd
from pandas import DataFrame


def get_documents(df: DataFrame) -> DataFrame:
    """
    Extract and transform specific columns from the input DataFrame.

    Args:
        df (DataFrame): The input DataFrame containing various columns.

    Returns:
        DataFrame: A new DataFrame containing selected columns with the
            'update_date' column transformed to Timestamp.

    """
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
    """
    Extract and transform author information from the input DataFrame.

    Args:
        df (DataFrame): The input DataFrame containing 'id' and 'authors_parsed'
            columns.

    Returns:
        DataFrame: A new DataFrame with exploded and formatted author names,
            containing 'id' and 'author' columns.

    """
    data: DataFrame = df[["id", "authors_parsed"]].copy()

    data = data.explode(
        column="authors_parsed",
        ignore_index=True,
    )

    data["author"] = data["authors_parsed"].apply(func=", ".join)

    return data.drop(columns="authors_parsed")


def get_document_versions(df: DataFrame) -> DataFrame:
    """
    Extract and transform version information from the input DataFrame.

    Args:
        df (DataFrame): The input DataFrame containing 'id' and 'versions'
            columns.

    Returns:
        DataFrame: A new DataFrame with exploded version details, containing
            'id', 'version', and 'created' columns.

    """
    data: DataFrame = df[["id", "versions"]].copy()
    data = data.explode(column="versions", ignore_index=True)

    data["version"] = data["versions"].apply(func=itemgetter("version"))
    data["created"] = data["versions"].apply(func=itemgetter("created"))
    data["created"] = data["created"].apply(func=pd.Timestamp)

    return data.drop(columns="versions")
