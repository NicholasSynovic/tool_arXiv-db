"""
Data manipulation functions.

Copyright (C) 2025 Nicholas M. Synovic.

"""

from operator import itemgetter

import pandas as pd
from pandas import DataFrame, Series


def get_unique_categories(json_chunk: DataFrame) -> Series:
    data: DataFrame = json_chunk[["categories"]].copy()
    data = data.fillna(value="EMPTY")
    data["categories"] = data["categories"].str.strip()
    data["split_categories"] = data["categories"].str.split(pat=" ")
    return Series(data["split_categories"].explode().unique())


def get_unique_submitters(json_chunk: DataFrame) -> Series:
    data: DataFrame = json_chunk[["submitter"]].copy()
    data = data.fillna(value="EMPTY")
    data["submitter"] = data["submitter"].str.strip()
    return Series(data["submitter"].unique())


def get_unique_authors(json_chunk: DataFrame) -> Series:
    data: DataFrame = json_chunk[["authors_parsed"]].copy()
    data = data.fillna(value="EMPTY")
    authors: Series = (
        data["authors_parsed"]
        .explode()
        .apply(" ".join)
        .str.replace(pat="  ", repl=" ")
        .str.strip()
    )
    return Series(authors.unique())


def get_document_versions(json_chunk: DataFrame) -> DataFrame:
    """
    Extract and transform version information from the input DataFrame.

    Args:
        json_chunk (DataFrame): The input DataFrame containing 'id' and 'versions'
            columns.

    Returns:
        DataFrame: A new DataFrame with exploded version details, containing
            'id', 'version', and 'created' columns.

    """
    data: DataFrame = json_chunk[["id", "versions"]].copy()
    data = data.explode(column="versions", ignore_index=True)

    data["version"] = data["versions"].apply(func=itemgetter("version"))
    data["created"] = data["versions"].apply(func=itemgetter("created"))
    data["created"] = data["created"].apply(func=pd.Timestamp)

    return data.drop(columns="versions")


def get_documents(json_chunk: DataFrame) -> DataFrame:
    """
    Extract and transform specific columns from the input DataFrame.

    Args:
        json_chunk (DataFrame): The input DataFrame containing various columns.

    Returns:
        DataFrame: A new DataFrame containing selected columns with the
            'update_date' column transformed to Timestamp.

    """
    data: DataFrame = json_chunk[
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
    ].copy()
    data["update_date"] = data["update_date"].apply(func=pd.Timestamp)

    return data
