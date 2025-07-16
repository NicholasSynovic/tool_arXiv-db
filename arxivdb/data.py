"""
Data manipulation functions.

Copyright (C) 2025 Nicholas M. Synovic.

"""

from operator import itemgetter

import pandas as pd
from pandas import DataFrame, Series


def get_categories(json_chunk: DataFrame) -> DataFrame:
    compressed_data: DataFrame = json_chunk[["id", "categories"]].copy()
    compressed_data = compressed_data.fillna(value="EMPTY")
    compressed_data["categories"] = compressed_data["categories"].str.strip()
    compressed_data["split_categories"] = compressed_data["categories"].str.split(
        pat=" "
    )

    return compressed_data.explode(column="split_categories", ignore_index=True)


def get_submitters(json_chunk: DataFrame) -> DataFrame:
    data: DataFrame = json_chunk[["id", "submitter"]].copy()
    data = data.fillna(value="EMPTY")
    data["submitter"] = data["submitter"].str.strip()
    return data


def get_authors(json_chunk: DataFrame) -> DataFrame:
    compressed_data: DataFrame = json_chunk[["id", "authors_parsed"]].copy()
    compressed_data = compressed_data.fillna(value="EMPTY")

    data: DataFrame = compressed_data.explode(
        column="authors_parsed", ignore_index=True
    )
    data["authors"] = (
        data["authors_parsed"]
        .apply(" ".join)
        .str.replace(pat="  ", repl=" ")
        .str.strip()
    )
    return data.drop(columns="authors_parsed")


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


def get_unique_categories(json_chunk: DataFrame) -> Series: ...


def get_unique_submitters(json_chunk: DataFrame) -> Series: ...


def get_unique_authors(json_chunk: DataFrame) -> Series: ...
