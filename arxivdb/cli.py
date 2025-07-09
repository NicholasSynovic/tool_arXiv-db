"""
Command line parser for `arxivdb`.

Copyright (C) 2025 Nicholas M. Synovic.

"""

import importlib
import importlib.metadata
from argparse import ArgumentParser, Namespace

from arxivdb.api import file_system


class CLI:
    """
    A class to handle command-line interface for processing arXiv metadata.

    This class uses the argparse library to define and parse command-line
    arguments for the program. It includes options for specifying the input JSON
    file, output database path, chunk size for processing, and displaying the
    program version.

    Attributes:
        parser (ArgumentParser): The argument parser for handling command-line inputs.

    """

    def __init__(self) -> None:
        """Initialize the CLI class and set up the argument parser."""
        self.parser: ArgumentParser = ArgumentParser(
            prog="arxivdb",
            description="Process arXiv metadata and store it in an SQLite3 database.",
            epilog="Copyright (C) 2025 Nicholas M. Synovic.",
        )

        self.parser.add_argument(
            "-c",
            "--chunksize",
            required=False,
            default=10000,
            help="Number of JSON lines to chunk when parsing the arXiv dataset",
            type=int,
            nargs=1,
        )

        self.parser.add_argument(
            "-i",
            "--input",
            required=True,
            help="Path to a JSON Lines arXiv metadata file",
            type=file_system.resolve_path,
            nargs=1,
        )

        self.parser.add_argument(
            "-o",
            "--output",
            required=True,
            help="Path to store SQLite3 database",
            type=file_system.resolve_path,
            nargs=1,
        )

        self.parser.add_argument(
            "-v",
            "--version",
            action="version",
            version=importlib.metadata.version(distribution_name="arxivdb"),
        )

    def parse_args(self) -> Namespace:
        """
        Parse the command-line arguments.

        Returns:
            Namespace: The parsed arguments as a Namespace object.

        """
        return self.parser.parse_args()
