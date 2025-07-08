import importlib
import importlib.metadata
from argparse import ArgumentParser, Namespace
from pathlib import Path

from arxivdb.api import utils


class CLI:
    def __init__(self) -> None:
        self.parser: ArgumentParser = ArgumentParser(
            prog="arxivdb",
            description="Process arXiv metadata and store it in an SQLite3 database.",
            epilog="Copyright (C) 2025 Nicholas M. Synovic.",
        )

        self.parser.add_argument(
            "-i",
            "--input",
            required=True,
            help="Path to a JSON Lines arXiv metadata file",
            type=utils.resolve_path,
        )

        self.parser.add_argument(
            "-o",
            "--output",
            required=True,
            help="Path to store SQLite3 database",
            type=utils.resolve_path,
        )

        self.parser.add_argument(
            "-v",
            "--version",
            action="version",
            version=importlib.metadata.version(distribution_name="arxivdb"),
        )

    def parse_args(self) -> Namespace:
        return self.parser.parse_args()
