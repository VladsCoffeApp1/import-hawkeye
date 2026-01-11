#!/usr/bin/env python3
# /// script
# requires-python = "==3.12.9"
# dependencies = []
# ///

"""
SPDX-License-Identifier: LicenseRef-NonCommercial-Only
© 2025 github.com/defmon3 — Non-commercial use only. Commercial use requires permission.
Format docstrings according to PEP 287
File: unzipper.py

ZIP extraction module. Single responsibility: extract files from ZIP archives.
"""

import zipfile
from io import BytesIO

from loguru import logger as log

from .custom_exceptions import RequestError


def extract_csv_files(zip_data: bytes) -> list[tuple[str, bytes]]:
    """
    Extract all CSV files from a ZIP archive.

    :param zip_data: ZIP file contents as bytes
    :returns: List of tuples (filename, csv_bytes)
    :raises RequestError: If ZIP is invalid or contains no CSVs
    """
    try:
        with zipfile.ZipFile(BytesIO(zip_data)) as zf:
            csv_files = [f for f in zf.namelist() if f.lower().endswith(".csv")]

            if not csv_files:
                raise RequestError("No CSV file found in ZIP archive")

            log.info(f"Found {len(csv_files)} CSV files in ZIP")

            results = []
            for csv_name in csv_files:
                log.debug(f"Extracting {csv_name}")
                csv_bytes = zf.read(csv_name)
                results.append((csv_name, csv_bytes))

            return results

    except zipfile.BadZipFile as e:
        raise RequestError(f"Invalid ZIP file: {e}") from e
