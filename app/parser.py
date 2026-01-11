#!/usr/bin/env python3
# /// script
# requires-python = "==3.12.9"
# dependencies = []
# ///

"""
SPDX-License-Identifier: LicenseRef-NonCommercial-Only
© 2025 github.com/defmon3 — Non-commercial use only. Commercial use requires permission.
Format docstrings according to PEP 287
File: parser.py

CSV parsing module. Single responsibility: parse CSV bytes into DataFrames.
"""

from io import BytesIO

import pandas as pd
from loguru import logger as log

from .custom_exceptions import RequestError


def parse_csv(csv_bytes: bytes, filename: str = "unknown") -> pd.DataFrame:
    """
    Parse CSV bytes into a DataFrame.

    :param csv_bytes: CSV file contents as bytes
    :param filename: Original filename for logging
    :returns: DataFrame with CSV contents
    :raises RequestError: If CSV parsing fails
    """
    try:
        df = pd.read_csv(BytesIO(csv_bytes))
        log.info(f"Parsed {len(df)} rows from {filename}")
        return df
    except Exception as e:
        raise RequestError(f"Failed to parse CSV {filename}: {e}") from e
