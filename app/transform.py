#!/usr/bin/env python3
# /// script
# requires-python = "==3.12.9"
# dependencies = []
# ///

"""
SPDX-License-Identifier: LicenseRef-NonCommercial-Only
© 2025 github.com/defmon3 — Non-commercial use only. Commercial use requires permission.
Format docstrings according to PEP 287
File: transform.py

Unified data transformation for all data types.
"""

import pandas as pd
from loguru import logger as log

from .custom_exceptions import TransformError
from .detector import DataType
from .schemas import get_schema


def clean_dataframe(df: pd.DataFrame, data_type: DataType) -> pd.DataFrame:
    """
    Apply transformations based on data type.

    :param df: Raw DataFrame from CSV
    :param data_type: Detected data type
    :returns: Cleaned DataFrame ready for BigQuery
    :raises TransformError: If transformation fails
    """
    try:
        schema = get_schema(data_type)
        log.debug(f"Transforming {len(df)} rows for {data_type.value}")

        # Drop unwanted columns
        cols_to_drop = [c for c in schema.columns_to_drop if c in df.columns]
        if cols_to_drop:
            log.debug(f"Dropping columns: {cols_to_drop}")
            df = df.drop(columns=cols_to_drop)

        # Rename columns
        rename_map = {k: v for k, v in schema.column_mapping.items() if k in df.columns}
        if rename_map:
            log.debug(f"Renaming {len(rename_map)} columns")
            df = df.rename(columns=rename_map)

        # Convert timestamps
        for col in schema.timestamp_columns:
            if col in df.columns:
                log.debug(f"Converting {col} to datetime")
                df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)

        # Convert floats
        for col in schema.float_columns:
            if col in df.columns:
                log.debug(f"Converting {col} to float")
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # Convert integers (round first to handle float values)
        for col in schema.int_columns:
            if col in df.columns:
                log.debug(f"Converting {col} to Int64")
                df[col] = pd.to_numeric(df[col], errors="coerce").round().astype("Int64")

        # Convert strings
        for col in schema.string_columns:
            if col in df.columns:
                df[col] = df[col].astype(str).replace("nan", None)

        # Add event_date partition column
        if "event_time" in df.columns:
            log.debug("Adding event_date partition column")
            df["event_date"] = pd.to_datetime(df["event_time"], errors="coerce").dt.date

        log.info(f"Transformation complete: {len(df)} rows, {len(df.columns)} columns")
        return df

    except Exception as e:
        log.error(f"Transformation failed: {e}")
        raise TransformError(f"Failed to transform data: {e}") from e
