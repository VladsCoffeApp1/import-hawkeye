#!/usr/bin/env python3
# /// script
# requires-python = "==3.12.9"
# dependencies = []
# ///

"""
SPDX-License-Identifier: LicenseRef-NonCommercial-Only
© 2025 github.com/defmon3 — Non-commercial use only. Commercial use requires permission.
Format docstrings according to PEP 287
File: conductor.py

Pipeline orchestrator. Delegates to specialized modules:
unzipper → parser → detector → transformer → loader
"""

from dataclasses import dataclass, field

import pandas as pd
from loguru import logger as log

from .bigquery_loader import load_to_bigquery
from .config import settings
from .custom_exceptions import HawkeyeError
from .detector import DataType, detect_data_type
from .parser import parse_csv
from .transform import clean_dataframe
from .unzipper import extract_csv_files


@dataclass
class ProcessedCSV:
    """Result of processing a single CSV file."""

    filename: str
    data_type: DataType | None = None
    table: str | None = None
    df: pd.DataFrame | None = None
    rows_loaded: int = 0
    error: str | None = None

    @property
    def success(self) -> bool:
        """Check if processing succeeded."""
        return self.error is None


@dataclass
class PipelineResult:
    """Result of running the full pipeline."""

    processed: list[ProcessedCSV] = field(default_factory=list)
    total_rows: int = 0

    @property
    def successful(self) -> list[ProcessedCSV]:
        """Get successfully processed CSVs."""
        return [p for p in self.processed if p.success]

    @property
    def failed(self) -> list[ProcessedCSV]:
        """Get failed CSVs."""
        return [p for p in self.processed if not p.success]

    @property
    def message(self) -> str:
        """Format results as a human-readable message."""
        parts = [f"{p.rows_loaded} {p.data_type.value} rows to {p.table}" for p in self.successful]
        msg = f"Loaded {self.total_rows} total rows ({'; '.join(parts)})"

        if self.failed:
            failures = [f"{p.filename}: {p.error}" for p in self.failed]
            msg += f" | Failed: {'; '.join(failures)}"

        return msg


def _process_single_csv(
    filename: str,
    csv_bytes: bytes,
    dry_run: bool,
) -> ProcessedCSV:
    """
    Process a single CSV file through the pipeline.

    :param filename: Name of the CSV file
    :param csv_bytes: CSV file contents
    :param dry_run: If True, skip BigQuery loading
    :returns: ProcessedCSV with results or error
    """
    processed = ProcessedCSV(filename=filename)

    try:
        # Parse CSV to DataFrame
        df = parse_csv(csv_bytes, filename)

        # Detect data type
        data_type, table = detect_data_type(df.columns.tolist())
        log.info(f"Detected {data_type.value} -> {table}")

        # Transform data
        df = clean_dataframe(df, data_type)

        # Store results
        processed.data_type = data_type
        processed.table = table
        processed.df = df

        # Load to BigQuery (unless dry_run)
        if dry_run:
            log.info(f"[DRY RUN] Would load {len(df)} rows to {table}")
            processed.rows_loaded = len(df)
        else:
            rows = load_to_bigquery(
                df=df,
                data_type=data_type,
                project=settings.project_id,
                dataset=settings.bq_dataset,
                table=table,
            )
            processed.rows_loaded = rows

    except HawkeyeError as e:
        log.error(f"Failed to process {filename}: {e}")
        processed.error = str(e)
    except Exception as e:
        log.error(f"Unexpected error processing {filename}: {e}")
        processed.error = f"Unexpected error: {e}"

    return processed


def run(zip_data: bytes, dry_run: bool = False) -> PipelineResult:
    """
    Run the full import pipeline.

    :param zip_data: ZIP file contents as bytes
    :param dry_run: If True, skip BigQuery loading (for testing)
    :returns: PipelineResult with processed data and stats
    :raises HawkeyeError: If ZIP extraction fails (critical failure)
    """
    log.info("Pipeline started")
    result = PipelineResult()

    # Extract CSVs from ZIP - this is critical, fail if it fails
    csv_files = extract_csv_files(zip_data)

    # Process each CSV - continue on individual failures
    for filename, csv_bytes in csv_files:
        log.info(f"Processing {filename}")

        processed = _process_single_csv(filename, csv_bytes, dry_run)
        result.processed.append(processed)

        if processed.success:
            result.total_rows += processed.rows_loaded

    log.info(f"Pipeline complete: {result.message}")
    return result
