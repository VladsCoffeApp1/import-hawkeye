#!/usr/bin/env python3
# /// script
# requires-python = "==3.12.9"
# dependencies = []
# ///

"""
SPDX-License-Identifier: LicenseRef-NonCommercial-Only
© 2025 github.com/defmon3 — Non-commercial use only. Commercial use requires permission.
Format docstrings according to PEP 287
File: main.py

Cloud Function entry point for unified Hawkeye imports.
"""

from typing import Any

import pendulum
from loguru import logger as log

from .bigquery_loader import load_to_bigquery
from .config import settings
from .detector import detect_data_type
from .discord_hook import handle_return
from .request_handler import (
    determine_input_mode,
    download_from_gcs,
    extract_all_csvs_from_zip,
    parse_gcs_trigger,
    parse_zip_upload,
)
from .transform import clean_dataframe


def main(request: Any) -> dict[str, Any]:
    """
    Cloud Function entry point.

    Accepts ZIP files containing CSV data, auto-detects the data type,
    transforms the data, and loads it to the appropriate BigQuery table.
    Handles ZIP files with multiple CSVs (e.g., shadowfleet with port_events and vessel_history).

    :param request: Flask request object
    :returns: Response dictionary with status and message
    """
    start = pendulum.now()
    log.info("Hawkeye import started")

    try:
        # 1. Determine input mode and get ZIP data
        input_mode = determine_input_mode(request)
        log.debug(f"Input mode: {input_mode}")

        if input_mode == "gcs":
            gcs_ref = parse_gcs_trigger(request)
            log.info(f"Processing GCS file: gs://{gcs_ref.bucket}/{gcs_ref.blob}")
            zip_data = download_from_gcs(gcs_ref.bucket, gcs_ref.blob)
        else:
            log.info("Processing uploaded file")
            zip_data = parse_zip_upload(request)

        # 2. Extract ALL CSVs from ZIP
        csv_files = extract_all_csvs_from_zip(zip_data)
        log.info(f"Found {len(csv_files)} CSV files in ZIP")

        # 3. Process each CSV file
        total_rows = 0
        results = []

        for csv_name, raw_df in csv_files:
            log.info(f"Processing {csv_name} ({len(raw_df)} rows)")

            # Detect type
            data_type, target_table = detect_data_type(raw_df.columns.tolist())
            log.info(f"Detected {data_type.value} -> {target_table}")

            # Transform data
            df = clean_dataframe(raw_df, data_type)

            # Load to BigQuery
            loaded_rows = load_to_bigquery(
                df=df,
                data_type=data_type,
                project=settings.project_id,
                dataset=settings.bq_dataset,
                table=target_table,
            )

            total_rows += loaded_rows
            results.append(f"{loaded_rows} {data_type.value} rows to {target_table}")

        # 4. Calculate duration and return
        duration = (pendulum.now() - start).in_seconds()
        message = f"Loaded {total_rows} total rows ({'; '.join(results)}) in {duration:.1f}s"
        log.info(message)

        return handle_return(message, settings.discord_hook_url)

    except Exception as e:
        duration = (pendulum.now() - start).in_seconds()
        error_msg = f"Import failed after {duration:.1f}s: {e}"
        log.error(error_msg)

        return {
            "status": "error",
            "message": error_msg,
        }
