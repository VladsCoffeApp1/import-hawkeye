#!/usr/bin/env python3
# /// script
# requires-python = "==3.12.9"
# dependencies = []
# ///

"""
SPDX-License-Identifier: LicenseRef-NonCommercial-Only
© 2025 github.com/defmon3 — Non-commercial use only. Commercial use requires permission.
Format docstrings according to PEP 287
File: request_handler.py

HTTP request parsing for GCS triggers and direct uploads.
"""

import zipfile
from dataclasses import dataclass
from io import BytesIO
from typing import Any

import pandas as pd
from google.cloud import storage
from loguru import logger as log

from .custom_exceptions import RequestError


@dataclass
class GCSReference:
    """Reference to a GCS object."""

    bucket: str
    blob: str


def determine_input_mode(request: Any) -> str:
    """
    Determine if request is GCS trigger or direct upload.

    :param request: Flask request object
    :returns: "gcs" or "upload"
    """
    # Check for GCS trigger format (Cloud Functions eventarc)
    request_json = request.get_json(silent=True) or {}

    if "bucket" in request_json and "name" in request_json:
        log.debug("Detected GCS trigger format")
        return "gcs"

    # Check for Cloud Storage trigger (legacy format)
    if "data" in request_json:
        data = request_json.get("data", {})
        if "bucket" in data and "name" in data:
            log.debug("Detected legacy GCS trigger format")
            return "gcs"

    # Check for file upload
    if request.files:
        log.debug("Detected file upload")
        return "upload"

    # Check for raw bytes
    if request.data:
        log.debug("Detected raw data upload")
        return "upload"

    raise RequestError("Cannot determine input mode from request")


def parse_gcs_trigger(request: Any) -> GCSReference:
    """
    Parse GCS trigger request to get bucket and blob.

    :param request: Flask request object
    :returns: GCSReference with bucket and blob
    :raises RequestError: If parsing fails
    """
    request_json = request.get_json(silent=True) or {}

    # Try direct format first
    if "bucket" in request_json and "name" in request_json:
        return GCSReference(
            bucket=request_json["bucket"],
            blob=request_json["name"],
        )

    # Try nested data format
    data = request_json.get("data", {})
    if "bucket" in data and "name" in data:
        return GCSReference(
            bucket=data["bucket"],
            blob=data["name"],
        )

    raise RequestError("Cannot parse GCS trigger: missing bucket or name")


def download_from_gcs(bucket: str, blob: str) -> bytes:
    """
    Download file from GCS.

    :param bucket: GCS bucket name
    :param blob: GCS blob/object name
    :returns: File contents as bytes
    :raises RequestError: If download fails
    """
    log.info(f"Downloading gs://{bucket}/{blob}")

    try:
        client = storage.Client()
        bucket_ref = client.bucket(bucket)
        blob_ref = bucket_ref.blob(blob)
        data = blob_ref.download_as_bytes()
        log.debug(f"Downloaded {len(data)} bytes")
        return data
    except Exception as e:
        log.error(f"Failed to download from GCS: {e}")
        raise RequestError(f"Failed to download from GCS: {e}") from e


def parse_zip_upload(request: Any) -> bytes:
    """
    Parse uploaded ZIP file from request.

    :param request: Flask request object
    :returns: ZIP file contents as bytes
    :raises RequestError: If parsing fails
    """
    # Check for file upload
    if request.files:
        file = next(iter(request.files.values()))
        log.debug(f"Received file upload: {file.filename}")
        return file.read()

    # Check for raw bytes
    if request.data:
        log.debug(f"Received raw data: {len(request.data)} bytes")
        return request.data

    raise RequestError("No file data in request")


def extract_csv_from_zip(zip_data: bytes) -> pd.DataFrame:
    """
    Extract CSV file from ZIP archive and read into DataFrame.

    :param zip_data: ZIP file contents as bytes
    :returns: DataFrame with CSV contents
    :raises RequestError: If extraction fails
    """
    try:
        with zipfile.ZipFile(BytesIO(zip_data)) as zf:
            # Find CSV file
            csv_files = [f for f in zf.namelist() if f.lower().endswith(".csv")]

            if not csv_files:
                raise RequestError("No CSV file found in ZIP archive")

            if len(csv_files) > 1:
                log.warning(f"Multiple CSV files found, using first: {csv_files[0]}")

            csv_name = csv_files[0]
            log.debug(f"Extracting {csv_name}")

            with zf.open(csv_name) as csv_file:
                df = pd.read_csv(csv_file)
                log.info(f"Read {len(df)} rows from {csv_name}")
                return df

    except zipfile.BadZipFile as e:
        raise RequestError(f"Invalid ZIP file: {e}") from e
    except Exception as e:
        raise RequestError(f"Failed to extract CSV from ZIP: {e}") from e


def extract_all_csvs_from_zip(zip_data: bytes) -> list[tuple[str, pd.DataFrame]]:
    """
    Extract ALL CSV files from ZIP archive and read into DataFrames.

    :param zip_data: ZIP file contents as bytes
    :returns: List of tuples (csv_name, DataFrame)
    :raises RequestError: If extraction fails
    """
    try:
        results = []
        with zipfile.ZipFile(BytesIO(zip_data)) as zf:
            # Find all CSV files
            csv_files = [f for f in zf.namelist() if f.lower().endswith(".csv")]

            if not csv_files:
                raise RequestError("No CSV file found in ZIP archive")

            log.info(f"Found {len(csv_files)} CSV files in ZIP")

            for csv_name in csv_files:
                log.debug(f"Extracting {csv_name}")
                with zf.open(csv_name) as csv_file:
                    df = pd.read_csv(csv_file)
                    log.info(f"Read {len(df)} rows from {csv_name}")
                    results.append((csv_name, df))

        return results

    except zipfile.BadZipFile as e:
        raise RequestError(f"Invalid ZIP file: {e}") from e
    except Exception as e:
        raise RequestError(f"Failed to extract CSV from ZIP: {e}") from e
