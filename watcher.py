#!/usr/bin/env python3
# /// script
# requires-python = "==3.12.9"
# dependencies = []
# ///

"""
SPDX-License-Identifier: LicenseRef-NonCommercial-Only
© 2025 github.com/defmon3 — Non-commercial use only. Commercial use requires permission.
Format docstrings according to PEP 287
File: watcher.py

Local file watcher that monitors a directory for new ZIP files
and sends them to the Cloud Function for processing.
"""

import io
import sys
import tempfile
import time
import zipfile
from pathlib import Path

import httpx
import pandas as pd
from loguru import logger as log

from app.config import settings
from app.custom_exceptions import WatcherError

# Configuration
WATCH_DIR = Path(settings.watch_directory)
POLL_INTERVAL = 10  # seconds
FILE_STABLE_TIME = 5  # seconds to wait for file to stabilize
REQUEST_TIMEOUT = 300  # 5 minutes
MAX_CHUNK_ROWS = 50000  # Max rows per chunk (keeps ZIP under ~20MB)
MAX_FILE_SIZE_MB = 30  # Files larger than this get chunked


def get_new_files() -> list[Path]:
    """
    Scan watch directory for ZIP files.

    :returns: List of ZIP file paths
    """
    if not WATCH_DIR.exists():
        log.warning(f"Watch directory does not exist: {WATCH_DIR}")
        return []

    files = list(WATCH_DIR.glob("*.zip"))
    log.debug(f"Found {len(files)} ZIP files in {WATCH_DIR}")
    return files


def needs_chunking(file_path: Path) -> bool:
    """
    Check if file is too large and needs to be chunked.

    :param file_path: Path to ZIP file
    :returns: True if file needs chunking
    """
    size_mb = file_path.stat().st_size / (1024 * 1024)
    return size_mb > MAX_FILE_SIZE_MB


def chunk_and_upload(file_path: Path) -> tuple[bool, str]:
    """
    Split large ZIP into chunks and upload each separately.

    :param file_path: Path to large ZIP file
    :returns: Tuple of (success, message)
    """
    log.info(f"Chunking large file: {file_path.name}")

    try:
        # Extract CSV from ZIP
        with zipfile.ZipFile(file_path, "r") as zf:
            csv_names = [n for n in zf.namelist() if n.endswith(".csv")]
            if not csv_names:
                return False, "No CSV file found in ZIP"

            csv_name = csv_names[0]
            with zf.open(csv_name) as f:
                df = pd.read_csv(f)

        total_rows = len(df)
        num_chunks = (total_rows // MAX_CHUNK_ROWS) + 1
        log.info(f"Splitting {total_rows} rows into {num_chunks} chunks")

        success_count = 0
        for i in range(num_chunks):
            start_idx = i * MAX_CHUNK_ROWS
            end_idx = min((i + 1) * MAX_CHUNK_ROWS, total_rows)
            chunk_df = df.iloc[start_idx:end_idx]

            if len(chunk_df) == 0:
                continue

            # Create temporary ZIP with chunk
            chunk_buffer = io.BytesIO()
            with zipfile.ZipFile(chunk_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
                csv_buffer = io.StringIO()
                chunk_df.to_csv(csv_buffer, index=False)
                zf.writestr(csv_name, csv_buffer.getvalue())

            chunk_buffer.seek(0)
            chunk_data = chunk_buffer.read()

            log.info(f"Uploading chunk {i+1}/{num_chunks} ({len(chunk_df)} rows)")

            # Upload chunk
            try:
                response = httpx.post(
                    settings.gcf_url,
                    content=chunk_data,
                    headers={"Content-Type": "application/octet-stream"},
                    timeout=REQUEST_TIMEOUT,
                )

                if response.status_code == 200:
                    result = response.json()
                    if result.get("status") == "success":
                        success_count += 1
                        log.info(f"Chunk {i+1}: {result.get('message', 'Success')}")
                    else:
                        log.error(f"Chunk {i+1} failed: {result.get('message')}")
                else:
                    log.error(f"Chunk {i+1} HTTP {response.status_code}")

            except Exception as e:
                log.error(f"Chunk {i+1} error: {e}")

        if success_count == num_chunks:
            return True, f"All {num_chunks} chunks uploaded ({total_rows} total rows)"
        else:
            return False, f"Only {success_count}/{num_chunks} chunks uploaded"

    except Exception as e:
        return False, f"Chunking failed: {e}"


def is_file_stable(file_path: Path) -> bool:
    """
    Check if file has stopped being written to.

    :param file_path: Path to check
    :returns: True if file size is stable
    """
    try:
        initial_size = file_path.stat().st_size
        time.sleep(FILE_STABLE_TIME)
        current_size = file_path.stat().st_size
        return initial_size == current_size and initial_size > 0
    except OSError:
        return False


def send_to_gcf(file_path: Path) -> tuple[bool, str]:
    """
    Send file to Cloud Function.

    :param file_path: Path to ZIP file
    :returns: Tuple of (success, message)
    """
    gcf_url = settings.gcf_url

    if not gcf_url:
        raise WatcherError("GCF_URL is not configured in settings")

    log.info(f"Sending {file_path.name} to {gcf_url}")

    try:
        with open(file_path, "rb") as f:
            file_data = f.read()

        response = httpx.post(
            gcf_url,
            content=file_data,
            headers={"Content-Type": "application/octet-stream"},
            timeout=REQUEST_TIMEOUT,
        )

        if response.status_code == 200:
            result = response.json()
            if result.get("status") == "success":
                return True, result.get("message", "Success")
            else:
                return False, result.get("message", "Unknown error")
        else:
            return False, f"HTTP {response.status_code}: {response.text}"

    except httpx.TimeoutException:
        return False, "Request timed out"
    except httpx.RequestError as e:
        return False, f"Request failed: {e}"
    except Exception as e:
        return False, f"Unexpected error: {e}"


def process_file(file_path: Path) -> bool:
    """
    Process a single file: send to GCF and delete if successful.

    :param file_path: Path to ZIP file
    :returns: True if successful
    """
    log.info(f"Processing: {file_path.name}")

    # Check file is stable (not still being written)
    if not is_file_stable(file_path):
        log.warning(f"File not stable, skipping: {file_path.name}")
        return False

    # Check if file needs chunking
    if needs_chunking(file_path):
        size_mb = file_path.stat().st_size / (1024 * 1024)
        log.info(f"File is {size_mb:.1f}MB, chunking required")
        success, message = chunk_and_upload(file_path)
    else:
        success, message = send_to_gcf(file_path)

    if success:
        log.info(f"Success: {message}")
        # TEMPORARILY DISABLED - Do not delete files until schemas verified
        # try:
        #     file_path.unlink()
        #     log.info(f"Deleted: {file_path.name}")
        # except OSError as e:
        #     log.error(f"Failed to delete {file_path.name}: {e}")
        log.info(f"File kept (deletion disabled): {file_path.name}")
        return True
    else:
        log.error(f"Failed: {message}")
        return False


def watch_loop():
    """
    Main watch loop - continuously monitor directory.
    """
    log.info(f"Starting watcher on {WATCH_DIR}")
    log.info(f"GCF URL: {settings.gcf_url}")
    log.info(f"Poll interval: {POLL_INTERVAL}s")

    while True:
        try:
            files = get_new_files()

            for file_path in files:
                process_file(file_path)

            time.sleep(POLL_INTERVAL)

        except KeyboardInterrupt:
            log.info("Watcher stopped by user")
            break
        except Exception as e:
            log.error(f"Error in watch loop: {e}")
            time.sleep(POLL_INTERVAL)


def main():
    """
    Main entry point.
    """
    # Configure logging
    log.remove()
    log.add(
        sys.stderr,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{message}</cyan>",
        level="DEBUG",
    )
    log.add(
        "watcher.log",
        rotation="10 MB",
        retention="7 days",
        level="INFO",
    )

    # Validate settings
    if not settings.gcf_url:
        log.error("GCF_URL must be set in app/project.env")
        log.error("Example: GCF_URL=https://europe-west3-soldier-tracker.cloudfunctions.net/import-hawkeye")
        sys.exit(1)

    # Create watch directory if it doesn't exist
    if not WATCH_DIR.exists():
        log.info(f"Creating watch directory: {WATCH_DIR}")
        WATCH_DIR.mkdir(parents=True, exist_ok=True)

    # Run watch loop
    watch_loop()


if __name__ == "__main__":
    main()
