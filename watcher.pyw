#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = ["httpx", "loguru"]
# ///

"""
SPDX-License-Identifier: LicenseRef-NonCommercial-Only
© 2025 github.com/defmon3 — Non-commercial use only. Commercial use requires permission.
Format docstrings according to PEP 287
File: watcher.pyw

Local file watcher that monitors a directory for new ZIP files
and sends them to the Cloud Function for processing.
"""

import io
import sys
import time
import zipfile
from pathlib import Path

import httpx
from loguru import logger as log

# ============================================================================
# HARDCODED CONFIGURATION
# ============================================================================
GCF_URL = "https://europe-west3-soldier-tracker.cloudfunctions.net/import-hawkeye"
WATCH_DIR = Path(r"D:\UTN_TTN\Shared\data")
LOG_FILE = Path(r"D:\tools\watcher.log")  # Log file location
POLL_INTERVAL = 10  # seconds between directory scans
FILE_STABLE_TIME = 5  # seconds to wait for file to stabilize
UPLOAD_DELAY = 60  # seconds between uploads to avoid quota limits
REQUEST_TIMEOUT = 300  # 5 minutes for HTTP requests
MAX_CHUNK_ROWS = 50000  # Max rows per chunk
MAX_FILE_SIZE_MB = 30  # Files larger than this get chunked
MAX_FILE_SIZE_BYTES = MAX_FILE_SIZE_MB * 1024 * 1024
HTTP_OK = 200
# ============================================================================


def get_new_files() -> list[Path]:
    """
    Scan watch directory for ZIP files. Never crashes.

    :returns: List of ZIP file paths (empty on error)
    """
    try:
        if not WATCH_DIR.exists():
            log.warning(f"Watch directory does not exist: {WATCH_DIR}")
            return []

        files = list(WATCH_DIR.glob("*.zip"))
        if files:
            log.debug(f"Found {len(files)} ZIP files in {WATCH_DIR}")
        return files
    except Exception as e:
        log.error(f"Error scanning directory: {e}")
        return []


def split_csv_content(csv_content: str, chunk_rows: int) -> list[str]:
    """Split CSV content into chunks. Never crashes."""
    try:
        lines = csv_content.strip().split("\n")
        if len(lines) <= 1:
            return [csv_content]

        header = lines[0]
        data_lines = lines[1:]

        chunks = []
        for i in range(0, len(data_lines), chunk_rows):
            chunk_lines = data_lines[i : i + chunk_rows]
            chunk_content = header + "\n" + "\n".join(chunk_lines)
            chunks.append(chunk_content)

        return chunks
    except Exception as e:
        log.error(f"Error splitting CSV: {e}")
        return [csv_content]  # Return original on error


def create_zip_from_csv(csv_name: str, csv_content: str) -> bytes:
    """Create a zip file in memory. Never crashes."""
    try:
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr(csv_name, csv_content)
        return buffer.getvalue()
    except Exception as e:
        log.error(f"Error creating ZIP: {e}")
        return b""  # Return empty bytes on error


def upload_bytes(data: bytes, filename: str, url: str) -> bool:
    """Upload bytes as a zip file to the cloud function."""
    try:
        files = {"file": (filename, io.BytesIO(data), "application/zip")}
        response = httpx.post(url, files=files, timeout=REQUEST_TIMEOUT)

        if response.status_code != HTTP_OK:
            log.error(f"{filename}: HTTP {response.status_code}")
            return False

        result = response.json()
        status = result.get("status", "unknown")
        msg = result.get("message", "No message")

        if status == "success":
            log.success(f"{filename}: {msg}")
            return True
        else:
            log.error(f"{filename}: {status} - {msg}")
            return False
    except Exception as e:
        log.error(f"{filename}: {e}")
        return False


def chunk_and_upload(file_path: Path) -> tuple[bool, str]:
    """
    Split large ZIP into chunks and upload. Never crashes.

    :param file_path: Path to large ZIP file
    :returns: Tuple of (success, message)
    """
    try:
        log.info(f"Splitting {file_path.name} into chunks...")
        all_success = True

        with zipfile.ZipFile(file_path, "r") as zf:
            csv_files = [n for n in zf.namelist() if n.lower().endswith(".csv")]

            if not csv_files:
                return False, "No CSV files found in ZIP"

            for csv_name in csv_files:
                csv_content = zf.read(csv_name).decode("utf-8", errors="replace")
                chunks = split_csv_content(csv_content, MAX_CHUNK_ROWS)
                log.info(f"  {csv_name}: {len(chunks)} chunk(s)")

                for i, chunk in enumerate(chunks):
                    chunk_zip = create_zip_from_csv(csv_name, chunk)
                    if not chunk_zip:  # Empty bytes means error
                        all_success = False
                        continue

                    chunk_name = f"{file_path.stem}_{Path(csv_name).stem}_part{i + 1}.zip"

                    if not upload_bytes(chunk_zip, chunk_name, GCF_URL):
                        all_success = False

        if all_success:
            return True, "All chunks uploaded successfully"
        else:
            return False, "Some chunks failed to upload"
    except Exception as e:
        log.error(f"Error chunking {file_path.name}: {e}")
        return False, f"Chunking error: {e}"


def is_file_stable(file_path: Path) -> bool:
    """
    Check if file has stopped being written to. Never crashes.

    :param file_path: Path to check
    :returns: True if file size is stable
    """
    try:
        initial_size = file_path.stat().st_size
        time.sleep(FILE_STABLE_TIME)
        current_size = file_path.stat().st_size
        return initial_size == current_size and initial_size > 0
    except Exception as e:
        log.error(f"Error checking stability for {file_path.name}: {e}")
        return False


def send_to_gcf(file_path: Path) -> tuple[bool, str]:
    """
    Send file to Cloud Function. Never crashes.

    :param file_path: Path to ZIP file
    :returns: Tuple of (success, message)
    """
    try:
        file_size_mb = file_path.stat().st_size / (1024 * 1024)
        log.info(f"Uploading {file_path.name} ({file_size_mb:.1f}MB)...")

        with open(file_path, "rb") as f:
            files = {"file": (file_path.name, f, "application/zip")}
            response = httpx.post(GCF_URL, files=files, timeout=REQUEST_TIMEOUT)

        if response.status_code != HTTP_OK:
            log.error(f"{file_path.name}: HTTP {response.status_code}")
            return False, f"HTTP {response.status_code}"

        result = response.json()
        status = result.get("status", "unknown")
        msg = result.get("message", "No message")

        if status == "success":
            log.success(f"{file_path.name}: {msg}")
            return True, msg
        else:
            log.error(f"{file_path.name}: {status} - {msg}")
            return False, f"{status} - {msg}"
    except Exception as e:
        log.error(f"Error uploading {file_path.name}: {e}")
        return False, str(e)


def process_file(file_path: Path) -> bool:
    """
    Process a single file. Never crashes.

    :param file_path: Path to ZIP file
    :returns: True if successful
    """
    try:
        log.info(f"Processing: {file_path.name}")

        # Check file is stable (not still being written)
        if not is_file_stable(file_path):
            log.warning(f"File not stable, skipping: {file_path.name}")
            return False

        # Check if file needs chunking
        file_size = file_path.stat().st_size
        if file_size > MAX_FILE_SIZE_BYTES:
            file_size_mb = file_size / (1024 * 1024)
            log.warning(f"{file_path.name} ({file_size_mb:.1f}MB) exceeds {MAX_FILE_SIZE_MB}MB limit")
            success, message = chunk_and_upload(file_path)
        else:
            success, message = send_to_gcf(file_path)

        if success:
            log.info(f"Success: {message}")
            # Delete file after successful upload
            try:
                file_path.unlink()
                log.success(f"Deleted {file_path.name}")
            except Exception as e:
                log.error(f"Failed to delete {file_path.name}: {e}")
            return True
        else:
            log.error(f"Failed: {message}")
            # Rename failed file so it won't be retried
            try:
                fail_path = file_path.with_suffix(".zip.fail")
                file_path.rename(fail_path)
                log.warning(f"Renamed {file_path.name} -> {fail_path.name}")
            except Exception as e:
                log.error(f"Failed to rename {file_path.name}: {e}")
            return False
    except Exception as e:
        log.error(f"Error processing {file_path.name}: {e}")
        return False


def watch_loop():
    """
    Main watch loop - runs forever, never crashes.
    """
    log.info(f"Starting watcher on {WATCH_DIR}")
    log.info(f"Target: {GCF_URL}")
    log.info(f"Max size: {MAX_FILE_SIZE_MB}MB (GCF limit: 32MB)")
    log.info(f"Poll interval: {POLL_INTERVAL}s, Upload delay: {UPLOAD_DELAY}s")

    while True:
        try:
            files = get_new_files()

            for i, file_path in enumerate(files):
                try:
                    process_file(file_path)
                    # Wait between uploads to avoid quota limits
                    if i < len(files) - 1:
                        log.info(f"Waiting {UPLOAD_DELAY}s before next upload...")
                        time.sleep(UPLOAD_DELAY)
                except Exception as e:
                    log.error(f"Error processing {file_path.name}: {e}")
                    # Continue to next file instead of crashing

            time.sleep(POLL_INTERVAL)

        except KeyboardInterrupt:
            log.info("Watcher stopped by user")
            break
        except Exception as e:
            log.error(f"Critical error in watch loop: {e}")
            log.info(f"Recovering in {POLL_INTERVAL}s...")
            time.sleep(POLL_INTERVAL)
            # Loop continues - never exit


def main():
    """
    Main entry point. Never crashes.
    """
    try:
        # Ensure log directory exists
        LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

        # Configure logging (log to file only, no stderr since we're using .pyw)
        log.remove()
        log.add(
            str(LOG_FILE),
            rotation="10 MB",
            retention="7 days",
            level="INFO",
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}",
        )

        log.info("=" * 60)
        log.info("Hawkeye Watcher Starting...")
        log.info("=" * 60)
        log.info(f"Watch Directory: {WATCH_DIR}")
        log.info(f"Cloud Function: {GCF_URL}")
        log.info(f"Log File: {LOG_FILE}")

        # Create watch directory if it doesn't exist
        if not WATCH_DIR.exists():
            log.info(f"Creating watch directory: {WATCH_DIR}")
            WATCH_DIR.mkdir(parents=True, exist_ok=True)

        # Run watch loop (never exits except on Ctrl+C)
        watch_loop()

    except Exception as e:
        # Last resort error handling
        try:
            log.error(f"FATAL ERROR: {e}")
        except Exception:
            pass  # Even logging failed, just exit silently
        sys.exit(1)


if __name__ == "__main__":
    main()
