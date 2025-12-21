#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = ["httpx", "loguru"]
# ///

"""
Upload .zip files from a directory to the import-hawkeye cloud function.

Automatically splits large files into chunks to avoid 413 errors.
Google Cloud Functions HTTP limit is 32MB.

Usage:
    uv run upload.py D:\\UTN_TTN\\Shared\\data
    uv run upload.py D:\\UTN_TTN\\Shared\\data --url https://custom-url.com
"""

import io
import sys
import zipfile
from pathlib import Path

import httpx
from loguru import logger as log

# Constants
HTTP_OK = 200
MIN_ARGS = 2
MAX_FILE_SIZE_MB = 30  # GCF limit is 32MB, leave headroom
MAX_FILE_SIZE_BYTES = MAX_FILE_SIZE_MB * 1024 * 1024
CHUNK_ROWS = 50000  # Rows per chunk when splitting


def find_zip_files(directory: str) -> list[Path]:
    """Find all .zip files in directory."""
    path = Path(directory)
    if not path.exists():
        log.error(f"Directory not found: {directory}")
        sys.exit(1)

    zips = list(path.glob("*.zip"))
    log.info(f"Found {len(zips)} .zip files in {directory}")
    return zips


def upload_bytes(data: bytes, filename: str, url: str) -> bool:
    """Upload bytes as a zip file to the cloud function."""
    try:
        files = {"file": (filename, io.BytesIO(data), "application/zip")}
        response = httpx.post(url, files=files, timeout=300)

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


def split_csv_content(csv_content: str, chunk_rows: int) -> list[str]:
    """Split CSV content into chunks of chunk_rows each."""
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


def create_zip_from_csv(csv_name: str, csv_content: str) -> bytes:
    """Create a zip file in memory containing a single CSV."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(csv_name, csv_content)
    return buffer.getvalue()


def process_large_zip(zip_path: Path, url: str) -> bool:
    """
    Process a large zip by extracting CSVs, splitting them, and uploading chunks.

    :returns: True if ALL chunks uploaded successfully
    """
    all_success = True

    log.info(f"Splitting {zip_path.name} into chunks...")

    with zipfile.ZipFile(zip_path, "r") as zf:
        csv_files = [n for n in zf.namelist() if n.lower().endswith(".csv")]

        for csv_name in csv_files:
            csv_content = zf.read(csv_name).decode("utf-8", errors="replace")
            chunks = split_csv_content(csv_content, CHUNK_ROWS)
            log.info(f"  {csv_name}: {len(chunks)} chunk(s)")

            for i, chunk in enumerate(chunks):
                chunk_zip = create_zip_from_csv(csv_name, chunk)
                chunk_name = f"{zip_path.stem}_{Path(csv_name).stem}_part{i + 1}.zip"

                if not upload_bytes(chunk_zip, chunk_name, url):
                    all_success = False

    return all_success


def upload_file(zip_path: Path, url: str) -> bool:
    """
    Upload a zip file, splitting if too large.

    :returns: True if upload succeeded (all parts for chunked files)
    """
    file_size = zip_path.stat().st_size
    file_size_mb = file_size / (1024 * 1024)

    if file_size > MAX_FILE_SIZE_BYTES:
        log.warning(f"{zip_path.name} ({file_size_mb:.1f}MB) exceeds {MAX_FILE_SIZE_MB}MB limit")
        return process_large_zip(zip_path, url)

    log.info(f"Uploading {zip_path.name} ({file_size_mb:.1f}MB)...")

    try:
        with open(zip_path, "rb") as f:
            files = {"file": (zip_path.name, f, "application/zip")}
            response = httpx.post(url, files=files, timeout=300)

        if response.status_code != HTTP_OK:
            log.error(f"{zip_path.name}: HTTP {response.status_code}")
            return False

        result = response.json()
        status = result.get("status", "unknown")
        msg = result.get("message", "No message")

        if status == "success":
            log.success(f"{zip_path.name}: {msg}")
            return True
        else:
            log.error(f"{zip_path.name}: {status} - {msg}")
            return False
    except Exception as e:
        log.error(f"{zip_path.name}: {e}")
        return False


def main():
    # Default URL for import-hawkeye cloud function
    default_url = "https://europe-west3-soldier-tracker.cloudfunctions.net/import-hawkeye"

    # Parse args
    if len(sys.argv) < MIN_ARGS:
        log.error("Usage: uv run upload.py <directory> [--url <url>]")
        sys.exit(1)

    directory = sys.argv[1]
    url = default_url

    if "--url" in sys.argv:
        url_idx = sys.argv.index("--url") + 1
        if url_idx < len(sys.argv):
            url = sys.argv[url_idx]

    log.info(f"Target: {url}")
    log.info(f"Max size: {MAX_FILE_SIZE_MB}MB (GCF limit: 32MB)")

    zip_files = find_zip_files(directory)

    if not zip_files:
        log.warning("No .zip files found.")
        return

    files_uploaded = 0
    files_failed = 0
    files_deleted = 0

    for zip_path in zip_files:
        if upload_file(zip_path, url):
            files_uploaded += 1
            # Delete file after successful upload
            try:
                zip_path.unlink()
                files_deleted += 1
                log.success(f"Deleted {zip_path.name}")
            except Exception as e:
                log.error(f"Failed to delete {zip_path.name}: {e}")
        else:
            files_failed += 1
            log.warning(f"Keeping {zip_path.name} (upload failed)")

    log.info(f"Done: {files_uploaded} uploaded, {files_deleted} deleted, {files_failed} failed")


if __name__ == "__main__":
    main()
