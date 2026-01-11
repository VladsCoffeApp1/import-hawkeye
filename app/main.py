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

Cloud Function entry point. HTTP boundary only - delegates to conductor.
"""

from typing import Any

import pendulum
from loguru import logger as log

from . import conductor
from .config import settings
from .custom_exceptions import RequestError
from .discord_hook import handle_return


def _parse_request(request: Any) -> bytes:
    """
    Extract ZIP data from HTTP request.

    :param request: Flask request object
    :returns: ZIP file contents as bytes
    :raises RequestError: If no file data found
    """
    if request.files:
        file = next(iter(request.files.values()))
        log.debug(f"Received file upload: {file.filename}")
        return file.read()

    if request.data:
        log.debug(f"Received raw data: {len(request.data)} bytes")
        return request.data

    raise RequestError("No file data in request")


def main(request: Any) -> dict[str, Any]:
    """
    Cloud Function entry point.

    :param request: Flask request object
    :returns: Response dictionary with status and message
    """
    start = pendulum.now()
    log.info("Hawkeye import started")

    try:
        # IN: HTTP request → bytes
        zip_data = _parse_request(request)

        # DELEGATE: conductor handles everything
        result = conductor.run(zip_data)

        # OUT: results → HTTP response
        duration = (pendulum.now() - start).in_seconds()
        message = f"{result.message} in {duration:.1f}s"

        # Determine status based on results
        if not result.successful:
            # All CSVs failed
            log.error(message)
            return {"status": "error", "message": message}

        if result.failed:
            # Some succeeded, some failed
            log.warning(message)
            return {"status": "partial", "message": message}

        # All succeeded
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
