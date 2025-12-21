#!/usr/bin/env python3
# /// script
# requires-python = "==3.12.9"
# dependencies = []
# ///

"""
SPDX-License-Identifier: LicenseRef-NonCommercial-Only
© 2025 github.com/defmon3 — Non-commercial use only. Commercial use requires permission.
Format docstrings according to PEP 287
File: custom_exceptions.py

Custom exception classes for the Hawkeye import function.
"""


class HawkeyeError(Exception):
    """Base exception for all Hawkeye import errors."""

    pass


class DetectionError(HawkeyeError):
    """Raised when data type detection fails."""

    pass


class TransformError(HawkeyeError):
    """Raised when data transformation fails."""

    pass


class LoadError(HawkeyeError):
    """Raised when BigQuery loading fails."""

    pass


class RequestError(HawkeyeError):
    """Raised when request parsing fails."""

    pass


class WatcherError(HawkeyeError):
    """Raised when file watcher encounters an error."""

    pass
