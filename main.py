#!/usr/bin/env python3
"""
Cloud Function entry point.

This file exposes the main function at the module root for Cloud Functions.
"""

from app.main import main  # noqa: F401

# Cloud Functions looks for a function named 'main' at the module root
# The import above makes app.main.main available as main.main
