#!/usr/bin/env python3
"""Run functions-framework locally with credentials."""
import os
import subprocess
import sys

# Set credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\credentials\mobile-research-328923-f2a7f95782d8.json"

# Run functions-framework
subprocess.run(
    [sys.executable, "-m", "functions_framework", "--target=main", "--port=8080"],
    cwd=os.path.dirname(__file__),
)
