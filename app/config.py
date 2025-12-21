#!/usr/bin/env python3
# /// script
# requires-python = "==3.12.9"
# dependencies = []
# ///

"""
SPDX-License-Identifier: LicenseRef-NonCommercial-Only
© 2025 github.com/defmon3 — Non-commercial use only. Commercial use requires permission.
Format docstrings according to PEP 287
File: config.py

Configuration settings using Pydantic.
"""

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


def _find_env_file() -> Path | None:
    """Find project.env in multiple locations."""
    # Locations to check (in order of priority)
    locations = [
        Path.cwd() / "project.env",  # Current working directory
        Path(__file__).parent / "project.env",  # Same directory as config.py
        Path(__file__).parent.parent / "project.env",  # Parent directory (repo root)
    ]
    for loc in locations:
        if loc.exists():
            return loc
    return None


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=_find_env_file(),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # GCP Settings
    project_id: str = Field(default="soldier-tracker", alias="PROJECT_ID")
    region: str = Field(default="europe-west3", alias="REGION")
    service_name: str = Field(default="import-hawkeye", alias="SERVICE_NAME")

    # BigQuery Settings
    bq_dataset: str = Field(default="hawkeye_eu", alias="BQ_DATASET")

    # Cloud Storage Settings
    gcs_bucket: str = Field(default="hawkeye-imports", alias="GCS_BUCKET")

    # Discord Settings
    discord_hook_url: str | None = Field(default=None, alias="DISCORD_HOOK_URL")
    discord_at_mention: str | None = Field(default=None, alias="DISCORD_AT_MENTION")

    # Local Watcher Settings
    watch_directory: str = Field(default="", alias="WATCH_DIRECTORY")
    gcf_url: str | None = Field(default=None, alias="GCF_URL")


# Singleton instance
settings = Settings()
