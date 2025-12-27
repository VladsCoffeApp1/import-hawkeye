#!/usr/bin/env python3
# /// script
# requires-python = "==3.12.9"
# dependencies = []
# ///

"""
SPDX-License-Identifier: LicenseRef-NonCommercial-Only
© 2025 github.com/defmon3 — Non-commercial use only. Commercial use requires permission.
Format docstrings according to PEP 287
File: gps.py

GPS schema definition for BigQuery.
Matches gps schema with 29 columns - KEEP ALL DATA!
"""

from google.cloud import bigquery

# Columns to drop from the CSV before loading
# ONLY drop columns that don't exist in BigQuery and are not needed
COLUMNS_TO_DROP: list[str] = [
    "_id",
    "_index",
    "Snet Username",
    "Image Id",  # Not in gps
]

# Column name mapping: CSV column name -> BigQuery column name
COLUMN_MAPPING: dict[str, str] = {
    "Document ID": "document_id",
    "Display Name": "display_name",
    "Id": "id",
    "Emitter Id": "emitter_id",
    "Pass Group Id": "pass_group_id",
    "City": "city",
    "Country": "country",
    "Lat": "latitude",
    "Lon": "longitude",
    "Detected Location": "detected_location",
    "Event Time": "event_time",
    "Last Updated": "last_updated",
    "Received At": "received_at",
    "Source": "source",
    "Super Type": "super_type",
    "Type": "type",
    "Constellation": "constellation",
    "Ellipse Area": "ellipse_area",
    "Frequency": "frequency",
    "Max Freq": "max_freq",
    "Min Freq": "min_freq",
    "Num Bursts": "num_bursts",
    "Orientation": "orientation",
    "Semi Major": "semi_major",
    "Semi Minor": "semi_minor",
    "Soi": "soi",
    "Version": "version",
    # Additional columns from new CSV format
    "Power Level": "power_level",
}

# Columns that should be converted to TIMESTAMP
TIMESTAMP_COLUMNS: list[str] = ["event_time", "last_updated", "received_at"]

# Columns that should be converted to FLOAT
FLOAT_COLUMNS: list[str] = [
    "latitude",
    "longitude",
    "ellipse_area",
    "frequency",
    "max_freq",
    "min_freq",
    "orientation",
    "semi_major",
    "semi_minor",
    "power_level",
]

# Columns that should be converted to INTEGER
INT_COLUMNS: list[str] = ["id", "num_bursts"]

# Columns that should be converted to STRING
STRING_COLUMNS: list[str] = [
    "document_id",
    "display_name",
    "emitter_id",
    "pass_group_id",
    "city",
    "country",
    "detected_location",
    "source",
    "super_type",
    "type",
    "constellation",
    "soi",
    "version",
]

# BigQuery schema definition - matches gps table
BIGQUERY_SCHEMA: list[bigquery.SchemaField] = [
    bigquery.SchemaField("document_id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("display_name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("id", "INT64", mode="NULLABLE"),
    bigquery.SchemaField("emitter_id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("pass_group_id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("city", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("country", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("latitude", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("longitude", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("detected_location", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("event_time", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("event_date", "DATE", mode="NULLABLE"),
    bigquery.SchemaField("last_updated", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("received_at", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("source", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("super_type", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("type", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("constellation", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("ellipse_area", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("frequency", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("max_freq", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("min_freq", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("num_bursts", "INT64", mode="NULLABLE"),
    bigquery.SchemaField("orientation", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("semi_major", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("semi_minor", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("soi", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("version", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("power_level", "FLOAT64", mode="NULLABLE"),
]

# Partition field for BigQuery table
PARTITION_FIELD: str = "event_date"

# Cluster fields for BigQuery table
CLUSTER_FIELDS: list[str] = ["country", "type", "source"]

# Deduplication key columns (natural key for MERGE)
DEDUP_KEY: list[str] = ["orientation", "semi_major", "semi_minor", "event_time", "event_date"]
