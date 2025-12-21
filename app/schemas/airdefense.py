#!/usr/bin/env python3
# /// script
# requires-python = "==3.12.9"
# dependencies = []
# ///

"""
SPDX-License-Identifier: LicenseRef-NonCommercial-Only
© 2025 github.com/defmon3 — Non-commercial use only. Commercial use requires permission.
Format docstrings according to PEP 287
File: airdefense.py

Airdefense schema definition for BigQuery.
Matches airdefense_v2 schema with 26 columns - KEEP ALL DATA!
"""

from google.cloud import bigquery

# Columns to drop from the CSV before loading
# ONLY drop columns that don't exist in BigQuery and are not needed
COLUMNS_TO_DROP: list[str] = [
    "_id",
    "_index",
    "Snet Username",
]

# Column name mapping: CSV column name -> BigQuery column name
COLUMN_MAPPING: dict[str, str] = {
    "Document ID": "document_id",
    "Display Name": "display_name",
    "Id": "id",
    "Image Id": "image_id",
    "Pass Group Id": "pass_group_id",
    "City": "city",
    "Country": "country",
    "Lat": "latitude",
    "Lon": "longitude",
    "Event Time": "event_time",
    "Last Updated": "last_updated",
    "Received At": "received_at",
    "Source": "source",
    "Super Type": "super_type",
    "Type": "type",
    "Constellation": "constellation",
    "Country Of Origin": "country_of_origin",
    "Detection Confidence": "detection_confidence",
    "Potential Emitter": "potential_emitter",
    "Potential Emitter Nato": "potential_emitter_nato",
    "Potential Purpose": "potential_purpose",
    "Potential Weapon System Association": "potential_weapon_system_association",
    "Orientation": "orientation",
    "Semi Major": "semi_major",
    "Semi Minor": "semi_minor",
}

# Columns that should be converted to TIMESTAMP
TIMESTAMP_COLUMNS: list[str] = ["event_time", "last_updated", "received_at"]

# Columns that should be converted to FLOAT
FLOAT_COLUMNS: list[str] = [
    "latitude",
    "longitude",
    "orientation",
    "semi_major",
    "semi_minor",
]

# Columns that should be converted to INTEGER
INT_COLUMNS: list[str] = []

# Columns that should be converted to STRING
STRING_COLUMNS: list[str] = [
    "document_id",
    "display_name",
    "id",
    "image_id",
    "pass_group_id",
    "city",
    "country",
    "source",
    "super_type",
    "type",
    "constellation",
    "country_of_origin",
    "detection_confidence",
    "potential_emitter",
    "potential_emitter_nato",
    "potential_purpose",
    "potential_weapon_system_association",
]

# BigQuery schema definition - matches airdefense_v2
BIGQUERY_SCHEMA: list[bigquery.SchemaField] = [
    bigquery.SchemaField("document_id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("display_name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("image_id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("pass_group_id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("city", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("country", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("latitude", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("longitude", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("event_time", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("event_date", "DATE", mode="NULLABLE"),
    bigquery.SchemaField("last_updated", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("received_at", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("source", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("super_type", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("type", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("constellation", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("country_of_origin", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("detection_confidence", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("potential_emitter", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("potential_emitter_nato", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("potential_purpose", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("potential_weapon_system_association", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("orientation", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("semi_major", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("semi_minor", "FLOAT64", mode="NULLABLE"),
]

# Partition field for BigQuery table
PARTITION_FIELD: str = "event_date"

# Cluster fields for BigQuery table
CLUSTER_FIELDS: list[str] = ["country", "type", "potential_emitter"]
