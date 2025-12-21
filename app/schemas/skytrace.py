#!/usr/bin/env python3
# /// script
# requires-python = "==3.12.9"
# dependencies = []
# ///

"""
SPDX-License-Identifier: LicenseRef-NonCommercial-Only
© 2025 github.com/defmon3 — Non-commercial use only. Commercial use requires permission.
Format docstrings according to PEP 287
File: skytrace.py

Skytrace schema definition for BigQuery.
"""

from google.cloud import bigquery

# Columns to drop from the CSV before loading
COLUMNS_TO_DROP: list[str] = [
    "Document ID",
    "Display Name",
    "City",
    "Country",
    "Last Updated",
    "Source",
    "Super Type",
    "Type",
    "_id",
    "_index",
    "Altitude",
    "Carrier",
    "Connected Wifi Vendor Name",
    "Day Month Year Local",
    "Day Of Week",
    "Day Of Week Local",
    "Device Os",
    "Device Os Version",
    "Entity Age",
    "Entity Type",
    "Event Location Accuracy Confidence",
    "Event Location Accuracy Score",
    "Event Time Local",
    "Event Timezone",
    "First Seen",
    "Heading",
    "Horizontal Accuracy",
    "Hour Local",
    "Ip V 4",
    "Ip V 6",
    "Last Seen",
    "Loc At",
    "Locale",
    "Mac",
    "Meta Row Id",
    "Minute Local",
    "Name",
    "Rssi",
    "Satellite Provider Country",
    "Satellite Provider Type",
    "Site Id",
    "Snet Username",
    "Speed",
    "Tech",
    "Time",
    "User Agent",
    "Vendor Name",
    "Vertical Accuracy",
    "Wifi Bssid",
    "Wifi Ssid",
    # Duplicate columns created by pandas when CSV has duplicate headers
    "Event Time.1",
    "Mac.1",
    "Name.1",
    "Tech.1",
]

# Column name mapping: CSV column name -> BigQuery column name
COLUMN_MAPPING: dict[str, str] = {
    "Event Time": "event_time",
    "Latitude": "latitude",
    "Longitude": "longitude",
    "Country Code": "country_code",
    "Advertiser Id": "advertiser_id",
    "App Id": "app_id",
    "Device Brand": "device_brand",
    "Device Model": "device_model",
    "Platform": "platform",
    "Entity Id": "entity_id",
    "Satellite Provider": "satellite_provider",
}

# Columns that should be converted to TIMESTAMP
TIMESTAMP_COLUMNS: list[str] = ["event_time"]

# Columns that should be converted to FLOAT
FLOAT_COLUMNS: list[str] = ["latitude", "longitude"]

# Columns that should be converted to INTEGER
INT_COLUMNS: list[str] = []

# Columns that should be converted to STRING
STRING_COLUMNS: list[str] = [
    "country_code",
    "advertiser_id",
    "app_id",
    "device_brand",
    "device_model",
    "platform",
    "entity_id",
    "satellite_provider",
]

# BigQuery schema definition
BIGQUERY_SCHEMA: list[bigquery.SchemaField] = [
    bigquery.SchemaField("event_time", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("event_date", "DATE", mode="NULLABLE"),
    bigquery.SchemaField("latitude", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("longitude", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("country_code", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("advertiser_id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("app_id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("device_brand", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("device_model", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("platform", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("entity_id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("satellite_provider", "STRING", mode="NULLABLE"),
]

# Partition field for BigQuery table
PARTITION_FIELD: str = "event_date"

# Cluster fields for BigQuery table
CLUSTER_FIELDS: list[str] = ["country_code", "satellite_provider", "entity_id"]
