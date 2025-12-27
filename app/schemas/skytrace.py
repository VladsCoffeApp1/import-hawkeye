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
Matches production table with ALL 53 columns - KEEP ALL DATA!
"""

from google.cloud import bigquery

# Columns to drop from the CSV before loading
COLUMNS_TO_DROP: list[str] = [
    "Snet Username",
    # Duplicate columns created by pandas when CSV has duplicate headers
    "Event Time.1",
    "Mac.1",
    "Name.1",
    "Tech.1",
]

# Column name mapping: CSV column name -> BigQuery column name
COLUMN_MAPPING: dict[str, str] = {
    "Document ID": "document_id",
    "Display Name": "display_name",
    "City": "city",
    "Country": "country",
    "Country Code": "country_code",
    "Latitude": "latitude",
    "Longitude": "longitude",
    "Altitude": "altitude",
    "Event Time": "event_time",
    "Last Updated": "last_updated",
    "Event Time Local": "event_time_local",
    "Event Timezone": "event_timezone",
    "First Seen": "first_seen",
    "Last Seen": "last_seen",
    "Time": "time",
    "Day Month Year Local": "day_month_year_local",
    "Day Of Week": "day_of_week",
    "Day Of Week Local": "day_of_week_local",
    "Hour Local": "hour_local",
    "Minute Local": "minute_local",
    "Source": "source",
    "Super Type": "super_type",
    "Type": "type",
    "_id": "_id",
    "_index": "_index",
    "Meta Row Id": "meta_row_id",
    "Advertiser Id": "advertiser_id",
    "App Id": "app_id",
    "Carrier": "carrier",
    "Device Brand": "device_brand",
    "Device Model": "device_model",
    "Platform": "platform",
    "User Agent": "user_agent",
    "Locale": "locale",
    "Entity Age": "entity_age",
    "Entity Id": "entity_id",
    "Entity Type": "entity_type",
    "Event Location Accuracy Confidence": "event_location_accuracy_confidence",
    "Event Location Accuracy Score": "event_location_accuracy_score",
    "Horizontal Accuracy": "horizontal_accuracy",
    "Vertical Accuracy": "vertical_accuracy",
    "Heading": "heading",
    "Speed": "speed",
    "Loc At": "loc_at",
    "Ip V 4": "ip_v4",
    "Connected Wifi Vendor Name": "connected_wifi_vendor_name",
    "Wifi Bssid": "wifi_bssid",
    "Wifi Ssid": "wifi_ssid",
    "Rssi": "rssi",
    "Satellite Provider": "satellite_provider",
    "Satellite Provider Country": "satellite_provider_country",
    "Satellite Provider Type": "satellite_provider_type",
    "Name": "name",
    "Mac": "mac",
    "Tech": "tech",
    "Device Os": "device_os",
    "Device Os Version": "device_os_version",
    "Ip V 6": "ip_v6",
    "Site Id": "site_id",
    "Vendor Name": "vendor_name",
}

# Columns that should be converted to TIMESTAMP
TIMESTAMP_COLUMNS: list[str] = [
    "event_time",
    "last_updated",
    "event_time_local",
    "first_seen",
    "last_seen",
    "time",
]

# Columns that should be converted to FLOAT
FLOAT_COLUMNS: list[str] = [
    "latitude",
    "longitude",
    "altitude",
    "horizontal_accuracy",
    "vertical_accuracy",
    "heading",
    "speed",
]

# Columns that should be converted to INTEGER
INT_COLUMNS: list[str] = [
    "hour_local",
    "minute_local",
    "entity_age",
    "event_location_accuracy_score",
    "rssi",
]

# Columns that should be converted to STRING
STRING_COLUMNS: list[str] = [
    "document_id",
    "display_name",
    "city",
    "country",
    "country_code",
    "event_timezone",
    "day_month_year_local",
    "day_of_week",
    "day_of_week_local",
    "source",
    "super_type",
    "type",
    "_id",
    "_index",
    "meta_row_id",
    "advertiser_id",
    "app_id",
    "carrier",
    "device_brand",
    "device_model",
    "platform",
    "user_agent",
    "locale",
    "entity_id",
    "entity_type",
    "event_location_accuracy_confidence",
    "loc_at",
    "ip_v4",
    "connected_wifi_vendor_name",
    "wifi_bssid",
    "wifi_ssid",
    "satellite_provider",
    "satellite_provider_country",
    "satellite_provider_type",
    "name",
    "mac",
    "tech",
    "device_os",
    "device_os_version",
    "ip_v6",
    "site_id",
    "vendor_name",
]

# BigQuery schema definition - matches production table
BIGQUERY_SCHEMA: list[bigquery.SchemaField] = [
    bigquery.SchemaField("document_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("display_name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("city", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("country", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("country_code", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("latitude", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("longitude", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("altitude", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("event_time", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("event_date", "DATE", mode="NULLABLE"),
    bigquery.SchemaField("last_updated", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("event_time_local", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("event_timezone", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("first_seen", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("last_seen", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("time", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("day_month_year_local", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("day_of_week", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("day_of_week_local", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("hour_local", "INT64", mode="NULLABLE"),
    bigquery.SchemaField("minute_local", "INT64", mode="NULLABLE"),
    bigquery.SchemaField("source", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("super_type", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("type", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("_id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("_index", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("meta_row_id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("advertiser_id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("app_id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("carrier", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("device_brand", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("device_model", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("platform", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("user_agent", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("locale", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("entity_age", "INT64", mode="NULLABLE"),
    bigquery.SchemaField("entity_id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("entity_type", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("event_location_accuracy_confidence", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("event_location_accuracy_score", "INT64", mode="NULLABLE"),
    bigquery.SchemaField("horizontal_accuracy", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("vertical_accuracy", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("heading", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("speed", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("loc_at", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("ip_v4", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("connected_wifi_vendor_name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("wifi_bssid", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("wifi_ssid", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("rssi", "INT64", mode="NULLABLE"),
    bigquery.SchemaField("satellite_provider", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("satellite_provider_country", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("satellite_provider_type", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("mac", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("tech", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("device_os", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("device_os_version", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("ip_v6", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("site_id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("vendor_name", "STRING", mode="NULLABLE"),
]

# Partition field for BigQuery table
PARTITION_FIELD: str = "event_date"

# Cluster fields for BigQuery table
CLUSTER_FIELDS: list[str] = ["country_code", "satellite_provider", "entity_id"]

# Deduplication key columns (natural key for MERGE)
DEDUP_KEY: list[str] = ["latitude", "longitude", "event_time", "entity_id"]
