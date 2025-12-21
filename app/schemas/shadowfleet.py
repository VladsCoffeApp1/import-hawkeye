#!/usr/bin/env python3
# /// script
# requires-python = "==3.12.9"
# dependencies = []
# ///

"""
SPDX-License-Identifier: LicenseRef-NonCommercial-Only
© 2025 github.com/defmon3 — Non-commercial use only. Commercial use requires permission.
Format docstrings according to PEP 287
File: shadowfleet.py

Shadowfleet schema definitions for BigQuery (Port Events and Vessel History).
Matches production tables with ALL 40 columns - KEEP ALL DATA!
"""

from google.cloud import bigquery

# =============================================================================
# PORT EVENTS SCHEMA (40 columns - KEEP ALL DATA!)
# =============================================================================

# Only drop columns that don't exist in BigQuery
PORT_EVENTS_COLUMNS_TO_DROP: list[str] = [
    "Snet Username",
]

PORT_EVENTS_COLUMN_MAPPING: dict[str, str] = {
    "Document ID": "document_id",
    "Display Name": "display_name",
    "City": "city",
    "Country": "country",
    "Event Time": "event_time",
    "Event Date": "event_date",  # CSV already has this column
    "Last Updated": "last_updated",
    "Source": "source",
    "Super Type": "super_type",
    "Type": "type",
    "_id": "_id",
    "_index": "_index",
    "Ata": "ata",
    "Atd": "atd",
    "Draught Ata": "draught_ata",
    "Draught Atd": "draught_atd",
    "Draught Change": "draught_change",
    "Duration Iso": "duration_iso",
    "Duration Seconds": "duration_seconds",
    "Duration Text": "duration_text",
    "Entity Cohort": "entity_cohort",
    "Event Id": "event_id",
    "Imo": "imo",
    "Latitude": "latitude",
    "Longitude": "longitude",
    "Mmsi": "mmsi",
    "Name": "name",
    "Port Country": "port_country",
    "Port Id": "port_id",
    "Port Name": "port_name",
    "Port Type": "port_type",
    "Port Unlocode": "port_unlocode",
    "State": "state",
    "Timestamp": "timestamp",
    "Update Timestamp": "update_timestamp",
    "Vessel Callsign": "vessel_callsign",
    "Vessel Category": "vessel_category",
    "Vessel Category Description": "vessel_category_description",
    "Vessel Id": "vessel_id",
    "Vessel Type": "vessel_type",
}

PORT_EVENTS_TIMESTAMP_COLUMNS: list[str] = [
    "event_time",
    "last_updated",
    "ata",
    "atd",
    "timestamp",
    "update_timestamp",
]

PORT_EVENTS_FLOAT_COLUMNS: list[str] = [
    "draught_ata",
    "draught_atd",
    "draught_change",
    "latitude",
    "longitude",
]

PORT_EVENTS_INT_COLUMNS: list[str] = [
    "duration_seconds",
]

PORT_EVENTS_STRING_COLUMNS: list[str] = [
    "document_id",
    "display_name",
    "city",
    "country",
    "source",
    "super_type",
    "type",
    "_id",
    "_index",
    "duration_iso",
    "duration_text",
    "entity_cohort",
    "event_id",
    "imo",  # STRING not INT64!
    "mmsi",
    "name",
    "port_country",
    "port_id",
    "port_name",
    "port_type",
    "port_unlocode",
    "state",
    "vessel_callsign",
    "vessel_category",
    "vessel_category_description",
    "vessel_id",
    "vessel_type",
]

PORT_EVENTS_BIGQUERY_SCHEMA: list[bigquery.SchemaField] = [
    bigquery.SchemaField("document_id", "STRING", mode="REQUIRED"),  # REQUIRED to match existing table
    bigquery.SchemaField("display_name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("city", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("country", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("event_time", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("event_date", "DATE", mode="NULLABLE"),
    bigquery.SchemaField("last_updated", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("source", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("super_type", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("type", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("_id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("_index", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("ata", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("atd", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("draught_ata", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("draught_atd", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("draught_change", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("duration_iso", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("duration_seconds", "INT64", mode="NULLABLE"),
    bigquery.SchemaField("duration_text", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("entity_cohort", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("event_id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("imo", "STRING", mode="NULLABLE"),  # STRING not INT64!
    bigquery.SchemaField("latitude", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("longitude", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("mmsi", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("port_country", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("port_id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("port_name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("port_type", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("port_unlocode", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("state", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("timestamp", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("update_timestamp", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("vessel_callsign", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("vessel_category", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("vessel_category_description", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("vessel_id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("vessel_type", "STRING", mode="NULLABLE"),
]

PORT_EVENTS_PARTITION_FIELD: str = "event_date"

PORT_EVENTS_CLUSTER_FIELDS: list[str] = ["port_country", "vessel_category", "imo"]

# =============================================================================
# VESSEL HISTORY SCHEMA (40 columns - KEEP ALL DATA!)
# =============================================================================

# Only drop columns that don't exist in BigQuery
VESSEL_HISTORY_COLUMNS_TO_DROP: list[str] = [
    "Snet Username",
    "Class 2 Code",
    "Registered Owner",
]

VESSEL_HISTORY_COLUMN_MAPPING: dict[str, str] = {
    "Document ID": "document_id",
    "Display Name": "display_name",
    "City": "city",
    "Country": "country",
    "Event Time": "event_time",
    "Event Date": "event_date",  # CSV already has this column
    "Last Updated": "last_updated",
    "Source": "source",
    "Super Type": "super_type",
    "Type": "type",
    "_id": "_id",
    "_index": "_index",
    "Built Year": "built_year",
    "Class 1 Code": "class_1_code",
    "Collection Type": "collection_type",
    "Commercial Owner": "commercial_owner",
    "Course Degrees": "course_degrees",
    "Draught Meters": "draught_meters",
    "Entity Cohort": "entity_cohort",
    "Geohash": "geohash",
    "Heading Degrees": "heading_degrees",
    "Hull Number": "hull_number",
    "Imo": "imo",
    "Iso Code": "iso_code",
    "Iso Code Flag": "iso_code_flag",
    "Latitude": "latitude",
    "Length Overall Meters": "length_overall_meters",
    "Longitude": "longitude",
    "Meta Row Id": "meta_row_id",
    "Mmsi": "mmsi",
    "Name": "name",
    "Navigation Status": "navigation_status",
    "Position Timestamp": "position_timestamp",
    "Raw Destination": "raw_destination",
    "Speed Knots": "speed_knots",
    "Vessel Callsign": "vessel_callsign",
    "Vessel Category": "vessel_category",
    "Vessel Category Description": "vessel_category_description",
    "Vessel Subtype": "vessel_subtype",
    "Vessel Type": "vessel_type",
}

VESSEL_HISTORY_TIMESTAMP_COLUMNS: list[str] = [
    "event_time",
    "last_updated",
    "position_timestamp",
]

VESSEL_HISTORY_FLOAT_COLUMNS: list[str] = [
    "latitude",
    "longitude",
    "speed_knots",
    "course_degrees",
    "heading_degrees",
    "draught_meters",
    "length_overall_meters",
]

VESSEL_HISTORY_INT_COLUMNS: list[str] = [
    "built_year",
]

VESSEL_HISTORY_STRING_COLUMNS: list[str] = [
    "document_id",
    "display_name",
    "city",
    "country",
    "source",
    "super_type",
    "type",
    "_id",
    "_index",
    "class_1_code",
    "collection_type",
    "commercial_owner",
    "entity_cohort",
    "geohash",
    "hull_number",
    "imo",  # STRING not INT64!
    "iso_code",
    "iso_code_flag",
    "meta_row_id",
    "mmsi",
    "name",
    "navigation_status",
    "raw_destination",
    "vessel_callsign",
    "vessel_category",
    "vessel_category_description",
    "vessel_subtype",
    "vessel_type",
]

VESSEL_HISTORY_BIGQUERY_SCHEMA: list[bigquery.SchemaField] = [
    bigquery.SchemaField("document_id", "STRING", mode="REQUIRED"),  # REQUIRED to match existing table
    bigquery.SchemaField("display_name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("city", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("country", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("event_time", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("event_date", "DATE", mode="NULLABLE"),
    bigquery.SchemaField("last_updated", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("source", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("super_type", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("type", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("_id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("_index", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("built_year", "INT64", mode="NULLABLE"),
    bigquery.SchemaField("class_1_code", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("collection_type", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("commercial_owner", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("course_degrees", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("draught_meters", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("entity_cohort", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("geohash", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("heading_degrees", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("hull_number", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("imo", "STRING", mode="NULLABLE"),  # STRING not INT64!
    bigquery.SchemaField("iso_code", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("iso_code_flag", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("latitude", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("length_overall_meters", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("longitude", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("meta_row_id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("mmsi", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("navigation_status", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("position_timestamp", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("raw_destination", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("speed_knots", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("vessel_callsign", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("vessel_category", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("vessel_category_description", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("vessel_subtype", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("vessel_type", "STRING", mode="NULLABLE"),
]

VESSEL_HISTORY_PARTITION_FIELD: str = "event_date"

VESSEL_HISTORY_CLUSTER_FIELDS: list[str] = ["vessel_category", "imo", "country"]
