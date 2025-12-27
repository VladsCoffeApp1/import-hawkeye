#!/usr/bin/env python3
# /// script
# requires-python = "==3.12.9"
# dependencies = []
# ///

"""
SPDX-License-Identifier: LicenseRef-NonCommercial-Only
© 2025 github.com/defmon3 — Non-commercial use only. Commercial use requires permission.
Format docstrings according to PEP 287
File: __init__.py

Schema registry for all data types.
"""

from typing import Any

from . import airdefense, gps, shadowfleet, skytrace, vhf
from ..detector import DataType


class SchemaConfig:
    """Container for schema configuration."""

    def __init__(
        self,
        columns_to_drop: list[str],
        column_mapping: dict[str, str],
        timestamp_columns: list[str],
        float_columns: list[str],
        int_columns: list[str],
        string_columns: list[str],
        bigquery_schema: list,
        partition_field: str,
        cluster_fields: list[str],
        dedup_key: list[str],
    ):
        self.columns_to_drop = columns_to_drop
        self.column_mapping = column_mapping
        self.timestamp_columns = timestamp_columns
        self.float_columns = float_columns
        self.int_columns = int_columns
        self.string_columns = string_columns
        self.bigquery_schema = bigquery_schema
        self.partition_field = partition_field
        self.cluster_fields = cluster_fields
        self.dedup_key = dedup_key


def get_schema(data_type: DataType) -> SchemaConfig:
    """
    Get schema configuration for a data type.

    :param data_type: The detected data type
    :returns: SchemaConfig with all configuration
    """
    if data_type == DataType.VHF:
        return SchemaConfig(
            columns_to_drop=vhf.COLUMNS_TO_DROP,
            column_mapping=vhf.COLUMN_MAPPING,
            timestamp_columns=vhf.TIMESTAMP_COLUMNS,
            float_columns=vhf.FLOAT_COLUMNS,
            int_columns=vhf.INT_COLUMNS,
            string_columns=vhf.STRING_COLUMNS,
            bigquery_schema=vhf.BIGQUERY_SCHEMA,
            partition_field=vhf.PARTITION_FIELD,
            cluster_fields=vhf.CLUSTER_FIELDS,
            dedup_key=vhf.DEDUP_KEY,
        )
    elif data_type == DataType.GPS:
        return SchemaConfig(
            columns_to_drop=gps.COLUMNS_TO_DROP,
            column_mapping=gps.COLUMN_MAPPING,
            timestamp_columns=gps.TIMESTAMP_COLUMNS,
            float_columns=gps.FLOAT_COLUMNS,
            int_columns=gps.INT_COLUMNS,
            string_columns=gps.STRING_COLUMNS,
            bigquery_schema=gps.BIGQUERY_SCHEMA,
            partition_field=gps.PARTITION_FIELD,
            cluster_fields=gps.CLUSTER_FIELDS,
            dedup_key=gps.DEDUP_KEY,
        )
    elif data_type == DataType.AIRDEFENSE:
        return SchemaConfig(
            columns_to_drop=airdefense.COLUMNS_TO_DROP,
            column_mapping=airdefense.COLUMN_MAPPING,
            timestamp_columns=airdefense.TIMESTAMP_COLUMNS,
            float_columns=airdefense.FLOAT_COLUMNS,
            int_columns=airdefense.INT_COLUMNS,
            string_columns=airdefense.STRING_COLUMNS,
            bigquery_schema=airdefense.BIGQUERY_SCHEMA,
            partition_field=airdefense.PARTITION_FIELD,
            cluster_fields=airdefense.CLUSTER_FIELDS,
            dedup_key=airdefense.DEDUP_KEY,
        )
    elif data_type == DataType.SKYTRACE:
        return SchemaConfig(
            columns_to_drop=skytrace.COLUMNS_TO_DROP,
            column_mapping=skytrace.COLUMN_MAPPING,
            timestamp_columns=skytrace.TIMESTAMP_COLUMNS,
            float_columns=skytrace.FLOAT_COLUMNS,
            int_columns=skytrace.INT_COLUMNS,
            string_columns=skytrace.STRING_COLUMNS,
            bigquery_schema=skytrace.BIGQUERY_SCHEMA,
            partition_field=skytrace.PARTITION_FIELD,
            cluster_fields=skytrace.CLUSTER_FIELDS,
            dedup_key=skytrace.DEDUP_KEY,
        )
    elif data_type == DataType.SHADOWFLEET_PORT_EVENTS:
        return SchemaConfig(
            columns_to_drop=shadowfleet.PORT_EVENTS_COLUMNS_TO_DROP,
            column_mapping=shadowfleet.PORT_EVENTS_COLUMN_MAPPING,
            timestamp_columns=shadowfleet.PORT_EVENTS_TIMESTAMP_COLUMNS,
            float_columns=shadowfleet.PORT_EVENTS_FLOAT_COLUMNS,
            int_columns=shadowfleet.PORT_EVENTS_INT_COLUMNS,
            string_columns=shadowfleet.PORT_EVENTS_STRING_COLUMNS,
            bigquery_schema=shadowfleet.PORT_EVENTS_BIGQUERY_SCHEMA,
            partition_field=shadowfleet.PORT_EVENTS_PARTITION_FIELD,
            cluster_fields=shadowfleet.PORT_EVENTS_CLUSTER_FIELDS,
            dedup_key=shadowfleet.PORT_EVENTS_DEDUP_KEY,
        )
    elif data_type == DataType.SHADOWFLEET_VESSEL_HISTORY:
        return SchemaConfig(
            columns_to_drop=shadowfleet.VESSEL_HISTORY_COLUMNS_TO_DROP,
            column_mapping=shadowfleet.VESSEL_HISTORY_COLUMN_MAPPING,
            timestamp_columns=shadowfleet.VESSEL_HISTORY_TIMESTAMP_COLUMNS,
            float_columns=shadowfleet.VESSEL_HISTORY_FLOAT_COLUMNS,
            int_columns=shadowfleet.VESSEL_HISTORY_INT_COLUMNS,
            string_columns=shadowfleet.VESSEL_HISTORY_STRING_COLUMNS,
            bigquery_schema=shadowfleet.VESSEL_HISTORY_BIGQUERY_SCHEMA,
            partition_field=shadowfleet.VESSEL_HISTORY_PARTITION_FIELD,
            cluster_fields=shadowfleet.VESSEL_HISTORY_CLUSTER_FIELDS,
            dedup_key=shadowfleet.VESSEL_HISTORY_DEDUP_KEY,
        )
    else:
        raise ValueError(f"Unknown data type: {data_type}")
