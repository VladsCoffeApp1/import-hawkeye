#!/usr/bin/env python3
# /// script
# requires-python = "==3.12.9"
# dependencies = []
# ///

"""
SPDX-License-Identifier: LicenseRef-NonCommercial-Only
© 2025 github.com/defmon3 — Non-commercial use only. Commercial use requires permission.
Format docstrings according to PEP 287
File: detector.py

Data type detection from CSV column headers.
"""

from enum import Enum

from loguru import logger as log


class DataType(str, Enum):
    """Supported data types for CSV import."""

    VHF = "vhf"
    GPS = "gps"
    AIRDEFENSE = "airdefense"
    SKYTRACE = "skytrace"
    SHADOWFLEET_PORT_EVENTS = "port_events"
    SHADOWFLEET_VESSEL_HISTORY = "vessel_history"


# Detection rules ordered from most specific to least specific
# Order matters: more specific types must be checked first
DETECTION_RULES: dict[DataType, dict] = {
    # Shadowfleet types (very specific columns)
    DataType.SHADOWFLEET_VESSEL_HISTORY: {
        "required_any": ["Built Year", "Speed Knots", "Navigation Status"],
        "table": "shadowfleet_vessel_history",
    },
    DataType.SHADOWFLEET_PORT_EVENTS: {
        "required_any": ["Ata", "Port Name", "Duration Seconds"],
        "table": "shadowfleet_port_events",
    },
    # Airdefense (specific military columns)
    DataType.AIRDEFENSE: {
        "required_any": ["Potential Emitter", "Country Of Origin"],
        "table": "airdefense",
    },
    # Skytrace (mobile/satellite specific)
    DataType.SKYTRACE: {
        "required_any": ["Advertiser Id", "Satellite Provider", "App Id"],
        "table": "skytrace",
    },
    # VHF (signal-specific columns)
    DataType.VHF: {
        "required_any": ["Bandwidth", "Signal Type", "Burst Duration"],
        "table": "vhf",
    },
    # GPS (fallback for generic Hawkeye signal data)
    DataType.GPS: {
        "required_any": ["Emitter Id", "Max Freq", "Min Freq"],
        "table": "gps",
    },
}


def detect_data_type(columns: list[str]) -> tuple[DataType, str]:
    """
    Detect data type from CSV column headers.

    :param columns: List of column names from CSV header
    :returns: Tuple of (DataType enum, target_table_name)
    :raises ValueError: If no matching data type found
    """
    column_set = set(columns)
    log.debug(f"Detecting data type from {len(columns)} columns: {columns[:5]}...")

    for data_type, rules in DETECTION_RULES.items():
        matched_cols = [col for col in rules["required_any"] if col in column_set]
        if matched_cols:
            log.info(f"Detected {data_type.value} (matched: {matched_cols})")
            return data_type, rules["table"]

    log.error(f"No matching data type for columns: {columns[:10]}")
    raise ValueError(f"Cannot detect data type from columns: {columns[:10]}...")
