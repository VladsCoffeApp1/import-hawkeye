"""Pytest fixtures for import-hawkeye tests."""

import pandas as pd
import pytest
from pathlib import Path


@pytest.fixture
def fixtures_dir() -> Path:
    """Return the path to the fixtures directory."""
    return Path(__file__).parent / "fixtures"


@pytest.fixture
def gps_csv_columns() -> list[str]:
    """Sample GPS CSV columns from actual data."""
    return [
        "Document ID", "Display Name", "Id", "Emitter Id", "Pass Group Id",
        "City", "Country", "Lat", "Lon", "Detected Location", "Event Time",
        "Last Updated", "Received At", "Source", "Super Type", "Type",
        "Constellation", "Ellipse Area", "Frequency", "Max Freq", "Min Freq",
        "Num Bursts", "Orientation", "Semi Major", "Semi Minor", "Soi",
        "Version", "Power Level", "_id", "_index", "Snet Username",
    ]


@pytest.fixture
def vhf_csv_columns() -> list[str]:
    """Sample VHF CSV columns from actual data."""
    return [
        "Document ID", "Display Name", "Id", "Pass Group Id", "City", "Country",
        "Lat", "Lon", "Detected Location", "Event Time", "Last Updated",
        "Received At", "Source", "Super Type", "Type", "Constellation",
        "Bandwidth", "Burst Duration", "Ellipse Area",
        "Estimated Transmit Frequency Hz", "Frequency", "Num Bursts",
        "Orientation", "Semi Major", "Semi Minor", "Signal Type", "Soi",
        "Sync Type", "Version", "Emitter Id", "_id", "_index", "Snet Username",
    ]


@pytest.fixture
def airdefense_csv_columns() -> list[str]:
    """Sample Airdefense CSV columns from actual data."""
    return [
        "Document ID", "Display Name", "Id", "Image Id", "Pass Group Id",
        "City", "Country", "Lat", "Lon", "Event Time", "Last Updated",
        "Received At", "Source", "Super Type", "Type", "Constellation",
        "Country Of Origin", "Detection Confidence", "Potential Emitter",
        "Potential Emitter Nato", "Potential Purpose",
        "Potential Weapon System Association", "Orientation", "Semi Major",
        "Semi Minor", "_id", "_index", "Snet Username",
    ]


@pytest.fixture
def skytrace_csv_columns() -> list[str]:
    """Sample Skytrace CSV columns from actual data."""
    return [
        "Document ID", "Display Name", "City", "Country", "Event Time",
        "Last Updated", "Source", "Super Type", "Type", "Latitude", "Longitude",
        "Country Code", "Advertiser Id", "App Id", "Device Brand",
        "Device Model", "Platform", "Entity Id", "Satellite Provider",
        "_id", "_index", "Snet Username",
    ]


@pytest.fixture
def port_events_csv_columns() -> list[str]:
    """Sample Shadowfleet Port Events CSV columns."""
    return [
        "Document ID", "Display Name", "City", "Country", "Event Time",
        "Last Updated", "Source", "Super Type", "Type", "_id", "_index",
        "Ata", "Atd", "Draught Ata", "Draught Atd", "Draught Change",
        "Duration Iso", "Duration Seconds", "Duration Text", "Entity Cohort",
        "Event Id", "Imo", "Latitude", "Longitude", "Mmsi", "Name",
        "Port Country", "Port Id", "Port Name", "Port Type", "Port Unlocode",
        "State", "Timestamp", "Update Timestamp", "Vessel Callsign",
        "Vessel Category", "Vessel Category Description", "Vessel Id",
        "Vessel Type", "Snet Username",
    ]


@pytest.fixture
def vessel_history_csv_columns() -> list[str]:
    """Sample Shadowfleet Vessel History CSV columns."""
    return [
        "Document ID", "Display Name", "City", "Country", "Event Time",
        "Last Updated", "Source", "Super Type", "Type", "_id", "_index",
        "Built Year", "Class 1 Code", "Collection Type", "Commercial Owner",
        "Course Degrees", "Draught Meters", "Entity Cohort", "Geohash",
        "Heading Degrees", "Hull Number", "Imo", "Iso Code", "Iso Code Flag",
        "Latitude", "Length Overall Meters", "Longitude", "Meta Row Id",
        "Mmsi", "Name", "Navigation Status", "Position Timestamp",
        "Raw Destination", "Speed Knots", "Vessel Callsign", "Vessel Category",
        "Vessel Category Description", "Vessel Subtype", "Vessel Type",
        "Snet Username", "Class 2 Code", "Registered Owner",
    ]
