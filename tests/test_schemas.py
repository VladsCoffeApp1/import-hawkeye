"""Tests for schema definitions - KEEP ALL DATA!"""

import pytest
from app.detector import DataType
from app.schemas import get_schema
from app.schemas import gps, vhf, airdefense, skytrace, shadowfleet


class TestGPSSchema:
    """Test GPS schema has all required columns."""

    def test_gps_has_29_columns(self):
        """GPS schema must have 29 BigQuery columns (matching gps_v3)."""
        assert len(gps.BIGQUERY_SCHEMA) == 29

    def test_gps_column_mapping_complete(self):
        """All CSV columns should be mapped."""
        assert len(gps.COLUMN_MAPPING) >= 27

    def test_gps_only_drops_internal_columns(self):
        """GPS should only drop internal columns."""
        allowed_drops = {"_id", "_index", "Snet Username", "Image Id"}
        assert set(gps.COLUMNS_TO_DROP).issubset(allowed_drops)

    def test_gps_has_all_type_columns(self):
        """All type columns must exist in column mapping values."""
        mapped_cols = set(gps.COLUMN_MAPPING.values())
        for col in gps.TIMESTAMP_COLUMNS:
            assert col in mapped_cols or col == "event_date"
        for col in gps.FLOAT_COLUMNS:
            assert col in mapped_cols
        for col in gps.INT_COLUMNS:
            assert col in mapped_cols
        for col in gps.STRING_COLUMNS:
            assert col in mapped_cols


class TestVHFSchema:
    """Test VHF schema has all required columns."""

    def test_vhf_has_31_columns(self):
        """VHF schema must have 31 BigQuery columns (matching vhf_v3)."""
        assert len(vhf.BIGQUERY_SCHEMA) == 31

    def test_vhf_column_mapping_complete(self):
        """All CSV columns should be mapped."""
        assert len(vhf.COLUMN_MAPPING) >= 29

    def test_vhf_only_drops_internal_columns(self):
        """VHF should only drop internal columns."""
        allowed_drops = {"_id", "_index", "Snet Username", "Image Id", "Coordinates"}
        assert set(vhf.COLUMNS_TO_DROP).issubset(allowed_drops)


class TestAirdefenseSchema:
    """Test Airdefense schema has all required columns."""

    def test_airdefense_has_26_columns(self):
        """Airdefense schema must have 26 BigQuery columns (matching airdefense_v2)."""
        assert len(airdefense.BIGQUERY_SCHEMA) == 26

    def test_airdefense_column_mapping_complete(self):
        """All CSV columns should be mapped."""
        assert len(airdefense.COLUMN_MAPPING) >= 24

    def test_airdefense_only_drops_internal_columns(self):
        """Airdefense should only drop internal columns."""
        allowed_drops = {"_id", "_index", "Snet Username"}
        assert set(airdefense.COLUMNS_TO_DROP).issubset(allowed_drops)


class TestSkytraceSchema:
    """Test Skytrace schema."""

    def test_skytrace_has_schema(self):
        """Skytrace should have BigQuery schema."""
        assert len(skytrace.BIGQUERY_SCHEMA) >= 10

    def test_skytrace_column_mapping(self):
        """Skytrace should have column mapping."""
        assert len(skytrace.COLUMN_MAPPING) >= 10


class TestShadowfleetPortEventsSchema:
    """Test Shadowfleet Port Events schema - KEEP ALL 40 COLUMNS!"""

    def test_port_events_has_40_columns(self):
        """Port Events schema must have 40 BigQuery columns."""
        assert len(shadowfleet.PORT_EVENTS_BIGQUERY_SCHEMA) == 40

    def test_port_events_column_mapping_complete(self):
        """All CSV columns should be mapped (40 columns including Event Date)."""
        assert len(shadowfleet.PORT_EVENTS_COLUMN_MAPPING) == 40

    def test_port_events_only_drops_snet_username(self):
        """Port Events should only drop Snet Username."""
        assert shadowfleet.PORT_EVENTS_COLUMNS_TO_DROP == ["Snet Username"]

    def test_port_events_imo_is_string(self):
        """IMO field must be STRING not INT64."""
        assert "imo" in shadowfleet.PORT_EVENTS_STRING_COLUMNS
        assert "imo" not in shadowfleet.PORT_EVENTS_INT_COLUMNS
        # Check BigQuery schema
        imo_field = next(
            (f for f in shadowfleet.PORT_EVENTS_BIGQUERY_SCHEMA if f.name == "imo"),
            None
        )
        assert imo_field is not None
        assert imo_field.field_type == "STRING"


class TestShadowfleetVesselHistorySchema:
    """Test Shadowfleet Vessel History schema - KEEP ALL 40 COLUMNS!"""

    def test_vessel_history_has_40_columns(self):
        """Vessel History schema must have 40 BigQuery columns."""
        assert len(shadowfleet.VESSEL_HISTORY_BIGQUERY_SCHEMA) == 40

    def test_vessel_history_column_mapping_complete(self):
        """All CSV columns should be mapped (40 columns including Event Date)."""
        assert len(shadowfleet.VESSEL_HISTORY_COLUMN_MAPPING) == 40

    def test_vessel_history_only_drops_required(self):
        """Vessel History should only drop required columns."""
        required_drops = {"Snet Username", "Class 2 Code", "Registered Owner"}
        assert set(shadowfleet.VESSEL_HISTORY_COLUMNS_TO_DROP) == required_drops

    def test_vessel_history_imo_is_string(self):
        """IMO field must be STRING not INT64."""
        assert "imo" in shadowfleet.VESSEL_HISTORY_STRING_COLUMNS
        assert "imo" not in shadowfleet.VESSEL_HISTORY_INT_COLUMNS
        # Check BigQuery schema
        imo_field = next(
            (f for f in shadowfleet.VESSEL_HISTORY_BIGQUERY_SCHEMA if f.name == "imo"),
            None
        )
        assert imo_field is not None
        assert imo_field.field_type == "STRING"


class TestGetSchema:
    """Test schema retrieval function."""

    @pytest.mark.parametrize("data_type", list(DataType))
    def test_get_schema_returns_valid_config(self, data_type):
        """All data types should return valid schema config."""
        schema = get_schema(data_type)
        assert schema.columns_to_drop is not None
        assert schema.column_mapping is not None
        assert schema.timestamp_columns is not None
        assert schema.float_columns is not None
        assert schema.int_columns is not None
        assert schema.string_columns is not None
        assert schema.bigquery_schema is not None
        assert schema.partition_field is not None
        assert schema.cluster_fields is not None

    def test_no_overlap_between_drop_and_map(self):
        """Columns to drop should not be in column mapping."""
        for data_type in DataType:
            schema = get_schema(data_type)
            drop_set = set(schema.columns_to_drop)
            map_keys = set(schema.column_mapping.keys())
            overlap = drop_set & map_keys
            assert not overlap, f"{data_type}: overlap {overlap}"
