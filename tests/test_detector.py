"""Tests for data type detection."""

import pytest
from app.detector import DataType, detect_data_type, DETECTION_RULES


class TestDetectionRules:
    """Test detection rules configuration."""

    def test_all_data_types_have_rules(self):
        """All DataType enums should have detection rules."""
        for data_type in DataType:
            assert data_type in DETECTION_RULES

    def test_all_rules_have_required_fields(self):
        """All rules should have required_any and table fields."""
        for data_type, rules in DETECTION_RULES.items():
            assert "required_any" in rules, f"{data_type} missing required_any"
            assert "table" in rules, f"{data_type} missing table"
            assert len(rules["required_any"]) > 0, f"{data_type} has empty required_any"


class TestGPSDetection:
    """Test GPS data type detection."""

    def test_detects_gps_by_emitter_id(self):
        """GPS should be detected by Emitter Id column."""
        columns = ["Event Time", "Emitter Id", "Country", "Lat", "Lon"]
        data_type, table = detect_data_type(columns)
        assert data_type == DataType.GPS
        assert table == "gps_v5"

    def test_detects_gps_by_max_freq(self):
        """GPS should be detected by Max Freq column."""
        columns = ["Event Time", "Max Freq", "Country", "Lat", "Lon"]
        data_type, table = detect_data_type(columns)
        assert data_type == DataType.GPS
        assert table == "gps_v5"

    def test_detects_gps_by_min_freq(self):
        """GPS should be detected by Min Freq column."""
        columns = ["Event Time", "Min Freq", "Country", "Lat", "Lon"]
        data_type, table = detect_data_type(columns)
        assert data_type == DataType.GPS
        assert table == "gps_v5"


class TestVHFDetection:
    """Test VHF data type detection."""

    def test_detects_vhf_by_bandwidth(self):
        """VHF should be detected by Bandwidth column."""
        columns = ["Event Time", "Bandwidth", "Country", "Lat", "Lon"]
        data_type, table = detect_data_type(columns)
        assert data_type == DataType.VHF
        assert table == "vhf_v5"

    def test_detects_vhf_by_signal_type(self):
        """VHF should be detected by Signal Type column."""
        columns = ["Event Time", "Signal Type", "Country", "Lat", "Lon"]
        data_type, table = detect_data_type(columns)
        assert data_type == DataType.VHF
        assert table == "vhf_v5"

    def test_detects_vhf_by_burst_duration(self):
        """VHF should be detected by Burst Duration column."""
        columns = ["Event Time", "Burst Duration", "Country", "Lat", "Lon"]
        data_type, table = detect_data_type(columns)
        assert data_type == DataType.VHF
        assert table == "vhf_v5"


class TestAirdefenseDetection:
    """Test Airdefense data type detection."""

    def test_detects_airdefense_by_potential_emitter(self):
        """Airdefense should be detected by Potential Emitter column."""
        columns = ["Event Time", "Potential Emitter", "Country", "Lat", "Lon"]
        data_type, table = detect_data_type(columns)
        assert data_type == DataType.AIRDEFENSE
        assert table == "airdefense_v4"

    def test_detects_airdefense_by_country_of_origin(self):
        """Airdefense should be detected by Country Of Origin column."""
        columns = ["Event Time", "Country Of Origin", "Country", "Lat", "Lon"]
        data_type, table = detect_data_type(columns)
        assert data_type == DataType.AIRDEFENSE
        assert table == "airdefense_v4"


class TestSkytraceDetection:
    """Test Skytrace data type detection."""

    def test_detects_skytrace_by_advertiser_id(self):
        """Skytrace should be detected by Advertiser Id column."""
        columns = ["Event Time", "Advertiser Id", "Latitude", "Longitude"]
        data_type, table = detect_data_type(columns)
        assert data_type == DataType.SKYTRACE
        assert table == "skytrace_v2"

    def test_detects_skytrace_by_satellite_provider(self):
        """Skytrace should be detected by Satellite Provider column."""
        columns = ["Event Time", "Satellite Provider", "Latitude", "Longitude"]
        data_type, table = detect_data_type(columns)
        assert data_type == DataType.SKYTRACE
        assert table == "skytrace_v2"

    def test_detects_skytrace_by_app_id(self):
        """Skytrace should be detected by App Id column."""
        columns = ["Event Time", "App Id", "Latitude", "Longitude"]
        data_type, table = detect_data_type(columns)
        assert data_type == DataType.SKYTRACE
        assert table == "skytrace_v2"


class TestPortEventsDetection:
    """Test Port Events data type detection."""

    def test_detects_port_events_by_ata(self):
        """Port Events should be detected by Ata column."""
        columns = ["Event Time", "Ata", "Imo", "Port Name"]
        data_type, table = detect_data_type(columns)
        assert data_type == DataType.SHADOWFLEET_PORT_EVENTS
        assert table == "shadowfleet_port_events"

    def test_detects_port_events_by_port_name(self):
        """Port Events should be detected by Port Name column."""
        columns = ["Event Time", "Port Name", "Imo", "Latitude"]
        data_type, table = detect_data_type(columns)
        assert data_type == DataType.SHADOWFLEET_PORT_EVENTS
        assert table == "shadowfleet_port_events"

    def test_detects_port_events_by_duration_seconds(self):
        """Port Events should be detected by Duration Seconds column."""
        columns = ["Event Time", "Duration Seconds", "Imo"]
        data_type, table = detect_data_type(columns)
        assert data_type == DataType.SHADOWFLEET_PORT_EVENTS
        assert table == "shadowfleet_port_events"


class TestVesselHistoryDetection:
    """Test Vessel History data type detection."""

    def test_detects_vessel_history_by_built_year(self):
        """Vessel History should be detected by Built Year column."""
        columns = ["Event Time", "Built Year", "Imo", "Latitude"]
        data_type, table = detect_data_type(columns)
        assert data_type == DataType.SHADOWFLEET_VESSEL_HISTORY
        assert table == "shadowfleet_vessel_history"

    def test_detects_vessel_history_by_speed_knots(self):
        """Vessel History should be detected by Speed Knots column."""
        columns = ["Event Time", "Speed Knots", "Imo"]
        data_type, table = detect_data_type(columns)
        assert data_type == DataType.SHADOWFLEET_VESSEL_HISTORY
        assert table == "shadowfleet_vessel_history"

    def test_detects_vessel_history_by_navigation_status(self):
        """Vessel History should be detected by Navigation Status column."""
        columns = ["Event Time", "Navigation Status", "Imo"]
        data_type, table = detect_data_type(columns)
        assert data_type == DataType.SHADOWFLEET_VESSEL_HISTORY
        assert table == "shadowfleet_vessel_history"


class TestDetectionPriority:
    """Test detection priority (more specific types first)."""

    def test_vessel_history_detected_before_general(self):
        """Vessel History columns should be detected before generic columns."""
        # Built Year is specific to Vessel History
        columns = ["Event Time", "Built Year", "Country", "Latitude", "Longitude"]
        data_type, _ = detect_data_type(columns)
        assert data_type == DataType.SHADOWFLEET_VESSEL_HISTORY


class TestUnknownColumns:
    """Test handling of unknown column combinations."""

    def test_raises_on_unknown_columns(self):
        """Should raise ValueError for unrecognized columns."""
        columns = ["Unknown Col 1", "Unknown Col 2", "Random Data"]
        with pytest.raises(ValueError, match="Cannot detect data type"):
            detect_data_type(columns)
