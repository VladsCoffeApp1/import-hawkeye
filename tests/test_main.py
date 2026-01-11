#!/usr/bin/env python3
"""
Integration tests for the main module.
"""

import io
import zipfile
from unittest.mock import MagicMock, patch


class TestMain:
    """Integration tests for main Cloud Function entry point."""

    def create_test_zip(self, csv_content: str) -> bytes:
        """Create a ZIP file with CSV content."""
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("data.csv", csv_content)
        return buffer.getvalue()

    @patch("app.conductor.load_to_bigquery")
    def test_processes_vhf_upload(self, mock_load):
        """VHF data is processed correctly from upload."""
        from app.main import main

        mock_load.return_value = 2

        csv_content = """Event Time,Latitude,Longitude,Bandwidth,Signal Type,Country
2024-01-15 10:30:00,48.8566,2.3522,25000,FM,France
2024-01-15 11:45:00,51.5074,-0.1278,12500,AM,UK"""

        zip_data = self.create_test_zip(csv_content)

        request = MagicMock()
        request.get_json.return_value = {}
        request.files = {}
        request.data = zip_data

        result = main(request)

        assert result["status"] == "success"
        assert "vhf" in result["message"].lower()
        mock_load.assert_called_once()

    @patch("app.conductor.load_to_bigquery")
    def test_processes_gps_upload(self, mock_load):
        """GPS data is processed correctly from upload."""
        from app.main import main

        mock_load.return_value = 2

        csv_content = """Event Time,Latitude,Longitude,Emitter Id,Max Freq,Min Freq
2024-01-15 10:30:00,48.8566,2.3522,GPS-001,1575420000,1575400000
2024-01-15 11:45:00,51.5074,-0.1278,GPS-002,1575450000,1575380000"""

        zip_data = self.create_test_zip(csv_content)

        request = MagicMock()
        request.get_json.return_value = {}
        request.files = {}
        request.data = zip_data

        result = main(request)

        assert result["status"] == "success"
        assert "gps" in result["message"].lower()

    @patch("app.conductor.load_to_bigquery")
    def test_processes_airdefense_upload(self, mock_load):
        """Airdefense data is processed correctly from upload."""
        from app.main import main

        mock_load.return_value = 1

        csv_content = """Event Time,Latitude,Longitude,Potential Emitter,Country Of Origin
2024-01-15 10:30:00,48.8566,2.3522,SA-11,Russia"""

        zip_data = self.create_test_zip(csv_content)

        request = MagicMock()
        request.get_json.return_value = {}
        request.files = {}
        request.data = zip_data

        result = main(request)

        assert result["status"] == "success"
        assert "airdefense" in result["message"].lower()

    @patch("app.conductor.load_to_bigquery")
    def test_processes_skytrace_upload(self, mock_load):
        """Skytrace data is processed correctly from upload."""
        from app.main import main

        mock_load.return_value = 1

        csv_content = """Event Time,Latitude,Longitude,Advertiser Id,Satellite Provider
2024-01-15 10:30:00,48.8566,2.3522,adv-123,Starlink"""

        zip_data = self.create_test_zip(csv_content)

        request = MagicMock()
        request.get_json.return_value = {}
        request.files = {}
        request.data = zip_data

        result = main(request)

        assert result["status"] == "success"
        assert "skytrace" in result["message"].lower()

    @patch("app.conductor.load_to_bigquery")
    def test_processes_port_events_upload(self, mock_load):
        """Port events data is processed correctly from upload."""
        from app.main import main

        mock_load.return_value = 1

        csv_content = """Event Time,Imo,Vessel Name,Port Name,Ata,Duration Seconds
2024-01-15 10:30:00,9876543,TANKER ONE,Rotterdam,2024-01-14 08:00:00,95400"""

        zip_data = self.create_test_zip(csv_content)

        request = MagicMock()
        request.get_json.return_value = {}
        request.files = {}
        request.data = zip_data

        result = main(request)

        assert result["status"] == "success"
        assert "port_events" in result["message"].lower()

    @patch("app.conductor.load_to_bigquery")
    def test_processes_vessel_history_upload(self, mock_load):
        """Vessel history data is processed correctly from upload."""
        from app.main import main

        mock_load.return_value = 1

        csv_content = """Event Time,Imo,Vessel Name,Built Year,Speed Knots,Navigation Status
2024-01-15 10:30:00,9876543,TANKER ONE,2015,12.5,Under Way Using Engine"""

        zip_data = self.create_test_zip(csv_content)

        request = MagicMock()
        request.get_json.return_value = {}
        request.files = {}
        request.data = zip_data

        result = main(request)

        assert result["status"] == "success"
        assert "vessel_history" in result["message"].lower()

    def test_returns_error_on_invalid_zip(self):
        """Error is returned for invalid ZIP file."""
        from app.main import main

        request = MagicMock()
        request.get_json.return_value = {}
        request.files = {}
        request.data = b"not a zip file"

        result = main(request)

        assert result["status"] == "error"
        assert "failed" in result["message"].lower()

    def test_returns_error_on_unknown_columns(self):
        """Error is returned when columns don't match any type."""
        from app.main import main

        csv_content = """Col1,Col2,Col3
val1,val2,val3"""

        zip_data = self.create_test_zip(csv_content)

        request = MagicMock()
        request.get_json.return_value = {}
        request.files = {}
        request.data = zip_data

        result = main(request)

        assert result["status"] == "error"
        assert "failed" in result["message"].lower()
