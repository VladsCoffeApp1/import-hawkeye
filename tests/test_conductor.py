#!/usr/bin/env python3
"""
Tests for the pipeline conductor module.
"""

import zipfile
from io import BytesIO
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from app.conductor import (
    PipelineResult,
    ProcessedCSV,
    _process_single_csv,
    run,
)
from app.custom_exceptions import RequestError
from app.detector import DataType


def create_zip(files: dict[str, bytes]) -> bytes:
    """
    Create an in-memory ZIP archive with the given files.

    :param files: Dict of filename -> contents
    :returns: ZIP file as bytes
    """
    buffer = BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        for filename, content in files.items():
            zf.writestr(filename, content)
    return buffer.getvalue()


class TestProcessedCSV:
    """Tests for ProcessedCSV dataclass."""

    def test_success_property_true_when_no_error(self):
        """Success is True when error is None."""
        processed = ProcessedCSV(
            filename="test.csv",
            data_type=DataType.GPS,
            table="gps",
            rows_loaded=100,
        )

        assert processed.success is True

    def test_success_property_false_when_error_set(self):
        """Success is False when error is set."""
        processed = ProcessedCSV(
            filename="test.csv",
            error="Something went wrong",
        )

        assert processed.success is False

    def test_default_values(self):
        """Default values are set correctly."""
        processed = ProcessedCSV(filename="test.csv")

        assert processed.filename == "test.csv"
        assert processed.data_type is None
        assert processed.table is None
        assert processed.df is None
        assert processed.rows_loaded == 0
        assert processed.error is None

    def test_stores_dataframe(self):
        """DataFrame is stored correctly."""
        df = pd.DataFrame({"col1": [1, 2, 3]})
        processed = ProcessedCSV(filename="test.csv", df=df)

        assert processed.df is not None
        assert len(processed.df) == 3


class TestPipelineResult:
    """Tests for PipelineResult dataclass."""

    def test_successful_property_filters_successes(self):
        """Successful property returns only successful ProcessedCSVs."""
        result = PipelineResult(
            processed=[
                ProcessedCSV(filename="good1.csv", rows_loaded=10),
                ProcessedCSV(filename="bad.csv", error="Failed"),
                ProcessedCSV(filename="good2.csv", rows_loaded=20),
            ]
        )

        successful = result.successful

        assert len(successful) == 2
        assert all(p.success for p in successful)

    def test_failed_property_filters_failures(self):
        """Failed property returns only failed ProcessedCSVs."""
        result = PipelineResult(
            processed=[
                ProcessedCSV(filename="good.csv", rows_loaded=10),
                ProcessedCSV(filename="bad1.csv", error="Error 1"),
                ProcessedCSV(filename="bad2.csv", error="Error 2"),
            ]
        )

        failed = result.failed

        assert len(failed) == 2
        assert all(not p.success for p in failed)

    def test_message_format_success_only(self):
        """Message format for successful-only results."""
        result = PipelineResult(
            processed=[
                ProcessedCSV(
                    filename="gps.csv",
                    data_type=DataType.GPS,
                    table="gps",
                    rows_loaded=100,
                ),
                ProcessedCSV(
                    filename="vhf.csv",
                    data_type=DataType.VHF,
                    table="vhf",
                    rows_loaded=50,
                ),
            ],
            total_rows=150,
        )

        msg = result.message

        assert "150 total rows" in msg
        assert "100 gps rows to gps" in msg
        assert "50 vhf rows to vhf" in msg
        assert "Failed" not in msg

    def test_message_format_with_failures(self):
        """Message format includes failures."""
        result = PipelineResult(
            processed=[
                ProcessedCSV(
                    filename="good.csv",
                    data_type=DataType.GPS,
                    table="gps",
                    rows_loaded=100,
                ),
                ProcessedCSV(
                    filename="bad.csv",
                    error="Parse failed",
                ),
            ],
            total_rows=100,
        )

        msg = result.message

        assert "100 total rows" in msg
        assert "Failed" in msg
        assert "bad.csv: Parse failed" in msg

    def test_empty_result(self):
        """Empty result has default values."""
        result = PipelineResult()

        assert result.processed == []
        assert result.total_rows == 0
        assert result.successful == []
        assert result.failed == []


class TestProcessSingleCsv:
    """Tests for _process_single_csv function."""

    @patch("app.conductor.load_to_bigquery")
    @patch("app.conductor.clean_dataframe")
    @patch("app.conductor.detect_data_type")
    @patch("app.conductor.parse_csv")
    def test_processes_csv_successfully(
        self,
        mock_parse,
        mock_detect,
        mock_transform,
        mock_load,
    ):
        """CSV is processed through the full pipeline."""
        # Arrange
        df_original = pd.DataFrame({"Emitter Id": ["E1"], "Event Time": ["2024-01-01"]})
        df_cleaned = pd.DataFrame({"emitter_id": ["E1"], "event_time": ["2024-01-01"]})

        mock_parse.return_value = df_original
        mock_detect.return_value = (DataType.GPS, "gps")
        mock_transform.return_value = df_cleaned
        mock_load.return_value = 1

        # Act
        result = _process_single_csv("test.csv", b"csv,data", dry_run=False)

        # Assert
        assert result.success is True
        assert result.data_type == DataType.GPS
        assert result.table == "gps"
        assert result.rows_loaded == 1
        mock_load.assert_called_once()

    @patch("app.conductor.parse_csv")
    def test_handles_parse_error(self, mock_parse):
        """Parse errors are captured in the result."""
        mock_parse.side_effect = RequestError("Invalid CSV")

        result = _process_single_csv("bad.csv", b"invalid", dry_run=False)

        assert result.success is False
        assert "Invalid CSV" in result.error

    @patch("app.conductor.detect_data_type")
    @patch("app.conductor.parse_csv")
    def test_handles_detection_error(self, mock_parse, mock_detect):
        """Detection errors are captured in the result."""
        mock_parse.return_value = pd.DataFrame({"unknown": [1]})
        mock_detect.side_effect = ValueError("Cannot detect")

        result = _process_single_csv("unknown.csv", b"data", dry_run=False)

        assert result.success is False
        assert "Cannot detect" in result.error

    @patch("app.conductor.clean_dataframe")
    @patch("app.conductor.detect_data_type")
    @patch("app.conductor.parse_csv")
    def test_dry_run_skips_bigquery_loading(
        self,
        mock_parse,
        mock_detect,
        mock_transform,
    ):
        """Dry run mode skips BigQuery loading."""
        df = pd.DataFrame({"col": [1, 2, 3]})
        mock_parse.return_value = df
        mock_detect.return_value = (DataType.GPS, "gps")
        mock_transform.return_value = df

        with patch("app.conductor.load_to_bigquery") as mock_load:
            result = _process_single_csv("test.csv", b"data", dry_run=True)

            mock_load.assert_not_called()
            assert result.rows_loaded == 3  # Uses df length

    @patch("app.conductor.load_to_bigquery")
    @patch("app.conductor.clean_dataframe")
    @patch("app.conductor.detect_data_type")
    @patch("app.conductor.parse_csv")
    def test_handles_load_error(
        self,
        mock_parse,
        mock_detect,
        mock_transform,
        mock_load,
    ):
        """Load errors are captured in the result."""
        from app.custom_exceptions import LoadError

        df = pd.DataFrame({"col": [1]})
        mock_parse.return_value = df
        mock_detect.return_value = (DataType.GPS, "gps")
        mock_transform.return_value = df
        mock_load.side_effect = LoadError("BQ connection failed")

        result = _process_single_csv("test.csv", b"data", dry_run=False)

        assert result.success is False
        assert "BQ connection failed" in result.error


class TestRun:
    """Tests for run function (full pipeline)."""

    @patch("app.conductor._process_single_csv")
    @patch("app.conductor.extract_csv_files")
    def test_run_processes_all_csvs_from_zip(self, mock_extract, mock_process):
        """All CSVs from ZIP are processed."""
        mock_extract.return_value = [
            ("file1.csv", b"csv1"),
            ("file2.csv", b"csv2"),
            ("file3.csv", b"csv3"),
        ]
        mock_process.return_value = ProcessedCSV(
            filename="test.csv",
            data_type=DataType.GPS,
            table="gps",
            rows_loaded=10,
        )

        result = run(b"zip_data", dry_run=True)

        assert mock_process.call_count == 3
        assert len(result.processed) == 3

    @patch("app.conductor._process_single_csv")
    @patch("app.conductor.extract_csv_files")
    def test_run_continues_on_individual_csv_failure(self, mock_extract, mock_process):
        """Pipeline continues processing after individual CSV failure."""
        mock_extract.return_value = [
            ("good1.csv", b"csv1"),
            ("bad.csv", b"csv2"),
            ("good2.csv", b"csv3"),
        ]

        # Return different results for each call
        mock_process.side_effect = [
            ProcessedCSV(filename="good1.csv", data_type=DataType.GPS, table="gps", rows_loaded=10),
            ProcessedCSV(filename="bad.csv", error="Parse failed"),
            ProcessedCSV(filename="good2.csv", data_type=DataType.VHF, table="vhf", rows_loaded=20),
        ]

        result = run(b"zip_data", dry_run=True)

        assert mock_process.call_count == 3
        assert len(result.successful) == 2
        assert len(result.failed) == 1
        assert result.total_rows == 30

    @patch("app.conductor.extract_csv_files")
    def test_run_raises_on_zip_extraction_failure(self, mock_extract):
        """ZIP extraction failure raises exception (critical failure)."""
        mock_extract.side_effect = RequestError("Invalid ZIP")

        with pytest.raises(RequestError, match="Invalid ZIP"):
            run(b"bad_zip")

    @patch("app.conductor._process_single_csv")
    @patch("app.conductor.extract_csv_files")
    def test_dry_run_passed_to_process_single_csv(self, mock_extract, mock_process):
        """Dry run flag is passed to _process_single_csv."""
        mock_extract.return_value = [("test.csv", b"csv")]
        mock_process.return_value = ProcessedCSV(filename="test.csv", data_type=DataType.GPS, table="gps", rows_loaded=5)

        run(b"zip_data", dry_run=True)

        mock_process.assert_called_once_with("test.csv", b"csv", True)

    @patch("app.conductor._process_single_csv")
    @patch("app.conductor.extract_csv_files")
    def test_total_rows_accumulates_from_successful(self, mock_extract, mock_process):
        """Total rows is sum of successful rows_loaded."""
        mock_extract.return_value = [
            ("f1.csv", b"1"),
            ("f2.csv", b"2"),
            ("f3.csv", b"3"),
        ]
        mock_process.side_effect = [
            ProcessedCSV(filename="f1.csv", data_type=DataType.GPS, table="gps", rows_loaded=100),
            ProcessedCSV(filename="f2.csv", error="failed"),  # Not counted
            ProcessedCSV(filename="f3.csv", data_type=DataType.VHF, table="vhf", rows_loaded=200),
        ]

        result = run(b"zip_data", dry_run=True)

        assert result.total_rows == 300


class TestRunIntegration:
    """Integration tests for run function with real modules."""

    def test_run_with_valid_gps_csv_dry_run(self, gps_csv_columns):
        """Full pipeline runs with valid GPS CSV in dry run mode."""
        # Create a valid GPS CSV
        header = ",".join(gps_csv_columns)
        values = ",".join(["val"] * len(gps_csv_columns))
        csv_content = f"{header}\n{values}".encode()

        zip_data = create_zip({"hawkeye_gps.csv": csv_content})

        result = run(zip_data, dry_run=True)

        assert len(result.processed) == 1
        assert result.successful[0].data_type == DataType.GPS
        assert result.total_rows == 1

    def test_run_with_valid_vhf_csv_dry_run(self, vhf_csv_columns):
        """Full pipeline runs with valid VHF CSV in dry run mode."""
        header = ",".join(vhf_csv_columns)
        values = ",".join(["val"] * len(vhf_csv_columns))
        csv_content = f"{header}\n{values}".encode()

        zip_data = create_zip({"hawkeye_vhf.csv": csv_content})

        result = run(zip_data, dry_run=True)

        assert len(result.processed) == 1
        assert result.successful[0].data_type == DataType.VHF
        assert result.total_rows == 1

    def test_run_with_multiple_data_types_dry_run(
        self,
        gps_csv_columns,
        vhf_csv_columns,
    ):
        """Pipeline handles multiple data types in one ZIP."""
        gps_header = ",".join(gps_csv_columns)
        gps_values = ",".join(["val"] * len(gps_csv_columns))
        gps_csv = f"{gps_header}\n{gps_values}".encode()

        vhf_header = ",".join(vhf_csv_columns)
        vhf_values = ",".join(["val"] * len(vhf_csv_columns))
        vhf_csv = f"{vhf_header}\n{vhf_values}".encode()

        zip_data = create_zip({
            "gps_data.csv": gps_csv,
            "vhf_data.csv": vhf_csv,
        })

        result = run(zip_data, dry_run=True)

        assert len(result.processed) == 2
        data_types = {p.data_type for p in result.successful}
        assert DataType.GPS in data_types
        assert DataType.VHF in data_types
