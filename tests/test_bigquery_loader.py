#!/usr/bin/env python3
"""
Tests for the BigQuery loader module.
"""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from app.detector import DataType


class TestLoadToBigQuery:
    """Tests for load_to_bigquery function."""

    @patch("app.bigquery_loader.bigquery.Client")
    def test_loads_via_staging_and_merge(self, mock_client_class):
        """Data is loaded via staging table and MERGE for deduplication."""
        from app.bigquery_loader import load_to_bigquery

        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        mock_table = MagicMock()
        mock_client.get_table.return_value = mock_table

        mock_load_job = MagicMock()
        mock_client.load_table_from_dataframe.return_value = mock_load_job

        mock_merge_job = MagicMock()
        mock_merge_job.num_dml_affected_rows = 10
        mock_client.query.return_value = mock_merge_job

        df = pd.DataFrame(
            {
                "event_time": ["2024-01-15 10:30:00"],
                "latitude": [48.8566],
                "longitude": [2.3522],
                "bandwidth": [25000],
            }
        )

        result = load_to_bigquery(
            df=df,
            data_type=DataType.VHF,
            project="test-project",
            dataset="test-dataset",
            table="test-table",
        )

        # Verify staging table load
        mock_client.load_table_from_dataframe.assert_called_once()
        call_args = mock_client.load_table_from_dataframe.call_args
        assert "test-table_staging" in call_args[0][1]

        # Verify MERGE query executed with dedup key columns
        mock_client.query.assert_called_once()
        merge_query = mock_client.query.call_args[0][0]
        assert "MERGE" in merge_query
        # VHF dedup key: latitude, longitude, event_time, frequency
        assert "latitude" in merge_query
        assert "longitude" in merge_query

        # Verify staging table cleanup
        mock_client.delete_table.assert_called_once()

        assert result == 10

    @patch("app.bigquery_loader.bigquery.Client")
    def test_uses_write_truncate_for_staging(self, mock_client_class):
        """Staging table uses WRITE_TRUNCATE to replace previous data."""
        from app.bigquery_loader import load_to_bigquery

        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        mock_table = MagicMock()
        mock_client.get_table.return_value = mock_table

        mock_load_job = MagicMock()
        mock_client.load_table_from_dataframe.return_value = mock_load_job

        mock_merge_job = MagicMock()
        mock_merge_job.num_dml_affected_rows = 5
        mock_client.query.return_value = mock_merge_job

        df = pd.DataFrame(
            {
                "event_time": ["2024-01-15 10:30:00"],
                "latitude": [48.8566],
            }
        )

        load_to_bigquery(
            df=df,
            data_type=DataType.VHF,
            table="test-table",
        )

        call_args = mock_client.load_table_from_dataframe.call_args
        job_config = call_args[1]["job_config"]

        from google.cloud import bigquery

        assert job_config.write_disposition == bigquery.WriteDisposition.WRITE_TRUNCATE

    @patch("app.bigquery_loader.bigquery.Client")
    def test_creates_table_if_not_exists(self, mock_client_class):
        """Table is created if it doesn't exist."""
        from app.bigquery_loader import load_to_bigquery

        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        # Simulate table not found
        mock_client.get_table.side_effect = Exception("Not found")

        mock_table = MagicMock()
        mock_client.create_table.return_value = mock_table

        mock_load_job = MagicMock()
        mock_client.load_table_from_dataframe.return_value = mock_load_job

        mock_merge_job = MagicMock()
        mock_merge_job.num_dml_affected_rows = 5
        mock_client.query.return_value = mock_merge_job

        df = pd.DataFrame(
            {
                "event_time": ["2024-01-15 10:30:00"],
                "latitude": [48.8566],
            }
        )

        load_to_bigquery(
            df=df,
            data_type=DataType.VHF,
            table="test-table",
        )

        mock_client.create_table.assert_called_once()

    def test_raises_on_missing_table_name(self):
        """LoadError is raised when table name is missing."""
        from app.bigquery_loader import load_to_bigquery
        from app.custom_exceptions import LoadError

        df = pd.DataFrame({"col": [1]})

        with pytest.raises(LoadError, match="Table name is required"):
            load_to_bigquery(df=df, data_type=DataType.VHF, table=None)

    @patch("app.bigquery_loader.bigquery.Client")
    def test_propagates_bigquery_errors(self, mock_client_class):
        """BigQuery errors are propagated as LoadError."""
        from app.bigquery_loader import load_to_bigquery
        from app.custom_exceptions import LoadError

        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        mock_table = MagicMock()
        mock_client.get_table.return_value = mock_table

        mock_client.load_table_from_dataframe.side_effect = Exception("BQ Error")

        df = pd.DataFrame({"col": [1]})

        with pytest.raises(LoadError, match="Failed to load data"):
            load_to_bigquery(df=df, data_type=DataType.VHF, table="test-table")

    @patch("app.bigquery_loader.bigquery.Client")
    def test_skips_duplicates(self, mock_client_class):
        """Duplicate rows are skipped and logged."""
        from app.bigquery_loader import load_to_bigquery

        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        mock_table = MagicMock()
        mock_client.get_table.return_value = mock_table

        mock_load_job = MagicMock()
        mock_client.load_table_from_dataframe.return_value = mock_load_job

        # Simulate 2 out of 5 rows being new
        mock_merge_job = MagicMock()
        mock_merge_job.num_dml_affected_rows = 2
        mock_client.query.return_value = mock_merge_job

        df = pd.DataFrame(
            {
                "document_id": ["a", "b", "c", "d", "e"],
                "event_time": ["2024-01-15"] * 5,
                "latitude": [1.0, 2.0, 3.0, 4.0, 5.0],
            }
        )

        result = load_to_bigquery(
            df=df,
            data_type=DataType.VHF,
            table="test-table",
        )

        # Only 2 new rows inserted
        assert result == 2


class TestGetOrCreateTable:
    """Tests for get_or_create_table function."""

    @patch("app.bigquery_loader.bigquery.Client")
    def test_returns_existing_table(self, mock_client_class):
        """Returns existing table without creating."""
        from app.bigquery_loader import get_or_create_table

        mock_client = MagicMock()
        mock_table = MagicMock()
        mock_client.get_table.return_value = mock_table

        result = get_or_create_table(
            client=mock_client,
            project="test-project",
            dataset="test-dataset",
            table="test-table",
            data_type=DataType.VHF,
        )

        assert result == mock_table
        mock_client.create_table.assert_not_called()

    @patch("app.bigquery_loader.bigquery.Client")
    def test_creates_new_table_with_partitioning(self, mock_client_class):
        """Creates new table with partitioning when not found."""
        from app.bigquery_loader import get_or_create_table

        mock_client = MagicMock()
        mock_client.get_table.side_effect = Exception("Not found")

        mock_new_table = MagicMock()
        mock_client.create_table.return_value = mock_new_table

        result = get_or_create_table(
            client=mock_client,
            project="test-project",
            dataset="test-dataset",
            table="test-table",
            data_type=DataType.VHF,
        )

        mock_client.create_table.assert_called_once()
        assert result == mock_new_table
