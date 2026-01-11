#!/usr/bin/env python3
"""
Tests for the CSV parsing module.
"""

import pytest

from app.custom_exceptions import RequestError
from app.parser import parse_csv


class TestParseCsv:
    """Tests for parse_csv function."""

    def test_parses_valid_csv_to_dataframe(self):
        """Valid CSV bytes are parsed into a DataFrame."""
        csv_bytes = b"name,age,city\nAlice,30,NYC\nBob,25,LA"

        df = parse_csv(csv_bytes, "test.csv")

        assert df is not None
        assert len(df) == 2
        assert list(df.columns) == ["name", "age", "city"]

    def test_returns_correct_row_count(self):
        """Row count matches input data."""
        rows = ["header1,header2"]
        rows.extend([f"val{i},data{i}" for i in range(100)])
        csv_bytes = "\n".join(rows).encode()

        df = parse_csv(csv_bytes, "large.csv")

        assert len(df) == 100

    def test_raises_request_error_for_malformed_csv(self):
        """Malformed CSV data raises RequestError."""
        # Binary data that's not valid CSV
        malformed_bytes = b"\x00\x01\x02\x03\xff\xfe"

        with pytest.raises(RequestError, match="Failed to parse CSV"):
            parse_csv(malformed_bytes, "bad.csv")

    def test_handles_empty_csv_with_headers_only(self):
        """CSV with headers but no data rows returns empty DataFrame."""
        csv_bytes = b"col1,col2,col3"

        df = parse_csv(csv_bytes, "empty.csv")

        assert len(df) == 0
        assert list(df.columns) == ["col1", "col2", "col3"]

    def test_preserves_column_names_from_csv(self):
        """Column names are preserved exactly as in the CSV."""
        csv_bytes = b"Document ID,Event Time,Lat,Lon,Emitter Id\n1,2024-01-01,50.0,30.0,E123"

        df = parse_csv(csv_bytes, "hawkeye.csv")

        expected_columns = ["Document ID", "Event Time", "Lat", "Lon", "Emitter Id"]
        assert list(df.columns) == expected_columns

    def test_handles_quoted_values_with_commas(self):
        """CSV with quoted values containing commas is parsed correctly."""
        csv_bytes = b'name,description,value\n"Smith, John","A, B, and C",123'

        df = parse_csv(csv_bytes, "quoted.csv")

        assert len(df) == 1
        assert df.iloc[0]["name"] == "Smith, John"
        assert df.iloc[0]["description"] == "A, B, and C"

    def test_handles_utf8_encoding(self):
        """UTF-8 encoded CSV is parsed correctly."""
        # UTF-8 encoded content with special characters
        csv_bytes = "name,city\nCafe,Paris\nMuller,Munchen".encode("utf-8")

        df = parse_csv(csv_bytes, "utf8.csv")

        assert len(df) == 2
        assert df.iloc[0]["name"] == "Cafe"
        assert df.iloc[1]["city"] == "Munchen"

    def test_includes_filename_in_error_message(self):
        """Error message includes the filename for debugging."""
        # Use data that actually causes pandas to fail (BOM + invalid UTF-8)
        malformed_bytes = b"\xff\xfe\x00\x01\x02\x03"
        filename = "problematic_file.csv"

        with pytest.raises(RequestError, match=filename):
            parse_csv(malformed_bytes, filename)

    def test_default_filename_when_not_provided(self):
        """Default filename is used when not specified."""
        # Use data that actually causes pandas to fail (BOM + invalid UTF-8)
        malformed_bytes = b"\xff\xfe\x00\x01\x02\x03"

        with pytest.raises(RequestError, match="unknown"):
            parse_csv(malformed_bytes)

    @pytest.mark.parametrize(
        ("csv_content", "expected_rows"),
        [
            pytest.param(b"h1,h2\na,b", 1, id="single_row"),
            pytest.param(b"h1,h2\na,b\nc,d\ne,f", 3, id="multiple_rows"),
            pytest.param(b"h1,h2\n", 0, id="headers_with_trailing_newline"),
        ],
    )
    def test_row_count_variations(self, csv_content, expected_rows):
        """Row count is correct for various CSV formats."""
        df = parse_csv(csv_content, "test.csv")

        assert len(df) == expected_rows

    def test_handles_numeric_columns(self):
        """Numeric values are parsed as numbers."""
        csv_bytes = b"id,value,amount\n1,42,3.14"

        df = parse_csv(csv_bytes, "numbers.csv")

        assert df.iloc[0]["id"] == 1
        assert df.iloc[0]["value"] == 42
        assert df.iloc[0]["amount"] == 3.14

    def test_handles_empty_values(self):
        """Empty values in CSV are handled correctly."""
        csv_bytes = b"name,value,optional\nAlice,42,\nBob,,data"

        df = parse_csv(csv_bytes, "sparse.csv")

        assert len(df) == 2
        # pandas represents missing values as NaN
        assert df.iloc[0]["name"] == "Alice"
        assert df.iloc[1]["name"] == "Bob"
