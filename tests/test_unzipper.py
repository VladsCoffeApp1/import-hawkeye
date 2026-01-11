#!/usr/bin/env python3
"""
Tests for the ZIP extraction module.
"""

import zipfile
from io import BytesIO

import pytest

from app.custom_exceptions import RequestError
from app.unzipper import extract_csv_files


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


class TestExtractCsvFiles:
    """Tests for extract_csv_files function."""

    def test_extracts_single_csv_from_valid_zip(self):
        """Single CSV file is extracted correctly."""
        csv_content = b"col1,col2\nval1,val2"
        zip_data = create_zip({"data.csv": csv_content})

        result = extract_csv_files(zip_data)

        assert len(result) == 1
        filename, content = result[0]
        assert filename == "data.csv"
        assert content == csv_content

    def test_extracts_multiple_csvs_from_zip(self):
        """Multiple CSV files are all extracted."""
        csv1 = b"col1\nval1"
        csv2 = b"col2\nval2"
        csv3 = b"col3\nval3"
        zip_data = create_zip({
            "file1.csv": csv1,
            "file2.csv": csv2,
            "file3.csv": csv3,
        })

        result = extract_csv_files(zip_data)

        assert len(result) == 3
        filenames = [name for name, _ in result]
        assert set(filenames) == {"file1.csv", "file2.csv", "file3.csv"}

    def test_raises_request_error_for_invalid_zip(self):
        """Invalid ZIP data raises RequestError."""
        invalid_data = b"not a zip file at all"

        with pytest.raises(RequestError, match="Invalid ZIP file"):
            extract_csv_files(invalid_data)

    def test_raises_request_error_when_no_csvs_in_zip(self):
        """ZIP with no CSV files raises RequestError."""
        zip_data = create_zip({
            "readme.txt": b"Just a text file",
            "data.json": b'{"key": "value"}',
        })

        with pytest.raises(RequestError, match="No CSV file found"):
            extract_csv_files(zip_data)

    def test_filters_only_csv_files_from_mixed_zip(self):
        """Only CSV files are extracted from a ZIP with mixed content."""
        csv_content = b"col1,col2\nval1,val2"
        zip_data = create_zip({
            "data.csv": csv_content,
            "readme.txt": b"Documentation",
            "config.json": b'{"setting": true}',
            "image.png": b"\x89PNG\r\n",
        })

        result = extract_csv_files(zip_data)

        assert len(result) == 1
        filename, content = result[0]
        assert filename == "data.csv"
        assert content == csv_content

    def test_csv_matching_is_case_insensitive(self):
        """CSV file extension matching is case-insensitive."""
        csv_lower = b"lower,case\n1,2"
        csv_upper = b"upper,case\n3,4"
        csv_mixed = b"mixed,case\n5,6"
        zip_data = create_zip({
            "lowercase.csv": csv_lower,
            "uppercase.CSV": csv_upper,
            "mixedcase.CsV": csv_mixed,
        })

        result = extract_csv_files(zip_data)

        assert len(result) == 3
        filenames = [name for name, _ in result]
        assert "lowercase.csv" in filenames
        assert "uppercase.CSV" in filenames
        assert "mixedcase.CsV" in filenames

    def test_preserves_csv_content_integrity(self):
        """Extracted CSV content matches original exactly."""
        # Include various special characters and encodings
        csv_content = b"name,value\n\"quoted,value\",123\nspecial\xc3\xa9,456"
        zip_data = create_zip({"special.csv": csv_content})

        result = extract_csv_files(zip_data)

        _, content = result[0]
        assert content == csv_content

    def test_handles_nested_csv_in_subdirectory(self):
        """CSV files in subdirectories are extracted."""
        csv_content = b"col1,col2\nval1,val2"
        zip_data = create_zip({
            "folder/subfolder/data.csv": csv_content,
        })

        result = extract_csv_files(zip_data)

        assert len(result) == 1
        filename, content = result[0]
        assert filename == "folder/subfolder/data.csv"
        assert content == csv_content

    def test_empty_zip_raises_request_error(self):
        """Empty ZIP file raises RequestError."""
        zip_data = create_zip({})

        with pytest.raises(RequestError, match="No CSV file found"):
            extract_csv_files(zip_data)
