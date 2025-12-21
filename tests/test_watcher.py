#!/usr/bin/env python3
"""
Tests for the watcher module.
"""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


class TestGetNewFiles:
    """Tests for get_new_files function."""

    def test_finds_zip_files(self, tmp_path):
        """Finds ZIP files in directory."""
        with patch("watcher.WATCH_DIR", tmp_path):
            from watcher import get_new_files

            # Create test files
            (tmp_path / "file1.zip").touch()
            (tmp_path / "file2.zip").touch()
            (tmp_path / "file3.txt").touch()

            files = get_new_files()

            assert len(files) == 2
            assert all(f.suffix == ".zip" for f in files)

    def test_returns_empty_for_nonexistent_dir(self):
        """Returns empty list if directory doesn't exist."""
        with patch("watcher.WATCH_DIR", Path("/nonexistent/path")):
            from watcher import get_new_files

            files = get_new_files()
            assert files == []


class TestSendToGcf:
    """Tests for send_to_gcf function."""

    @patch("watcher.settings")
    @patch("watcher.httpx.post")
    def test_successful_upload(self, mock_post, mock_settings, tmp_path):
        """Successful upload returns True."""
        from watcher import send_to_gcf

        mock_settings.gcf_url = "https://example.com/function"

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"status": "success", "message": "Loaded 10 rows"}
        mock_post.return_value = mock_response

        test_file = tmp_path / "test.zip"
        test_file.write_bytes(b"test data")

        success, message = send_to_gcf(test_file)

        assert success is True
        assert "10 rows" in message

    @patch("watcher.settings")
    @patch("watcher.httpx.post")
    def test_failed_upload_keeps_file(self, mock_post, mock_settings, tmp_path):
        """Failed upload returns False."""
        from watcher import send_to_gcf

        mock_settings.gcf_url = "https://example.com/function"

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"status": "error", "message": "Failed"}
        mock_post.return_value = mock_response

        test_file = tmp_path / "test.zip"
        test_file.write_bytes(b"test data")

        success, message = send_to_gcf(test_file)

        assert success is False
        assert "Failed" in message

    @patch("watcher.settings")
    @patch("watcher.httpx.post")
    def test_timeout_handling(self, mock_post, mock_settings, tmp_path):
        """Timeout is handled gracefully."""
        import httpx
        from watcher import send_to_gcf

        mock_settings.gcf_url = "https://example.com/function"
        mock_post.side_effect = httpx.TimeoutException("Timeout")

        test_file = tmp_path / "test.zip"
        test_file.write_bytes(b"test data")

        success, message = send_to_gcf(test_file)

        assert success is False
        assert "timed out" in message.lower()

    @patch("watcher.settings")
    def test_raises_without_gcf_url(self, mock_settings, tmp_path):
        """Raises WatcherError if GCF_URL not set."""
        from watcher import send_to_gcf
        from app.custom_exceptions import WatcherError

        mock_settings.gcf_url = None

        test_file = tmp_path / "test.zip"
        test_file.write_bytes(b"test data")

        with pytest.raises(WatcherError, match="GCF_URL is not configured"):
            send_to_gcf(test_file)


class TestProcessFile:
    """Tests for process_file function."""

    @patch("watcher.send_to_gcf")
    @patch("watcher.is_file_stable")
    def test_keeps_file_on_success_deletion_disabled(self, mock_stable, mock_send, tmp_path):
        """File is KEPT after successful upload (deletion temporarily disabled)."""
        from watcher import process_file

        mock_stable.return_value = True
        mock_send.return_value = (True, "Success")

        test_file = tmp_path / "test.zip"
        test_file.write_bytes(b"test data")

        result = process_file(test_file)

        assert result is True
        # File is kept because deletion is temporarily disabled
        assert test_file.exists()

    @patch("watcher.send_to_gcf")
    @patch("watcher.is_file_stable")
    def test_keeps_file_on_failure(self, mock_stable, mock_send, tmp_path):
        """File is kept after failed upload."""
        from watcher import process_file

        mock_stable.return_value = True
        mock_send.return_value = (False, "Failed")

        test_file = tmp_path / "test.zip"
        test_file.write_bytes(b"test data")

        result = process_file(test_file)

        assert result is False
        assert test_file.exists()

    @patch("watcher.is_file_stable")
    def test_skips_unstable_file(self, mock_stable, tmp_path):
        """Unstable files are skipped."""
        from watcher import process_file

        mock_stable.return_value = False

        test_file = tmp_path / "test.zip"
        test_file.write_bytes(b"test data")

        result = process_file(test_file)

        assert result is False
        assert test_file.exists()
