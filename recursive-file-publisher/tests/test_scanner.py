"""Tests for scanner module."""

import tempfile
from pathlib import Path

import pytest

from recursive_file_publisher.scanner import iter_files, file_to_message


class TestIterFiles:
    """Tests for iter_files function."""

    def test_iter_files_finds_all_files(self):
        """Test that iter_files finds all files in a directory tree."""
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)

            # Create test file structure
            (root / "file1.txt").write_text("content1")
            (root / "file2.log").write_text("content2")

            subdir = root / "subdir"
            subdir.mkdir()
            (subdir / "file3.txt").write_text("content3")

            nested = subdir / "nested"
            nested.mkdir()
            (nested / "file4.dat").write_text("content4")

            # Collect all files
            files = list(iter_files(root))
            file_names = {f.name for f in files}

            # Verify all files are found
            assert len(files) == 4
            assert file_names == {"file1.txt", "file2.log", "file3.txt", "file4.dat"}

    def test_iter_files_skips_directories(self):
        """Test that iter_files does not yield directories."""
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)

            # Create directories and files
            (root / "file.txt").write_text("content")
            (root / "subdir1").mkdir()
            (root / "subdir2").mkdir()

            files = list(iter_files(root))

            # Only the file should be yielded
            assert len(files) == 1
            assert files[0].name == "file.txt"

    def test_iter_files_handles_empty_directory(self):
        """Test that iter_files handles empty directories gracefully."""
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)

            files = list(iter_files(root))

            assert len(files) == 0

    def test_iter_files_raises_on_non_directory(self):
        """Test that iter_files raises ValueError for non-directory paths."""
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            file_path = root / "file.txt"
            file_path.write_text("content")

            with pytest.raises(ValueError, match="not a directory"):
                list(iter_files(file_path))


class TestFileToMessage:
    """Tests for file_to_message function."""

    def test_file_to_message_contains_required_fields(self):
        """Test that file_to_message returns all required fields."""
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            file_path = root / "test.txt"
            file_path.write_text("hello world")

            message = file_to_message(file_path)

            # Check all required fields are present
            assert "path" in message
            assert "name" in message
            assert "size_bytes" in message
            assert "modified_ts" in message

    def test_file_to_message_has_correct_name(self):
        """Test that file_to_message returns the correct filename."""
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            file_path = root / "myfile.txt"
            file_path.write_text("content")

            message = file_to_message(file_path)

            assert message["name"] == "myfile.txt"

    def test_file_to_message_has_absolute_path(self):
        """Test that file_to_message returns an absolute path."""
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            file_path = root / "test.txt"
            file_path.write_text("content")

            message = file_to_message(file_path)

            # Path should be absolute
            assert Path(message["path"]).is_absolute()
            assert message["path"] == str(file_path.resolve())

    def test_file_to_message_has_correct_size(self):
        """Test that file_to_message returns the correct file size."""
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            file_path = root / "test.txt"
            content = "hello world"
            file_path.write_text(content)

            message = file_to_message(file_path)

            assert message["size_bytes"] == len(content.encode("utf-8"))
            assert message["size_bytes"] >= 0

    def test_file_to_message_has_valid_timestamp(self):
        """Test that file_to_message returns a valid timestamp."""
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            file_path = root / "test.txt"
            file_path.write_text("content")

            message = file_to_message(file_path)

            # Timestamp should be a string (ISO format)
            assert isinstance(message["modified_ts"], str)
            # Should contain date-time separator
            assert "T" in message["modified_ts"] or " " in message["modified_ts"]
