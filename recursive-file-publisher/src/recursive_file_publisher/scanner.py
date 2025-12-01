"""File system scanning utilities."""

import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable

logger = logging.getLogger(__name__)


def iter_files(root: Path) -> Iterable[Path]:
    """Recursively iterate over all files under the given root directory.

    This function yields files one at a time without loading all paths into memory,
    making it suitable for scanning large directory trees.

    Args:
        root: Root directory to scan

    Yields:
        Path objects for each file found (directories are skipped)

    Raises:
        ValueError: If root is not a directory
    """
    if not root.is_dir():
        raise ValueError(f"Root path is not a directory: {root}")

    logger.debug(f"Starting recursive scan of: {root}")

    try:
        for path in root.rglob("*"):
            try:
                # Skip directories
                if path.is_dir():
                    continue

                # Skip broken symlinks
                if path.is_symlink() and not path.exists():
                    logger.warning(f"Skipping broken symlink: {path}")
                    continue

                yield path

            except (PermissionError, OSError) as e:
                logger.warning(f"Error accessing {path}: {e}")
                continue

    except (PermissionError, OSError) as e:
        logger.error(f"Error scanning directory {root}: {e}")
        raise


def file_to_message(path: Path) -> Dict[str, any]:
    """Convert a file path to a JSON-serializable message.

    Args:
        path: File path to convert

    Returns:
        Dictionary containing file metadata:
            - path: Absolute path as string
            - name: Filename
            - size_bytes: File size in bytes
            - modified_ts: Last modification timestamp (ISO 8601 format)

    Raises:
        OSError: If file metadata cannot be accessed
    """
    try:
        stat = path.stat()
        absolute_path = path.resolve()

        # Convert timestamp to ISO 8601 format for better interoperability
        modified_dt = datetime.fromtimestamp(stat.st_mtime)
        modified_iso = modified_dt.isoformat()

        return {
            "path": str(absolute_path),
            "name": path.name,
            "size_bytes": stat.st_size,
            "modified_ts": modified_iso,
        }

    except (OSError, ValueError) as e:
        logger.error(f"Error creating message for {path}: {e}")
        raise
