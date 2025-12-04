"""Command-line interface for recursive file publisher."""

import argparse
import logging
import sys
from pathlib import Path

from .config import RabbitConfig
from .rabbit import RabbitClient
from .scanner import iter_files, file_to_message

logger = logging.getLogger(__name__)


def setup_logging(level: str = "INFO") -> None:
    """Configure logging with timestamp and level.

    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    """
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments.

    Returns:
        Parsed arguments namespace
    """
    parser = argparse.ArgumentParser(
        prog="recursive-file-publisher",
        description="Recursively scan a directory and publish file metadata to RabbitMQ",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # Required arguments
    parser.add_argument(
        "--root",
        type=str,
        required=True,
        help="Root directory to scan (required)",
    )

    # RabbitMQ configuration
    parser.add_argument(
        "--rabbit-host",
        type=str,
        default="localhost",
        help="RabbitMQ host (default: localhost, env: RABBITMQ_HOST)",
    )
    parser.add_argument(
        "--rabbit-port",
        type=int,
        default=5672,
        help="RabbitMQ port (default: 5672, env: RABBITMQ_PORT)",
    )
    parser.add_argument(
        "--rabbit-user",
        type=str,
        default="guest",
        help="RabbitMQ username (default: guest, env: RABBITMQ_USER)",
    )
    parser.add_argument(
        "--rabbit-password",
        type=str,
        default="guest",
        help="RabbitMQ password (default: guest, env: RABBITMQ_PASSWORD)",
    )
    parser.add_argument(
        "--rabbit-vhost",
        type=str,
        default="/",
        help="RabbitMQ virtual host (default: /, env: RABBITMQ_VHOST)",
    )
    parser.add_argument(
        "--queue",
        type=str,
        default="file_events",
        help="Queue name (default: file_events, env: RABBITMQ_QUEUE)",
    )

    # Operational flags
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Scan and log messages without publishing to RabbitMQ",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging level (default: INFO)",
    )

    return parser.parse_args()


def main() -> int:
    """Main entry point for the CLI.

    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    args = parse_args()

    # Setup logging first
    setup_logging(args.log_level)

    logger.info("Starting recursive file publisher")
    logger.info(f"Root directory: {args.root}")
    logger.info(f"Dry run mode: {args.dry_run}")

    # Validate root directory
    root_path = Path(args.root)
    if not root_path.exists():
        logger.error(f"Root path does not exist: {args.root}")
        return 1

    if not root_path.is_dir():
        logger.error(f"Root path is not a directory: {args.root}")
        return 1

    # Build RabbitMQ configuration
    rabbit_config = RabbitConfig.from_env(
        host=args.rabbit_host,
        port=args.rabbit_port,
        username=args.rabbit_user,
        password=args.rabbit_password,
        virtual_host=args.rabbit_vhost,
        queue=args.queue,
    )

    # Initialize RabbitMQ client (unless dry-run)
    client = None
    if not args.dry_run:
        try:
            client = RabbitClient(rabbit_config)
            client.connect()
        except Exception as e:
            logger.error(f"Failed to connect to RabbitMQ: {e}")
            return 1

    # Process files
    file_count = 0
    error_count = 0

    try:
        for file_path in iter_files(root_path):
            try:
                # Convert file to message
                message = file_to_message(file_path)

                if args.dry_run:
                    logger.info(f"[DRY-RUN] Would publish: {message}")
                else:
                    client.publish_json(message)

                file_count += 1

                # Log progress every 1000 files
                if file_count % 1000 == 0:
                    logger.info(f"Processed {file_count} files so far...")

            except Exception as e:
                error_count += 1
                logger.error(f"Error processing {file_path}: {e}")
                # Continue processing other files

    except KeyboardInterrupt:
        logger.warning("Interrupted by user")
        if client:
            client.close()
        return 130

    except Exception as e:
        logger.error(f"Fatal error during scanning: {e}")
        if client:
            client.close()
        return 1

    finally:
        # Clean up
        if client:
            client.close()

    # Summary
    logger.info("=" * 60)
    logger.info(f"Scan complete!")
    logger.info(f"Files processed: {file_count}")
    logger.info(f"Errors encountered: {error_count}")
    logger.info("=" * 60)

    return 0 if error_count == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
