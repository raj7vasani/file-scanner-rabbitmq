"""Example consumer for reading file metadata messages from RabbitMQ.

This script demonstrates how to consume messages published by the file scanner.
It connects to the same queue and prints each message.

Usage:
    python -m recursive_file_publisher.consumer_example --queue file_events
"""

import argparse
import json
import logging
import signal
import sys

import pika

from .config import RabbitConfig

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
        prog="consumer-example",
        description="Example consumer for file metadata messages",
    )

    parser.add_argument(
        "--rabbit-host",
        type=str,
        default="localhost",
        help="RabbitMQ host (default: localhost)",
    )
    parser.add_argument(
        "--rabbit-port",
        type=int,
        default=5672,
        help="RabbitMQ port (default: 5672)",
    )
    parser.add_argument(
        "--rabbit-user",
        type=str,
        default="guest",
        help="RabbitMQ username (default: guest)",
    )
    parser.add_argument(
        "--rabbit-password",
        type=str,
        default="guest",
        help="RabbitMQ password (default: guest)",
    )
    parser.add_argument(
        "--rabbit-vhost",
        type=str,
        default="/",
        help="RabbitMQ virtual host (default: /)",
    )
    parser.add_argument(
        "--queue",
        type=str,
        default="file_events",
        help="Queue name (default: file_events)",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging level (default: INFO)",
    )

    return parser.parse_args()


def on_message(channel, method, properties, body):
    """Callback function for processing received messages.

    Args:
        channel: Channel object
        method: Method frame
        properties: Message properties
        body: Message body (bytes)
    """
    try:
        # Decode and parse JSON message
        message = json.loads(body.decode("utf-8"))

        # Print message details
        logger.info("=" * 60)
        logger.info("Received file metadata:")
        logger.info(f"  Path: {message.get('path')}")
        logger.info(f"  Name: {message.get('name')}")
        logger.info(f"  Size: {message.get('size_bytes')} bytes")
        logger.info(f"  Modified: {message.get('modified_ts')}")
        logger.info("=" * 60)

        # Acknowledge the message
        channel.basic_ack(delivery_tag=method.delivery_tag)

    except json.JSONDecodeError as e:
        logger.error(f"Failed to decode JSON message: {e}")
        # Reject and don't requeue malformed messages
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    except Exception as e:
        logger.error(f"Error processing message: {e}")
        # Reject but requeue for retry
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)


def main() -> int:
    """Main entry point for the consumer.

    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    args = parse_args()
    setup_logging(args.log_level)

    logger.info("Starting consumer example")
    logger.info(f"Connecting to {args.rabbit_host}:{args.rabbit_port}")
    logger.info(f"Queue: {args.queue}")

    # Build configuration
    config = RabbitConfig.from_env(
        host=args.rabbit_host,
        port=args.rabbit_port,
        username=args.rabbit_user,
        password=args.rabbit_password,
        virtual_host=args.rabbit_vhost,
        queue=args.queue,
    )

    try:
        # Connect to RabbitMQ
        credentials = pika.PlainCredentials(config.username, config.password)
        parameters = pika.ConnectionParameters(
            host=config.host,
            port=config.port,
            virtual_host=config.virtual_host,
            credentials=credentials,
            heartbeat=600,
        )

        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()

        # Declare queue (idempotent)
        channel.queue_declare(queue=config.queue, durable=True)

        # Set up consumer
        channel.basic_qos(prefetch_count=1)  # Process one message at a time
        channel.basic_consume(
            queue=config.queue, on_message_callback=on_message, auto_ack=False
        )

        logger.info("Waiting for messages. Press CTRL+C to exit.")

        # Handle graceful shutdown
        def signal_handler(sig, frame):
            logger.info("Shutting down consumer...")
            channel.stop_consuming()
            connection.close()
            sys.exit(0)

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        # Start consuming
        channel.start_consuming()

    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        return 0

    except Exception as e:
        logger.error(f"Error: {e}")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
