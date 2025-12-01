"""RabbitMQ client for publishing file metadata messages."""

import json
import logging
from typing import Dict, Optional

import pika
from pika.exceptions import AMQPConnectionError, AMQPChannelError

from .config import RabbitConfig

logger = logging.getLogger(__name__)


class RabbitClient:
    """RabbitMQ client for publishing messages to a queue.

    This client manages a connection to RabbitMQ, declares a durable queue,
    and provides methods for publishing JSON messages with persistent delivery.

    Attributes:
        config: RabbitMQ configuration
    """

    def __init__(self, config: RabbitConfig):
        """Initialize the RabbitMQ client.

        Args:
            config: RabbitMQ connection configuration
        """
        self.config = config
        self._connection: Optional[pika.BlockingConnection] = None
        self._channel: Optional[pika.channel.Channel] = None

    def connect(self) -> None:
        """Establish connection to RabbitMQ and declare the queue.

        Raises:
            AMQPConnectionError: If connection to RabbitMQ fails
        """
        try:
            logger.info(
                f"Connecting to RabbitMQ at {self.config.host}:{self.config.port}"
            )

            credentials = pika.PlainCredentials(
                self.config.username, self.config.password
            )
            parameters = pika.ConnectionParameters(
                host=self.config.host,
                port=self.config.port,
                virtual_host=self.config.virtual_host,
                credentials=credentials,
                heartbeat=600,
                blocked_connection_timeout=300,
            )

            self._connection = pika.BlockingConnection(parameters)
            self._channel = self._connection.channel()

            # Declare queue as durable so it survives broker restarts
            self._channel.queue_declare(queue=self.config.queue, durable=True)

            logger.info(f"Connected to RabbitMQ, queue '{self.config.queue}' ready")

        except AMQPConnectionError as e:
            logger.error(f"Failed to connect to RabbitMQ: {e}")
            raise

    def _ensure_connection(self) -> None:
        """Ensure the connection and channel are open, reconnecting if necessary.

        Raises:
            AMQPConnectionError: If reconnection fails
        """
        if self._connection is None or self._connection.is_closed:
            logger.warning("Connection closed, reconnecting...")
            self.connect()
        elif self._channel is None or self._channel.is_closed:
            logger.warning("Channel closed, reopening...")
            self._channel = self._connection.channel()
            self._channel.queue_declare(queue=self.config.queue, durable=True)

    def publish_json(self, payload: Dict, retry_count: int = 3) -> None:
        """Publish a JSON message to the configured queue.

        The message is published with:
        - content_type: application/json
        - delivery_mode: 2 (persistent)
        - exchange: '' (default/direct)
        - routing_key: queue name

        Args:
            payload: Dictionary to be serialized as JSON and published
            retry_count: Number of times to retry on failure (default: 3)

        Raises:
            AMQPChannelError: If publishing fails after all retries
            json.JSONDecodeError: If payload cannot be serialized to JSON
        """
        body = json.dumps(payload).encode("utf-8")

        for attempt in range(retry_count):
            try:
                self._ensure_connection()

                properties = pika.BasicProperties(
                    content_type="application/json",
                    delivery_mode=2,  # Persistent message
                )

                self._channel.basic_publish(
                    exchange="",
                    routing_key=self.config.queue,
                    body=body,
                    properties=properties,
                )

                logger.debug(f"Published message: {payload.get('path', 'unknown')}")
                return

            except (AMQPConnectionError, AMQPChannelError) as e:
                logger.warning(
                    f"Publish attempt {attempt + 1}/{retry_count} failed: {e}"
                )
                if attempt == retry_count - 1:
                    logger.error(f"Failed to publish message after {retry_count} attempts")
                    raise

    def close(self) -> None:
        """Close the RabbitMQ connection gracefully."""
        try:
            if self._channel and not self._channel.is_closed:
                self._channel.close()
                logger.debug("Channel closed")

            if self._connection and not self._connection.is_closed:
                self._connection.close()
                logger.info("Connection to RabbitMQ closed")

        except Exception as e:
            logger.warning(f"Error closing connection: {e}")

    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
