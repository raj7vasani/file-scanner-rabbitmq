"""Tests for RabbitMQ client module."""

import json
from unittest.mock import Mock, MagicMock, patch, call

import pytest

from recursive_file_publisher.config import RabbitConfig
from recursive_file_publisher.rabbit import RabbitClient


@pytest.fixture
def rabbit_config():
    """Create a test RabbitMQ configuration."""
    return RabbitConfig(
        host="localhost",
        port=5672,
        username="guest",
        password="guest",
        virtual_host="/",
        queue="test_queue",
    )


class TestRabbitClient:
    """Tests for RabbitClient class."""

    @patch("recursive_file_publisher.rabbit.pika.BlockingConnection")
    def test_connect_creates_connection(self, mock_connection_class, rabbit_config):
        """Test that connect() creates a connection and channel."""
        # Setup mocks
        mock_connection = MagicMock()
        mock_channel = MagicMock()
        mock_connection.channel.return_value = mock_channel
        mock_connection_class.return_value = mock_connection

        # Create client and connect
        client = RabbitClient(rabbit_config)
        client.connect()

        # Verify connection was created
        assert mock_connection_class.called
        assert client._connection is not None
        assert client._channel is not None

    @patch("recursive_file_publisher.rabbit.pika.BlockingConnection")
    def test_connect_declares_queue(self, mock_connection_class, rabbit_config):
        """Test that connect() declares the queue as durable."""
        # Setup mocks
        mock_connection = MagicMock()
        mock_channel = MagicMock()
        mock_connection.channel.return_value = mock_channel
        mock_connection_class.return_value = mock_connection

        # Create client and connect
        client = RabbitClient(rabbit_config)
        client.connect()

        # Verify queue was declared with durable=True
        mock_channel.queue_declare.assert_called_once_with(
            queue="test_queue", durable=True
        )

    @patch("recursive_file_publisher.rabbit.pika.BlockingConnection")
    def test_publish_json_serializes_and_publishes(
        self, mock_connection_class, rabbit_config
    ):
        """Test that publish_json() serializes payload and publishes to queue."""
        # Setup mocks
        mock_connection = MagicMock()
        mock_channel = MagicMock()
        mock_connection.channel.return_value = mock_channel
        mock_connection.is_closed = False
        mock_channel.is_closed = False
        mock_connection_class.return_value = mock_connection

        # Create client and connect
        client = RabbitClient(rabbit_config)
        client.connect()

        # Publish a message
        payload = {"path": "/test/file.txt", "size_bytes": 123}
        client.publish_json(payload)

        # Verify basic_publish was called
        assert mock_channel.basic_publish.called

        # Extract the call arguments
        call_args = mock_channel.basic_publish.call_args

        # Verify routing key
        assert call_args[1]["routing_key"] == "test_queue"

        # Verify exchange is empty (default)
        assert call_args[1]["exchange"] == ""

        # Verify body is JSON
        body = call_args[1]["body"]
        decoded_payload = json.loads(body.decode("utf-8"))
        assert decoded_payload == payload

        # Verify properties
        properties = call_args[1]["properties"]
        assert properties.content_type == "application/json"
        assert properties.delivery_mode == 2  # Persistent

    @patch("recursive_file_publisher.rabbit.pika.BlockingConnection")
    def test_close_closes_connection_and_channel(
        self, mock_connection_class, rabbit_config
    ):
        """Test that close() closes both channel and connection."""
        # Setup mocks
        mock_connection = MagicMock()
        mock_channel = MagicMock()
        mock_connection.channel.return_value = mock_channel
        mock_connection.is_closed = False
        mock_channel.is_closed = False
        mock_connection_class.return_value = mock_connection

        # Create client, connect, and close
        client = RabbitClient(rabbit_config)
        client.connect()
        client.close()

        # Verify both were closed
        mock_channel.close.assert_called_once()
        mock_connection.close.assert_called_once()

    @patch("recursive_file_publisher.rabbit.pika.BlockingConnection")
    def test_context_manager_connects_and_closes(
        self, mock_connection_class, rabbit_config
    ):
        """Test that RabbitClient works as a context manager."""
        # Setup mocks
        mock_connection = MagicMock()
        mock_channel = MagicMock()
        mock_connection.channel.return_value = mock_channel
        mock_connection.is_closed = False
        mock_channel.is_closed = False
        mock_connection_class.return_value = mock_connection

        # Use as context manager
        with RabbitClient(rabbit_config) as client:
            assert client._connection is not None
            assert client._channel is not None

        # Verify connection was closed
        mock_connection.close.assert_called_once()

    @patch("recursive_file_publisher.rabbit.pika.BlockingConnection")
    def test_publish_retries_on_failure(self, mock_connection_class, rabbit_config):
        """Test that publish_json() retries on connection failure."""
        # Setup mocks
        mock_connection = MagicMock()
        mock_channel = MagicMock()
        mock_connection.channel.return_value = mock_channel
        mock_connection.is_closed = False
        mock_channel.is_closed = False
        mock_connection_class.return_value = mock_connection

        # Make first publish attempt fail, second succeed
        mock_channel.basic_publish.side_effect = [
            Exception("Connection error"),
            None,  # Success on second attempt
        ]

        # Create client and connect
        client = RabbitClient(rabbit_config)
        client.connect()

        # Publish should succeed after retry
        payload = {"path": "/test/file.txt"}
        client.publish_json(payload, retry_count=2)

        # Verify publish was called twice (first failed, second succeeded)
        assert mock_channel.basic_publish.call_count == 2

    @patch("recursive_file_publisher.rabbit.pika.BlockingConnection")
    def test_reconnect_on_closed_connection(self, mock_connection_class, rabbit_config):
        """Test that client reconnects when connection is closed."""
        # Setup mocks
        mock_connection = MagicMock()
        mock_channel = MagicMock()
        mock_connection.channel.return_value = mock_channel
        mock_connection_class.return_value = mock_connection

        # Create client and connect
        client = RabbitClient(rabbit_config)
        client.connect()

        # Simulate closed connection
        mock_connection.is_closed = True

        # Publish should trigger reconnection
        payload = {"path": "/test/file.txt"}
        client.publish_json(payload)

        # Verify connect was called again (reconnection)
        # Initial connection + reconnection = 2 connection instances
        assert mock_connection_class.call_count >= 2
