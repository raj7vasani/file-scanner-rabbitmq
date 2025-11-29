"""Configuration management for RabbitMQ connection."""

import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class RabbitConfig:
    """Configuration for RabbitMQ connection.

    Attributes:
        host: RabbitMQ server hostname
        port: RabbitMQ server port
        username: Username for authentication
        password: Password for authentication
        virtual_host: Virtual host to use
        queue: Queue name for publishing messages
    """

    host: str
    port: int
    username: str
    password: str
    virtual_host: str
    queue: str

    @classmethod
    def from_env(
        cls,
        host: Optional[str] = None,
        port: Optional[int] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        virtual_host: Optional[str] = None,
        queue: Optional[str] = None,
    ) -> "RabbitConfig":
        """Create RabbitConfig from environment variables with optional overrides.

        Priority: explicit arguments > environment variables > defaults

        Environment variables:
            RABBITMQ_HOST: RabbitMQ hostname (default: localhost)
            RABBITMQ_PORT: RabbitMQ port (default: 5672)
            RABBITMQ_USER: Username (default: guest)
            RABBITMQ_PASSWORD: Password (default: guest)
            RABBITMQ_VHOST: Virtual host (default: /)
            RABBITMQ_QUEUE: Queue name (default: file_events)

        Args:
            host: Override hostname
            port: Override port
            username: Override username
            password: Override password
            virtual_host: Override virtual host
            queue: Override queue name

        Returns:
            RabbitConfig instance with resolved configuration
        """
        return cls(
            host=host or os.getenv("RABBITMQ_HOST", "localhost"),
            port=port or int(os.getenv("RABBITMQ_PORT", "5672")),
            username=username or os.getenv("RABBITMQ_USER", "guest"),
            password=password or os.getenv("RABBITMQ_PASSWORD", "guest"),
            virtual_host=virtual_host or os.getenv("RABBITMQ_VHOST", "/"),
            queue=queue or os.getenv("RABBITMQ_QUEUE", "file_events"),
        )
