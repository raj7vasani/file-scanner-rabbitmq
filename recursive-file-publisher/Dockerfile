# Use Python 3.11 slim image for smaller size
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install dependencies first (for better layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src/ ./src/

# Set PYTHONPATH to include src directory
ENV PYTHONPATH=/app/src

# Create a mount point for the directory to scan
VOLUME ["/data"]

# Use environment variables for configuration (can be overridden)
ENV RABBITMQ_HOST=rabbitmq
ENV RABBITMQ_PORT=5672
ENV RABBITMQ_USER=guest
ENV RABBITMQ_PASSWORD=guest
ENV RABBITMQ_VHOST=/
ENV RABBITMQ_QUEUE=file_events

# Set default command
# Users can override this with docker run arguments
ENTRYPOINT ["python", "-m", "recursive_file_publisher"]
CMD ["--root", "/data"]
