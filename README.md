# File Scanner with RabbitMQ

A Python service that recursively scans directories and publishes file metadata to RabbitMQ as individual messages.

## Implementation

- **`scanner.py`** - Recursively traverses directories and extracts file metadata (path, name, size, modified timestamp)
- **`rabbit.py`** - RabbitMQ client with persistent delivery, retry logic, and automatic reconnection
- **`cli.py`** - Command-line interface with argument parsing and main orchestration logic
- **`config.py`** - Configuration management supporting both CLI arguments and environment variables
- **`consumer_example.py`** - Example consumer demonstrating how to read and process messages from the queue
- **`Dockerfile` & `docker-compose.yml`** - Docker containerization for easy deployment

## How to Run

### 1. Start RabbitMQ
```bash
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
```

### 2. Install Dependencies
```bash
cd recursive-file-publisher
pip install -r requirements.txt
```

### 3. Run the Scanner
```bash
# Scan a directory and publish to RabbitMQ
PYTHONPATH=src python -m recursive_file_publisher --root /path/to/scan

# Dry-run mode (preview without publishing)
PYTHONPATH=src python -m recursive_file_publisher --root /path/to/scan --dry-run
```

### 4. Consume Messages
```bash
# In a separate terminal
PYTHONPATH=src python -m recursive_file_publisher.consumer_example --queue file_events
```

## Expected Output

### Scanner Output
```
2025-12-04 18:04:21 [INFO] Starting recursive file publisher
2025-12-04 18:04:21 [INFO] Root directory: ./src
2025-12-04 18:04:21 [INFO] Dry run mode: False
2025-12-04 18:04:21 [INFO] Connecting to RabbitMQ at localhost:5672
2025-12-04 18:04:21 [INFO] Connected to RabbitMQ, queue 'file_events' ready
2025-12-04 18:04:21 [INFO] ============================================================
2025-12-04 18:04:21 [INFO] Scan complete!
2025-12-04 18:04:21 [INFO] Files processed: 13
2025-12-04 18:04:21 [INFO] Errors encountered: 0
2025-12-04 18:04:21 [INFO] ============================================================
```

### Consumer Output
```
2025-12-04 18:04:56 [INFO] Waiting for messages. Press CTRL+C to exit.
2025-12-04 18:04:56 [INFO] ============================================================
2025-12-04 18:04:56 [INFO] Received file metadata:
2025-12-04 18:04:56 [INFO]   Path: /absolute/path/to/file.py
2025-12-04 18:04:56 [INFO]   Name: file.py
2025-12-04 18:04:56 [INFO]   Size: 2482 bytes
2025-12-04 18:04:56 [INFO]   Modified: 2025-11-29T19:38:56.937669
2025-12-04 18:04:56 [INFO] ============================================================
```

### Message Format
Each file is published as a JSON message:
```json
{
  "path": "/absolute/path/to/file.txt",
  "name": "file.txt",
  "size_bytes": 1024,
  "modified_ts": "2025-11-29T10:30:45.123456"
}
```

## Verification

View messages in RabbitMQ Management UI: http://localhost:15672 (guest/guest)

## AI Usage Disclosure

This project was developed with assistance from GitHub Copilot for:
- Code generation and boilerplate reduction
- Documentation and docstring creation
- Best practices suggestions

All code has been reviewed, tested, and is fully understood by me, the developer.
