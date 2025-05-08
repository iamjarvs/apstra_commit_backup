# Apstra API Polling and Backup Tool

A robust utility that monitors Apstra API endpoints for changes across multiple blueprints and triggers backups when changes are detected, with secure transfer to remote storage.

## Features

- **Multi-Blueprint Support**: Monitor multiple Apstra blueprints simultaneously
- **Periodic Polling**: Configurable polling intervals for each blueprint
- **Change Detection**: Identifies new revisions based on revision IDs
- **Automated Backups**: Triggers backup script when changes are detected
- **Remote Transfer**: Securely transfers backup files to remote storage
- **Flexible Authentication**: Supports password and SSH key authentication
- **Comprehensive Logging**: Detailed logs for monitoring and troubleshooting
- **State Management**: Persistent tracking of blueprint states across restarts
- **Docker Support**: Easy containerization with provided Dockerfile

## Installation

### Prerequisites

- Python 3.8 or higher
- pip package manager
- Access to an Apstra server
- Remote storage server (for backup transfers)

### Basic Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/apstra-api-backup-tool.git
   cd apstra-api-backup-tool
   ```

2. Install the requirements:
   ```bash
   pip install -r requirements.txt
   ```

3. Copy the example environment file:
   ```bash
   cp .env.example .env
   ```

4. Update the `.env` file with your credentials:
   ```
   # Apstra API credentials
   APSTRA_USERNAME=admin
   APSTRA_PASSWORD=your_password

   # Remote server credentials
   REMOTE_USERNAME=backup_user
   REMOTE_PASSWORD=your_remote_password
   # Or use SSH key authentication
   # SSH_KEY_PATH=/path/to/private_key
   ```

5. Update the configuration in `app/config/config.yaml`



## Configuration

Configuration is handled through a combination of the `config.yaml` file and environment variables.

# Environment Variable Management

Two methods are supported for providing environment variables:

1. **System environment variables**
2. **`.env` file** in the project root directory

#### Required Environment Variables

- `APSTRA_USERNAME`: Username for Apstra API authentication
- `APSTRA_PASSWORD`: Password for Apstra API authentication

## Using a .env File

Create a file named `.env` in the project root directory with the following format:

```
# Apstra API credentials
APSTRA_USERNAME=admin
APSTRA_PASSWORD=your_password

# Remote server credentials
REMOTE_USERNAME=backup_user
REMOTE_PASSWORD=your_remote_password
# Alternatively, use SSH key authentication
SSH_KEY_PATH=/path/to/private_key
SSH_KEY_PASSPHRASE=your_key_passphrase
```

The application will automatically load this file if it exists.

## Required Environment Variables

The following environment variables are required:

- `APSTRA_USERNAME`: Username for Apstra API authentication
- `APSTRA_PASSWORD`: Password for Apstra API authentication

## Optional Environment Variables

These environment variables are optional:

- `REMOTE_USERNAME`: Username for remote server
- `REMOTE_PASSWORD`: Password for remote server (for password authentication)
- `SSH_KEY_PATH`: Path to SSH key file (for key-based authentication)
- `SSH_KEY_PASSPHRASE`: Passphrase for SSH key if it's protected

## Command Line Options

The application supports the following command line options:

```
--config PATH     Path to custom config file (default: app/config/config.yaml)
--env-file PATH   Path to .env file (default: .env in project root)
```

Example:
```
python3 app/main.py --config custom-config.yaml --env-file /path/to/.env
```


### Configuration File

#### Optional Environment Variables

- `REMOTE_USERNAME`: Username for remote server
- `REMOTE_PASSWORD`: Password for remote server (for password authentication)
- `SSH_KEY_PATH`: Path to SSH key file (for key-based authentication)
- `SSH_KEY_PASSPHRASE`: Passphrase for SSH key if it's protected

### Configuration File

The configuration file (`app/config/config.yaml`) contains settings for:

#### Multi-Blueprint Configuration

```yaml
# API Configuration
api:
  server: "10.28.143.3"
  polling_interval_seconds: 30
  
  # Multiple blueprints configuration
  blueprints:
    - id: "494c107b-3620-4be1-9ffc-1ffc8611482b"
      name: "DataCenter-1"  # Optional friendly name
      endpoint: "/api/blueprints/494c107b-3620-4be1-9ffc-1ffc8611482b/revisions"
      
    - id: "5a7e309c-4521-4af2-8971-12b96c5adef8"
      name: "DataCenter-2"  # Optional friendly name
      endpoint: "/api/blueprints/5a7e309c-4521-4af2-8971-12b96c5adef8/revisions"

backup:
  script_path: "/usr/sbin/aos_backup"
  parameters: []
  
transfer:
  method: "scp"
  host: "backup.example.com"
  port: 22
  remote_directory: "/backups/apstra/"
```

Each blueprint requires:
- `id`: The UUID of the blueprint (used for identification and backup parameters)
- `endpoint`: The API endpoint to poll for this blueprint

Optional fields:
- `name`: A friendly name for the blueprint (used in logs and filenames)

## Usage

### Running the Application

```bash
python app/main.py
```

With custom configuration and environment files:

```bash
python app/main.py --config /path/to/custom-config.yaml --env-file /path/to/.env
```

### Command Line Options

- `--config PATH`: Path to custom configuration file
- `--env-file PATH`: Path to custom .env file


## How It Works

### Polling Process

1. The application authenticates with the Apstra server using the provided credentials
2. For each configured blueprint:
   - The API endpoint is polled for revision information
   - The latest revision is compared with the last known revision
   - If a new revision is detected, the backup process is triggered

### Backup Process

1. When changes are detected in a blueprint, the backup script is called with the blueprint ID
2. The backup script creates a snapshot of the blueprint configuration
3. The path to the backup file is extracted from the script output
4. The backup file is transferred to the remote server

### File Transfer

Backup files are transferred to the remote server with filenames that include:
- The blueprint name (or ID if no name is provided)
- A timestamp
- The file extension

Example: `DataCenter-1-20250508_101530-aos.data.tar.gz`

### State Management

The application maintains state about each blueprint in a JSON file:

```json
{
  "last_poll_time": "2025-05-08T10:15:30.123456",
  "last_updated": "2025-05-08T10:15:30.123456",
  "blueprints": {
    "494c107b-3620-4be1-9ffc-1ffc8611482b": {
      "last_revision_id": "123",
      "last_poll_time": "2025-05-08T10:15:30.123456",
      "blueprint_id": "494c107b-3620-4be1-9ffc-1ffc8611482b",
      "blueprint_name": "DataCenter-1"
    },
    "5a7e309c-4521-4af2-8971-12b96c5adef8": {
      "last_revision_id": "456",
      "last_poll_time": "2025-05-08T10:15:25.123456",
      "blueprint_id": "5a7e309c-4521-4af2-8971-12b96c5adef8", 
      "blueprint_name": "DataCenter-2"
    }
  }
}
```

## Advanced Configuration

### Full Configuration Reference

```yaml
# API Configuration
api:
  # Apstra server address (without http/https)
  server: "10.28.143.3"
  # Polling interval in seconds
  polling_interval_seconds: 30
  # Custom headers (optional)
  headers:
    Content-Type: "application/json"
    Accept: "application/json"
  # API request timeout in seconds (optional)
  timeout: 10
  # Whether to verify SSL certificates (optional)
  verify_ssl: false
  
  # Multiple blueprints configuration
  blueprints:
    - id: "494c107b-3620-4be1-9ffc-1ffc8611482b"
      name: "DataCenter-1"
      endpoint: "/api/blueprints/494c107b-3620-4be1-9ffc-1ffc8611482b/revisions"
    
    - id: "5a7e309c-4521-4af2-8971-12b96c5adef8"
      name: "DataCenter-2"
      endpoint: "/api/blueprints/5a7e309c-4521-4af2-8971-12b96c5adef8/revisions"

# Backup Configuration
backup:
  # Path to the backup script
  script_path: "/usr/sbin/aos_backup"
  # Additional parameters to pass to the script (optional)
  parameters: ["--full", "--compress"]
  # Timeout for backup script execution in seconds (optional)
  timeout: 600
  # Working directory for the backup script (optional)
  working_dir: "/path/to/working/dir"
  # Environment variables to pass to the backup script (optional)
  env:
    BACKUP_LEVEL: "full"
    COMPRESSION: "gzip"

# Remote Transfer Configuration
transfer:
  # Transfer method: scp, sftp, or ftp
  method: "scp"
  # Remote server hostname or IP
  host: "backup.example.com"
  # Remote server port (default: 22 for scp/sftp, 21 for ftp)
  port: 22
  # Directory on the remote server where backups will be stored
  remote_directory: "/backups/apstra/"
  # Number of retry attempts for failed transfers (optional)
  retry_attempts: 3
  # Delay between retry attempts in seconds (optional)
  retry_delay: 5
  # Additional SCP options (optional, only for SCP method)
  scp_options: ["-C", "-o", "ConnectTimeout=10"]
  # Whether to create remote directory if it doesn't exist (optional)
  create_remote_dir: true
  # Timeout for transfer operations in seconds (optional)
  timeout: 300
  # Whether to keep the local backup file after transfer (optional)
  keep_local: true

# State Management
state:
  # Path to the state file (stores the last seen revision ID)
  file_path: "data/backup_state.json"
  # Backup state file after each update (optional)
  backup_state: true
  # Maximum number of state backups to keep (optional)
  max_backups: 5

# Logging
logging:
  # Logging level: DEBUG, INFO, WARNING, ERROR, CRITICAL
  level: "INFO"
  # Log file path
  file: "logs/backup_service.log"
  # Maximum log file size in bytes before rotation (optional)
  max_size: 10485760  # 10 MB
  # Number of backup logs to keep (optional)
  backup_count: 5
  # Log format (optional)
  format: "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
  # Whether to log to console as well (optional)
  console: true
  # Console log level (optional, can be different from file log level)
  console_level: "INFO"
```

### Transfer Method Examples

#### SCP Transfer (with password)

```yaml
transfer:
  method: "scp"
  host: "backup.example.com"
  port: 22
  remote_directory: "/backups/apstra/"
```

**Required Environment Variables:**
```
REMOTE_USERNAME=backup_user
REMOTE_PASSWORD=your_remote_password
```

#### SCP Transfer (with SSH key)

```yaml
transfer:
  method: "scp"
  host: "backup.example.com"
  port: 22
  remote_directory: "/backups/apstra/"
```

**Required Environment Variables:**
```
REMOTE_USERNAME=backup_user
SSH_KEY_PATH=/home/user/.ssh/id_rsa
# Optional passphrase if key is protected
SSH_KEY_PASSPHRASE=your_key_passphrase
```

## Backup Script

The backup script is called when changes are detected. The application passes the blueprint ID as a parameter to the script, which should:

1. Perform the backup operation for the specified blueprint
2. Output the path to the backup file with a format that can be parsed by the application
3. Return exit code 0 on success

Sample output format:
```
New AOS snapshot: snapshot-20250508-101530
```

## Logging

Logs are written to both stdout and a log file specified in the configuration.

Default log location: `logs/backup_service.log`

Log example:
```
2025-05-08 10:15:30,123 - app.main - INFO - Starting API polling and backup service
2025-05-08 10:15:31,456 - app.services.api_poller - INFO - Successfully authenticated to Apstra API
2025-05-08 10:15:32,789 - app.services.api_poller - INFO - Polling blueprint: DataCenter-1 (494c107b-3620-4be1-9ffc-1ffc8611482b)
2025-05-08 10:15:33,012 - app.services.api_poller - INFO - New revision detected: 124 (previous: 123)
2025-05-08 10:15:33,345 - app.main - INFO - Changes detected in blueprint: DataCenter-1 (494c107b-3620-4be1-9ffc-1ffc8611482b)
```

## Best Practices

1. **Security**: 
   - Keep sensitive information in environment variables, not in configuration files
   - Use SSH key authentication rather than passwords when possible
   - Run the application with minimal required permissions

2. **Monitoring**:
   - Set up monitoring for the application's log file
   - Configure alerts for backup failures
   - Periodically verify that backups are being created and transferred

3. **Reliability**:
   - Use appropriate timeouts for API, backup, and transfer operations
   - Configure retry attempts for network operations to handle transient failures
   - Ensure the backup script is idempotent (can be run multiple times safely)

4. **Performance**:
   - Adjust polling intervals based on how frequently blueprints change
   - Consider staggering polling for multiple blueprints to avoid overloading the API

5. **Docker Deployment**:
   - Use volumes for persistent storage of logs and state files
   - Consider using Docker Compose for easier configuration management
   - Set up health checks to monitor the application's status

## Troubleshooting

### Common Issues

**Connection refused when connecting to Apstra server**
- Verify the server address is correct
- Ensure network connectivity to the server
- Check if the Apstra service is running

**Authentication failures**
- Verify username and password are correct
- Check for expired credentials
- Ensure the API user has sufficient permissions

**Backup script failures**
- Check if the script path is correct
- Ensure the script has execute permissions
- Verify the script can be run manually

**Transfer failures**
- Verify remote server address and credentials
- Check network connectivity to the remote server
- Ensure sufficient disk space on the remote server

### Debug Mode

To enable more detailed logging, set the logging level to DEBUG in the configuration:

```yaml
logging:
  level: "DEBUG"
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgements

- The Apstra team for their API documentation
- The Python community for excellent libraries
- All contributors to this project