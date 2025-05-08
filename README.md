# Apstra API Polling and Backup Tool

A utility that polls Apstra API endpoints for changes and triggers backups when changes are detected.

## Features

- Periodically polls Apstra API endpoints
- Detects new revisions based on revision IDs
- Triggers backup script when changes are detected
- Transfers backup files to a remote server
- Supports SCP, SFTP, and FTP transfer methods
- Configurable via YAML and environment variables

## Installation

1. Clone the repository or copy the files
2. Install the requirements:
   ```
   pip install -r requirements.txt
   ```
3. Copy `.env.example` to `.env` and fill in your credentials:
   ```
   cp .env.example .env
   ```
4. Update the configuration in `app/config/config.yaml`

## Configuration

Configuration is handled through a combination of the `config.yaml` file and environment variables. Sensitive information like passwords should be stored in environment variables rather than the config file.

# Environment Variable Management

This application supports two methods for providing environment variables:

1. Using environment variables directly in your system
2. Using a `.env` file in the project root directory

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

The configuration file (`config.yaml`) contains settings for:

- API connection details
- Backup script configuration
- Remote transfer settings
- State management parameters
- Logging configuration

## Configuration Examples

Here are example configurations for different transfer methods:

### SCP Transfer (with password)

**config.yaml:**
```yaml
api:
  server: "10.28.143.3"
  endpoint: "/api/blueprints/494c107b-3620-4be1-9ffc-1ffc8611482b/revisions"
  polling_interval_seconds: 30

backup:
  script_path: "/path/to/backup.sh"
  parameters: []
  
transfer:
  method: "scp"
  host: "backup.example.com"
  port: 22
  remote_directory: "/backups/apstra/"
```

**Required Environment Variables:**
```
APSTRA_USERNAME=admin
APSTRA_PASSWORD=your_apstra_password
REMOTE_USERNAME=backup_user
REMOTE_PASSWORD=your_remote_password
```

### SCP Transfer (with SSH key)

**config.yaml:**
```yaml
api:
  server: "10.28.143.3"
  endpoint: "/api/blueprints/494c107b-3620-4be1-9ffc-1ffc8611482b/revisions"
  polling_interval_seconds: 30

backup:
  script_path: "/path/to/backup.sh"
  parameters: []
  
transfer:
  method: "scp"
  host: "backup.example.com"
  port: 22
  remote_directory: "/backups/apstra/"
```

**Required Environment Variables:**
```
APSTRA_USERNAME=admin
APSTRA_PASSWORD=your_apstra_password
REMOTE_USERNAME=backup_user
SSH_KEY_PATH=/home/user/.ssh/id_rsa
```

### SFTP Transfer

**config.yaml:**
```yaml
api:
  server: "10.28.143.3"
  endpoint: "/api/blueprints/494c107b-3620-4be1-9ffc-1ffc8611482b/revisions"
  polling_interval_seconds: 30

backup:
  script_path: "/path/to/backup.sh"
  parameters: []
  
transfer:
  method: "sftp"
  host: "sftp.example.com"
  port: 22
  remote_directory: "/backups/apstra/"
```

**Required Environment Variables:**
```
APSTRA_USERNAME=admin
APSTRA_PASSWORD=your_apstra_password
REMOTE_USERNAME=backup_user
REMOTE_PASSWORD=your_remote_password
# Or use SSH key authentication
# SSH_KEY_PATH=/home/user/.ssh/id_rsa
```

### FTP Transfer

**config.yaml:**
```yaml
api:
  server: "10.28.143.3"
  endpoint: "/api/blueprints/494c107b-3620-4be1-9ffc-1ffc8611482b/revisions"
  polling_interval_seconds: 30

backup:
  script_path: "/path/to/backup.sh"
  parameters: []
  
transfer:
  method: "ftp"
  host: "ftp.example.com"
  port: 21
  remote_directory: "/backups/apstra/"
```

**Required Environment Variables:**
```
APSTRA_USERNAME=admin
APSTRA_PASSWORD=your_apstra_password
REMOTE_USERNAME=backup_user
REMOTE_PASSWORD=your_remote_password
```

## Setting Environment Variables

### Linux/macOS:
```bash
export APSTRA_USERNAME=admin
export APSTRA_PASSWORD=your_password
```

### Windows Command Prompt:
```cmd
set APSTRA_USERNAME=admin
set APSTRA_PASSWORD=your_password
```

### Windows PowerShell:
```powershell
$env:APSTRA_USERNAME="admin"
$env:APSTRA_PASSWORD="your_password"
```

### Using a .env file:
Create a file named `.env` in the project root with the following content:
```
APSTRA_USERNAME=admin
APSTRA_PASSWORD=your_password
REMOTE_USERNAME=backup_user
REMOTE_PASSWORD=remote_password
```

## Usage

Run the application:

```
python app/main.py
```

Or with a custom configuration file:

```
python app/main.py --config /path/to/custom-config.yaml
```

## Backup Script

The backup script is called when changes are detected. It should:

1. Perform the backup operation
2. Output the path to the backup file with the format: `BACKUP_FILE: /path/to/backup.file`
3. Return exit code 0 on success

## State Management

The application keeps track of the last seen revision ID in a JSON file specified in the configuration. By default, this is stored in `data/backup_state.json`.

## Logging

Logs are written to both stdout and a log file specified in the configuration.



# Complete Configuration Reference

## Configuration File (config.yaml)

The configuration file uses YAML format and includes several sections for different aspects of the application.

### Full Configuration Structure

```yaml
# API Configuration
api:
  # Apstra server address (without http/https)
  server: "10.28.143.3"
  # API endpoint to poll
  endpoint: "/api/blueprints/494c107b-3620-4be1-9ffc-1ffc8611482b/revisions"
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

# Backup Configuration
backup:
  # Path to the backup script
  script_path: "/path/to/backup.sh"
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

# Notification Configuration (optional)
notification:
  # Enable email notifications
  email:
    enabled: false
    # SMTP server settings
    smtp_server: "smtp.example.com"
    smtp_port: 587
    use_tls: true
    username: "notification@example.com"
    # (password should be in environment variables)
    # Recipients
    recipients: ["admin@example.com"]
    # Notification triggers
    notify_on:
      success: true
      failure: true
      error: true
  
  # Enable webhook notifications
  webhook:
    enabled: false
    # Webhook URL
    url: "https://webhooks.example.com/backup-notify"
    # Custom headers
    headers:
      Content-Type: "application/json"
      X-API-Key: "your-api-key"
    # Notification triggers
    notify_on:
      success: true
      failure: true
      error: true
```

## Environment Variables

### API Authentication
- `APSTRA_USERNAME`: Username for Apstra API authentication
- `APSTRA_PASSWORD`: Password for Apstra API authentication

### Remote Transfer Authentication
- `REMOTE_USERNAME`: Username for remote server
- `REMOTE_PASSWORD`: Password for remote server (for password authentication)
- `SSH_KEY_PATH`: Path to SSH key file (for key-based authentication)
- `SSH_KEY_PASSPHRASE`: Passphrase for SSH key if it's protected

### Notification Configuration
- `SMTP_PASSWORD`: Password for SMTP server (for email notifications)
- `WEBHOOK_SECRET`: Secret for webhook authentication

### Security Options
- `SSL_CERT_PATH`: Path to custom SSL certificate for API connections
- `ENCRYPT_STATE`: Set to "true" to encrypt the state file (requires additional setup)
- `STATE_ENCRYPTION_KEY`: Encryption key for state file if encryption is enabled

### Advanced Options
- `DEBUG`: Set to "true" to enable debug mode
- `MAX_RETRIES`: Maximum number of API request retries on failure
- `BACKUP_TIMEOUT`: Override the backup script timeout defined in config
- `TRANSFER_TIMEOUT`: Override the transfer timeout defined in config
- `LOG_LEVEL`: Override the logging level defined in config

## How Environment Variables Interact with Configuration

Environment variables take precedence over values defined in the configuration file. This means that if you define a value both in the configuration file and as an environment variable, the environment variable value will be used.

Some configuration options (especially sensitive ones like passwords) should only be provided via environment variables and not in the configuration file:

- Passwords and authentication tokens
- Encryption keys
- API secrets

## Setting Environment Variables

### Linux/macOS:
```bash
export APSTRA_USERNAME=admin
export APSTRA_PASSWORD=your_password
export REMOTE_USERNAME=backup_user
export REMOTE_PASSWORD=remote_password
```

### Windows Command Prompt:
```cmd
set APSTRA_USERNAME=admin
set APSTRA_PASSWORD=your_password
set REMOTE_USERNAME=backup_user
set REMOTE_PASSWORD=remote_password
```

### Windows PowerShell:
```powershell
$env:APSTRA_USERNAME="admin"
$env:APSTRA_PASSWORD="your_password"
$env:REMOTE_USERNAME="backup_user"
$env:REMOTE_PASSWORD="remote_password"
```

### Using a .env file:
Create a file named `.env` in the project root with the following content:
```
APSTRA_USERNAME=admin
APSTRA_PASSWORD=your_password
REMOTE_USERNAME=backup_user
REMOTE_PASSWORD=remote_password
SSH_KEY_PATH=/home/user/.ssh/id_rsa
SSH_KEY_PASSPHRASE=your_key_passphrase
```

## Configuration Best Practices

1. **Security**: Keep sensitive information in environment variables, not in the configuration file.
2. **Logging**: Use appropriate logging levels; DEBUG for development, INFO for normal operation.
3. **State Management**: Ensure the state file is in a persistent location if running in a container.
4. **Timeout Settings**: Set reasonable timeouts for API, backup, and transfer operations based on your environment.
5. **Retry Logic**: Configure retry attempts for network operations to handle transient failures.
6. **Directory Structure**: Create necessary directories for logs and state files before running the application.