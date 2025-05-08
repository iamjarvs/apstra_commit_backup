"""
File transfer service for copying backups to remote storage.
"""
import os
import subprocess
import logging
import ftplib
from pathlib import Path
import paramiko
from paramiko import SSHClient
from scp import SCPClient
import datetime

logger = logging.getLogger(__name__)

def transfer_file(config, full_path, blueprint_id=None, blueprint_name=None):
    """
    Transfer a file to the remote server using the configured method.
    
    Args:
        config (dict): Transfer configuration
        full_path (str): Path to the local file
        blueprint_id (str, optional): ID of the blueprint being backed up
        blueprint_name (str, optional): Name of the blueprint being backed up
        
    Returns:
        bool: True if successful, False otherwise
    """
    # Create a filename that includes blueprint information
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    filename_parts = []
    
    # Add blueprint info if available
    if blueprint_name and blueprint_name != "Default Blueprint":
        filename_parts.append(blueprint_name)
    elif blueprint_id and blueprint_id != "default":
        filename_parts.append(blueprint_id)
    
    # Add timestamp
    filename_parts.append(timestamp)
    
    # Create custom filename for remote server
    custom_filename = "-".join(filename_parts)
    local_file_path = os.path.basename(full_path)
    
    logger.info(f"Processing file transfer for: {local_file_path}")
    logger.info(f"Full path: {full_path}")
    
    transfer_config = config.get("transfer", {})
    # Just SCP for now 
    method = "scp"
    
    if method == "scp":
        return transfer_scp(transfer_config, local_file_path, full_path, custom_filename)
    # elif method == "sftp":
    #     return transfer_sftp(transfer_config, local_file_path, custom_filename)
    # elif method == "ftp":
    #     return transfer_ftp(transfer_config, local_file_path, custom_filename)
    else:
        logger.error(f"Unsupported transfer method: {method}")
        return False

def transfer_scp(config, local_file_path, full_path, custom_filename=None):
    """
    Transfer a file using SCP via Paramiko.
    
    Args:
        config (dict): SCP configuration
        local_file_path (str): Path to the local file
        full_path (str): Full path information
        custom_filename (str, optional): Custom filename for the remote server
        
    Returns:
        bool: True if successful, False otherwise
    """
    host = config.get("host")
    port = config.get("port", 22)
    username = config.get("username")
    password = config.get("password")
    ssh_key_path = config.get("ssh_key_path")
    remote_dir = config.get("remote_directory", "~/")
    
    # Debug authentication info (without revealing sensitive data)
    logger.info(f"SCP host: {host}, port: {port}")
    logger.info(f"Username set: {'Yes' if username else 'No'}")
    logger.info(f"Password set: {'Yes' if password else 'No'}")
    logger.info(f"SSH key path set: {'Yes' if ssh_key_path else 'No'}")
    
    if not host:
        logger.error("Missing host in transfer configuration")
        return False
        
    if not username:
        logger.error("Missing username in transfer configuration")
        return False
    
    # Get filename from path
    filename = os.path.basename(full_path)
    
    # Check if the path is a directory (common for Apstra snapshots)
    if os.path.isdir(full_path):
        logger.info(f"Detected directory path: {full_path}")
        # Construct path to the data file inside the snapshot directory
        full_aos_path = os.path.join(full_path, "aos.data.tar.gz")
        if not os.path.exists(full_aos_path):
            # Look for any .tar.gz files in the directory
            tar_files = [f for f in os.listdir(full_path) if f.endswith('.tar.gz')]
            if tar_files:
                full_aos_path = os.path.join(full_path, tar_files[0])
                logger.info(f"Found alternative tar.gz file: {full_aos_path}")
            else:
                # Try to find any file to transfer
                logger.warning("No aos.data.tar.gz found, attempting to locate any file")
                dir_files = os.listdir(full_path)
                if dir_files:
                    full_aos_path = os.path.join(full_path, dir_files[0])
                    logger.info(f"Found alternative file: {full_aos_path}")
                else:
                    logger.error(f"No files found in directory: {full_path}")
                    return False
    else:
        # If it's already a file, use it directly
        full_aos_path = full_path
    
    if not os.path.exists(full_aos_path):
        logger.error(f"File not found: {full_aos_path}")
        return False
        
    logger.info(f"Will transfer file: {full_aos_path}")
    
    # Use custom filename if provided
    if custom_filename:
        aos_filename = os.path.basename(full_aos_path)
        remote_filename = f"{custom_filename}-{aos_filename}"
    else:
        remote_filename = f"{filename}-aos.data.tar.gz"
    
    try:
        # Create SSH client
        ssh = SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
        connect_kwargs = {
            "hostname": host,
            "port": port,
            "username": username,
        }
        
        auth_method = "none"
        
        if ssh_key_path and os.path.exists(ssh_key_path):
            connect_kwargs["key_filename"] = ssh_key_path
            auth_method = "ssh_key"
            logger.info(f"Using SSH key authentication with key: {ssh_key_path}")
        elif password:
            connect_kwargs["password"] = password
            auth_method = "password"
            logger.info("Using password authentication")
        else:
            logger.warning("No valid authentication method provided")
            logger.warning("Ensure either SSH_KEY_PATH or REMOTE_PASSWORD is set in your .env file")
            return False
        
        # Connect to the remote server
        logger.info(f"Connecting to {host}:{port} with {auth_method} authentication")
        ssh.connect(**connect_kwargs)
        
        # Create SCP client
        scp = SCPClient(ssh.get_transport())
        
        # Ensure remote directory exists and ends with slash
        if not remote_dir.endswith('/'):
            remote_dir = f"{remote_dir}/"
            
        # Determine full remote path
        if remote_dir.startswith('~/'):
            # Handle home directory expansion for remote path
            _, stdout, _ = ssh.exec_command("echo $HOME")
            home_dir = stdout.read().decode().strip()
            remote_path = f"{home_dir}/{remote_dir[2:]}{remote_filename}"
        else:
            remote_path = f"{remote_dir}{remote_filename}"
        
        logger.info(f"Transferring file via SCP: {full_aos_path} -> {host}:{remote_path}")
        
        # Transfer the file
        scp.put(full_aos_path, remote_path)
        
        # Close connections
        scp.close()
        ssh.close()
        
        logger.info("SCP transfer completed successfully")
        return True
        
    except paramiko.AuthenticationException as e:
        logger.error(f"Authentication failed: {str(e)}")
        logger.error("Check your username, password or SSH key")
        return False
    except paramiko.SSHException as e:
        logger.error(f"SSH error: {str(e)}")
        logger.error("This could be due to incorrect authentication or connection issues")
        return False
    except FileNotFoundError as e:
        logger.error(f"File not found error: {str(e)}")
        return False
    except Exception as e:
        logger.error(f"Error during SCP transfer: {str(e)}")
        return False

# Additional transfer methods (SFTP, FTP) can be added here if needed