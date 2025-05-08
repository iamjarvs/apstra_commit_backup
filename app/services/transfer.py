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
    
    if not all([host, username]):
        logger.error("Missing required SCP configuration")
        return False
    
    # Get filename from path
    filename = os.path.basename(full_path)
    full_aos_path = f"/var/lib/aos/snapshot/{local_file_path}/aos.data.tar.gz"
    
    # Use custom filename if provided
    if custom_filename:
        remote_filename = f"{custom_filename}-aos.data.tar.gz"
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
        
        if ssh_key_path:
            connect_kwargs["key_filename"] = ssh_key_path
            logger.info(f"Using SSH key authentication with key: {ssh_key_path}")
        elif password:
            connect_kwargs["password"] = password
            logger.info("Using password authentication")
        else:
            logger.warning("No authentication method provided, using default keys")
        
        # Connect to the remote server
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
        return False
    except paramiko.SSHException as e:
        logger.error(f"SSH error: {str(e)}")
        return False
    except Exception as e:
        logger.error(f"Error during SCP transfer: {str(e)}")
        return False

# Additional transfer methods (SFTP, FTP) can be added here if needed