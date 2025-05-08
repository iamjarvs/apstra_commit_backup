"""
Functions for triggering the backup script.
"""
import os
import subprocess
import logging
import re
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

def run_backup_script(script_path, parameters=None):
    """
    Execute the backup bash script.
    
    Args:
        script_path (str): Path to the backup script
        parameters (list): Additional parameters to pass to the script
        
    Returns:
        tuple: (success, output, error)
    """
    if not os.path.exists(script_path):
        logger.error(f"Backup script not found at {script_path}")
        return False, "", f"Script not found: {script_path}"
    
    # Prepare command
    command = [script_path]
    if parameters:
        command.extend(parameters)
    
    logger.info(f"Executing backup script: {' '.join(command)}")

    try:
        # Check if we need to use sudo
        use_sudo = script_path.startswith('/usr/') or script_path.startswith('/bin/')
        
        if use_sudo:
            full_command = ['sudo', 'bash'] + command
            logger.info(f"Using sudo to execute: {' '.join(full_command)}")
        else:
            full_command = ['bash'] + command
        
        result = subprocess.run(
            full_command,
            capture_output=True, 
            text=True, 
            check=True
        )
        
        stdout = result.stdout
        stderr = result.stderr
        
        logger.info(f"Backup command output: {stdout}")
        if stderr:
            logger.warning(f"Backup command error output: {stderr}")
        
        return True, stdout, stderr
    
    except subprocess.CalledProcessError as e:
        logger.error(f"Command failed with error code {e.returncode}")
        logger.error(f"Error output: {e.stderr}")
        return False, e.stdout, e.stderr
    except Exception as e:
        logger.error(f"Failed to execute backup script: {str(e)}")
        return False, "", str(e)

def get_latest_backup_file(backup_output):
    """
    Extract the path to the latest backup file from the script output.
    
    Args:
        backup_output (str): Output from the backup script
        
    Returns:
        str: Path to the backup file or None
    """
    logger.info("Parsing backup output to find backup file path")
    
    # Pattern 1: Look for "New AOS snapshot:" line
    for line in backup_output.splitlines():
        if line.startswith("New AOS snapshot:"):
            file_path = line.split(":", 1)[1].strip()
            # Check if it's a full path or just a filename
            if os.path.isabs(file_path):
                if os.path.exists(file_path):
                    logger.info(f"Found backup file (absolute path): {file_path}")
                    return file_path
            else:
                # Try with standard Apstra snapshot path
                complete_path = f"/var/lib/aos/snapshot/{file_path}"
                if os.path.exists(complete_path):
                    logger.info(f"Found backup file: {complete_path}")
                    return complete_path
    
    # Pattern 2: Look for date-based snapshot patterns in output
    date_patterns = [
        r'\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}',  # 2025-05-08_19-51-35
        r'\d{8}-\d{6}',                          # 20250508-195135
        r'snapshot-\d{8}-\d{6}',                 # snapshot-20250508-195135
        r'aos-snapshot-\d{8}-\d{6}'              # aos-snapshot-20250508-195135
    ]
    
    for pattern in date_patterns:
        matches = re.findall(pattern, backup_output)
        if matches:
            latest_match = matches[-1]  # Use the last match in case there are multiple
            # Check if this is a valid directory
            potential_path = f"/var/lib/aos/snapshot/{latest_match}"
            if os.path.exists(potential_path):
                logger.info(f"Found backup directory using pattern match: {potential_path}")
                return potential_path
    
    # Pattern 3: Fallback - check for any absolute path in the output
    path_pattern = r'/[/\w\.-]+'
    paths = []
    
    for match in re.finditer(path_pattern, backup_output):
        potential_path = match.group(0)
        if os.path.exists(potential_path):
            paths.append(potential_path)
    
    if paths:
        # Sort by modification time to get the most recent one
        latest_path = sorted(paths, key=os.path.getmtime, reverse=True)[0]
        logger.info(f"Found potential backup file using path extraction: {latest_path}")
        return latest_path
    
    # Pattern 4: Last resort - check if there are recent snapshots in the standard directory
    snapshot_dir = "/var/lib/aos/snapshot"
    if os.path.isdir(snapshot_dir):
        # List all subdirectories
        subdirs = [os.path.join(snapshot_dir, d) for d in os.listdir(snapshot_dir) 
                 if os.path.isdir(os.path.join(snapshot_dir, d))]
        
        if subdirs:
            # Get creation times
            dir_times = [(d, os.path.getctime(d)) for d in subdirs]
            
            # Filter to only include directories created in the last minute
            # (assuming backup was just created)
            now = datetime.now().timestamp()
            recent_dirs = [(d, t) for d, t in dir_times if now - t < 60]  # Last 60 seconds
            
            if recent_dirs:
                # Sort by creation time, most recent first
                sorted_dirs = sorted(recent_dirs, key=lambda x: x[1], reverse=True)
                latest_dir = sorted_dirs[0][0]
                logger.info(f"Found recent backup directory: {latest_dir}")
                return latest_dir
    
    # If we got here, we couldn't find a backup file
    logger.warning("Could not determine backup file path from script output")
    return None