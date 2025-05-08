#!/usr/bin/env python3
"""
Main entry point for the API polling and backup service.
"""
import os
import sys
import time
import logging
import signal
import argparse
from pathlib import Path
from utils.config import load_config
from utils.env_loader import load_environment_variables, apply_env_to_config
from utils.state import load_state, save_state, update_state
from services.api_poller import poll_api
from services.backup_trigger import run_backup_script, get_latest_backup_file
from services.transfer import transfer_file


# Add the app directory to the path
app_dir = Path(__file__).resolve().parent
if str(app_dir) not in sys.path:
    sys.path.insert(0, str(app_dir))

# Global variables for signal handling
running = True

def setup_logging(config):
    """
    Set up logging configuration.
    
    Args:
        config (dict): Configuration dictionary
    """
    log_config = config.get("logging", {})
    log_level = getattr(logging, log_config.get("level", "INFO"))
    log_file = log_config.get("file")
    
    # Create logs directory if it doesn't exist
    if log_file:
        log_dir = os.path.dirname(log_file)
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)
    
    # Configure logging
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_file) if log_file else logging.NullHandler()
        ]
    )

def handle_signal(signum, frame):
    """Signal handler for graceful shutdown."""
    global running
    logging.info(f"Received signal {signum}, shutting down...")
    running = False

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='API Polling and Backup Service')
    parser.add_argument(
        '--config', 
        default='app/config/config.yaml',
        help='Path to configuration file'
    )
    parser.add_argument(
        '--env-file',
        help='Path to .env file (defaults to .env in the project root)'
    )
    return parser.parse_args()

def main():
    """Main function to run the service."""
    # Parse command line arguments
    args = parse_arguments()
    
    # Load configuration
    config = load_config(args.config)
    
    # Set up logging
    setup_logging(config)
    logger = logging.getLogger(__name__)
    
    # Get environment variables
    env_vars, env_success = load_environment_variables(args.env_file)
    
    if not env_success:
        logger.error("Missing required environment variables. Service cannot start.")
        logger.error("Please set APSTRA_USERNAME and APSTRA_PASSWORD environment variables or in .env file.")
        sys.exit(1)
    
    # Apply environment variables to config
    config = apply_env_to_config(config, env_vars)
    
    # Load initial state
    state_file = config.get("state", {}).get("file_path", "data/backup_state.json")
    state = load_state(state_file)
    
    # Set up signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)
    
    logger.info("Starting API polling and backup service")
    
    # Main polling loop
    global running
    while running:
        try:
            # Poll the API and check for changes
            changes_detected, new_state, token = poll_api(config, state)
            
            if changes_detected:
                logger.info("Changes detected, triggering backup")
                
                # Run backup script
                backup_script = config.get("backup", {}).get("script_path") or "/usr/sbin/aos_backup"
                backup_params = config.get("backup", {}).get("parameters", [])
                
                success, output, error = run_backup_script(backup_script, backup_params)
                
                if success:
                    # Get the backup file path from the output
                    backup_file = get_latest_backup_file(output)
                    if backup_file:
                        # Transfer the backup file
                        logger.info(f"Transferring backup file: {backup_file}")
                        transfer_success = transfer_file(config, backup_file)
                        
                        if transfer_success:
                            logger.info("Backup process completed successfully")
                            # Update state only after successful backup and transfer
                            state = new_state
                            save_state(state_file, state)
                        else:
                            logger.error("Failed to transfer backup file")
                    else:
                        logger.error("Could not determine backup file path")
                else:
                    logger.error(f"Backup script failed: {error}")
            else:
                # No changes detected, just update the state if needed
                if new_state != state:
                    state = new_state
                    save_state(state_file, state)
            
            # Wait for the next polling interval
            polling_interval = config.get("api", {}).get("polling_interval_seconds", 30)
            logger.debug(f"Waiting {polling_interval} seconds before next poll")
            
            # Break the sleep into smaller chunks to respond to signals quickly
            for _ in range(polling_interval):
                if not running:
                    break
                time.sleep(1)
                
        except Exception as e:
            logger.error(f"Error in main loop: {str(e)}")
            # Wait before retrying
            time.sleep(5)
    
    logger.info("Service shutting down gracefully")

if __name__ == "__main__":
    main()