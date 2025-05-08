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

# Import utility functions
from utils.config import load_config
from utils.env_loader import load_environment_variables, apply_env_to_config
from utils.state import load_state, save_state, update_state
from services.api_poller import poll_api
from services.backup_trigger import run_backup_script, get_latest_backup_file
from services.transfer import transfer_file

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
    log_file = log_config.get("file", "logs/backup_service.log")
    
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

def process_blueprint_changes(config, blueprint_id, blueprint_name):
    """
    Process changes for a specific blueprint.
    
    Args:
        config (dict): Configuration dictionary
        blueprint_id (str): ID of the blueprint with changes
        blueprint_name (str): Name of the blueprint with changes
        
    Returns:
        bool: True if backup and transfer succeeded, False otherwise
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Processing changes for blueprint: {blueprint_name} ({blueprint_id})")
    
    # Run backup script
    backup_script = config.get("backup", {}).get("script_path") or "/usr/sbin/aos_backup"
    
    # Add blueprint ID as a parameter if not already in the parameters list
    backup_params = config.get("backup", {}).get("parameters", []).copy()
    if "--blueprint" not in " ".join(backup_params):
        backup_params.extend(["--blueprint", blueprint_id])
    
    success, output, error = run_backup_script(backup_script, backup_params)
    
    if success:
        # Get the backup file path from the output
        backup_file = get_latest_backup_file(output)
        if backup_file:
            # Transfer the backup file
            logger.info(f"Transferring backup file for {blueprint_name}: {backup_file}")
            transfer_success = transfer_file(config, backup_file, blueprint_id, blueprint_name)
            
            if transfer_success:
                logger.info(f"Backup process for {blueprint_name} completed successfully")
                return True
            else:
                logger.error(f"Failed to transfer backup file for {blueprint_name}")
                return False
        else:
            logger.error(f"Could not determine backup file path for {blueprint_name}")
            return False
    else:
        logger.error(f"Backup script failed for {blueprint_name}: {error}")
        return False

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
    
    # Verify transfer configuration
    transfer_config = config.get("transfer", {})
    if "username" not in transfer_config:
        logger.warning("Remote username not found in configuration")
        logger.warning("Make sure REMOTE_USERNAME is set in environment or .env file")

    
    # Load initial state
    state_file = config.get("state", {}).get("file_path", "data/backup_state.json")
    state = load_state(state_file)
    
    # Initialize blueprints state if not present
    if "blueprints" not in state:
        state["blueprints"] = {}
    
    # Set up signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)
    
    logger.info("Starting API polling and backup service")
    
    # For compatibility with older code - handle single blueprint configuration
    api_config = config.get("api", {})
    if "endpoint" in api_config and "blueprints" not in api_config:
        logger.info("Converting single blueprint configuration to multi-blueprint format")
        api_config["blueprints"] = [{
            "id": "default",
            "name": "Default Blueprint",
            "endpoint": api_config["endpoint"]
        }]
        config["api"] = api_config
    
    # Main polling loop
    global running
    while running:
        try:
            # Poll the API and check for changes across all blueprints
            changes_by_blueprint, new_state, token = poll_api(config, state)
            
            if not changes_by_blueprint:
                logger.warning("Failed to poll API or no blueprints configured")
                time.sleep(5)
                continue
            
            # Process each blueprint with changes
            blueprints_updated = []
            for blueprint_id, has_changes in changes_by_blueprint.items():
                if has_changes:
                    # Get blueprint details from new state
                    blueprint_state = new_state["blueprints"].get(blueprint_id, {})
                    blueprint_name = blueprint_state.get("blueprint_name", blueprint_id)
                    
                    logger.info(f"Changes detected in blueprint: {blueprint_name} ({blueprint_id})")
                    
                    # Process changes for this blueprint
                    success = process_blueprint_changes(config, blueprint_id, blueprint_name)
                    
                    if success:
                        # Update state only for successful backup
                        blueprints_updated.append(blueprint_id)
            
            # Update state after processing all changes
            if blueprints_updated:
                # Only update state for blueprints that were successfully backed up
                for blueprint_id in blueprints_updated:
                    state["blueprints"][blueprint_id] = new_state["blueprints"][blueprint_id]
                save_state(state_file, state)
            elif new_state != state:
                # If no backups were needed but state changed, update it
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