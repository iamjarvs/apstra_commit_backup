"""
State management for the API polling and backup service.
"""
import os
import json
import logging
import shutil
from pathlib import Path
from datetime import datetime

logger = logging.getLogger(__name__)

def load_state(state_file_path):
    """
    Load the state from a JSON file.
    
    Args:
        state_file_path (str): Path to the state file
    
    Returns:
        dict: Loaded state or empty dict if file doesn't exist
    """
    try:
        # Create directory if it doesn't exist
        state_dir = os.path.dirname(state_file_path)
        if state_dir:
            os.makedirs(state_dir, exist_ok=True)
        
        # Check if file exists
        if os.path.exists(state_file_path):
            with open(state_file_path, 'r') as file:
                state = json.load(file)
                logger.debug(f"State loaded from {state_file_path}")
                
                # Ensure blueprints section exists
                if "blueprints" not in state:
                    state["blueprints"] = {}
                    
                return state
        else:
            logger.info(f"State file {state_file_path} does not exist, creating new state")
            return {
                "last_poll_time": None,
                "blueprints": {}
            }
    except Exception as e:
        logger.error(f"Error loading state: {str(e)}")
        return {
            "last_poll_time": None,
            "blueprints": {}
        }

def save_state(state_file_path, state):
    """
    Save the state to a JSON file.
    
    Args:
        state_file_path (str): Path to the state file
        state (dict): State to save
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Create directory if it doesn't exist
        state_dir = os.path.dirname(state_file_path)
        if state_dir:
            os.makedirs(state_dir, exist_ok=True)
        
        # Create a backup of the current state file if it exists
        if os.path.exists(state_file_path):
            backup_file = f"{state_file_path}.bak"
            shutil.copy2(state_file_path, backup_file)
            logger.debug(f"Created backup of state file at {backup_file}")
        
        # Update last modification time
        state["last_updated"] = datetime.now().isoformat()
        
        # Write the new state
        with open(state_file_path, 'w') as file:
            json.dump(state, file, indent=2)
            logger.debug(f"State saved to {state_file_path}")
        return True
    except Exception as e:
        logger.error(f"Error saving state: {str(e)}")
        return False

def update_state(state, blueprint_id, revision_id, poll_time, blueprint_name=None):
    """
    Update the state for a specific blueprint.
    
    Args:
        state (dict): Current state dictionary
        blueprint_id (str): ID of the blueprint
        revision_id (str): Latest revision ID
        poll_time (str): Current poll time
        blueprint_name (str, optional): Name of the blueprint
        
    Returns:
        dict: Updated state
    """
    # Ensure blueprints section exists
    if "blueprints" not in state:
        state["blueprints"] = {}
    
    # Update blueprint state
    blueprint_state = state["blueprints"].get(blueprint_id, {})
    blueprint_state["last_revision_id"] = revision_id
    blueprint_state["last_poll_time"] = poll_time
    blueprint_state["blueprint_id"] = blueprint_id
    
    if blueprint_name:
        blueprint_state["blueprint_name"] = blueprint_name
    
    # Update state
    state["blueprints"][blueprint_id] = blueprint_state
    state["last_poll_time"] = poll_time
    
    return state

def get_blueprit_state(state, blueprint_id):
    """
    Get the state for a specific blueprint.
    
    Args:
        state (dict): State dictionary
        blueprint_id (str): ID of the blueprint
        
    Returns:
        dict: Blueprint state or empty dict if not found
    """
    if "blueprints" not in state:
        return {}
    
    return state["blueprints"].get(blueprint_id, {})