"""
Environment variable management for the API polling and backup service.
Supports loading from .env file or directly from environment.
"""
import os
import logging
from pathlib import Path
import sys

# Add support for python-dotenv if available
try:
    from dotenv import load_dotenv
    DOTENV_AVAILABLE = True
except ImportError:
    DOTENV_AVAILABLE = False

logger = logging.getLogger(__name__)

def load_environment_variables(env_file=None, required_vars=None, optional_vars=None):
    """
    Load environment variables from .env file and/or system environment.
    
    Args:
        env_file (str, optional): Path to .env file. Defaults to searching in standard locations.
        required_vars (list, optional): List of required variable names.
        optional_vars (list, optional): List of optional variable names.
    
    Returns:
        dict: Dictionary containing all loaded environment variables
        bool: True if all required variables were loaded, False otherwise
    """
    # Initialize result dictionary
    env_vars = {}
    
    # Set default required and optional variable lists if not provided
    if required_vars is None:
        required_vars = ["APSTRA_USERNAME", "APSTRA_PASSWORD"]
    
    if optional_vars is None:
        optional_vars = ["REMOTE_USERNAME", "REMOTE_PASSWORD", "SSH_KEY_PATH", "SSH_KEY_PASSPHRASE"]
    
    # Try to load from .env file if dotenv is available
    if DOTENV_AVAILABLE:
        # Determine .env file location
        if env_file is None:
            # Check common locations for .env file
            potential_locations = [
                ".env",                           # Current directory
                Path(__file__).parent / ".env",   # Same directory as this script
                Path(__file__).parent.parent / ".env",  # Parent directory
                Path(__file__).parent.parent.parent / ".env",  # Project root
                Path.home() / ".env",             # User's home directory
            ]
            
            for location in potential_locations:
                if Path(location).is_file():
                    env_file = location
                    break
        
        # Load .env file if found
        if env_file and Path(env_file).is_file():
            logger.info(f"Loading environment variables from: {env_file}")
            load_dotenv(env_file)
            logger.info("Environment variables loaded from .env file")
        else:
            logger.warning("No .env file found, will use system environment variables only")
    else:
        logger.warning("python-dotenv not installed, cannot load .env file. Using system environment variables only.")
    
    # Collect all variables from environment (this includes ones loaded from .env file and system environment)
    for var in required_vars + optional_vars:
        env_vars[var] = os.environ.get(var)
    
    # Check if all required variables are present
    missing_vars = [var for var in required_vars if not env_vars.get(var)]
    
    if missing_vars:
        env_help_message = f"""
Missing essential environment variables: {', '.join(missing_vars)}

How to set environment variables:

1. Create a .env file in the project root with:
   {os.linesep.join([f'{var}=your_{var.lower()}' for var in missing_vars])}

2. Or set directly in your terminal:

   In Linux/macOS terminal:
     {os.linesep.join([f'export {var}=your_{var.lower()}' for var in missing_vars])}

   In Windows Command Prompt:
     {os.linesep.join([f'set {var}=your_{var.lower()}' for var in missing_vars])}

   In Windows PowerShell:
     {os.linesep.join([f'$env:{var}="your_{var.lower()}"' for var in missing_vars])}
  
To make env vars persistent, add them to your shell profile (~/.bashrc, ~/.bash_profile, etc.)
        """
        logger.warning(env_help_message)
        return env_vars, False
    
    return env_vars, True

def apply_env_to_config(config, env_vars):
    """
    Apply environment variables to configuration object.
    
    Args:
        config (dict): Configuration dictionary
        env_vars (dict): Environment variables dictionary
    
    Returns:
        dict: Updated configuration with environment variables applied
    """
    # Create a new config to avoid modifying the original
    updated_config = config.copy()
    
    # Apply API authentication if config has api section
    if "api" in updated_config and env_vars.get("APSTRA_USERNAME") and env_vars.get("APSTRA_PASSWORD"):
        updated_config["api"]["username"] = env_vars.get("APSTRA_USERNAME")
        updated_config["api"]["password"] = env_vars.get("APSTRA_PASSWORD")
    
    # Apply transfer authentication if config has transfer section
    if "transfer" in updated_config:
        transfer_config = updated_config["transfer"]
        
        # Add username if available
        if env_vars.get("REMOTE_USERNAME"):
            transfer_config["username"] = env_vars["REMOTE_USERNAME"]
        
        # Add password if available
        if env_vars.get("REMOTE_PASSWORD"):
            transfer_config["password"] = env_vars["REMOTE_PASSWORD"]
        
        # Add SSH key path if available
        if env_vars.get("SSH_KEY_PATH"):
            transfer_config["ssh_key_path"] = env_vars["SSH_KEY_PATH"]
            
        # Add SSH key passphrase if available and needed
        if env_vars.get("SSH_KEY_PASSPHRASE") and env_vars.get("SSH_KEY_PATH"):
            transfer_config["ssh_key_passphrase"] = env_vars["SSH_KEY_PASSPHRASE"]
    
    return updated_config