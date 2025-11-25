from loguru import logger
import json
import sys
import os

'''
To use default config minimum, copy the following code snippet into your main program.
----------------------------------------------------------------------------------------------------------------------------------------

from LogLibrary import Load_Config, Loguru_Logging
# ----------------------- Configuration Values -----------------------
Program_Name = ""        # Program name for identification and logging.
Program_Version = ""            # Program version used for file naming and logging.
# ---------------------------------------------------------------------

default_config = {
            "log_Level": "DEBUG",
            "Log_Console": 1,  # Set to "true" to enable console logging.
            "log_Backup": 90,         # Log retention duration (number of backup files).
            "Log_Size": "10 MB"       # Maximum log file size before rotation. 
        }

config = Load_Config(default_config, Program_Name)
logger = Loguru_Logging(config, Program_Name, Program_Version)

----------------------------------------------------------------------------------------------------------------------------------------
'''
global script_dir

if getattr(sys, 'frozen', False):
    # When packaged into a single executable (e.g., PyInstaller), place files
    # next to the executable to keep configuration and logs with the binary.
    script_dir = os.path.dirname(sys.executable)
else:
    # When running as a normal script, co-locate config/logs with this module.
    script_dir = os.path.dirname(os.path.abspath(__file__))

def Load_Config(default_config, Program_Name):
    """Load or create a JSON config for the application.

    Behavior:
    - If `<Program_Name>_config.json` does not exist, create it with
      `default_config` and write to disk (pretty-printed).
    - Read and return the config as a Python dictionary.

    Args:
        default_config: A dict with default settings to seed the file.
        Program_Name: The app name used to derive the config filename.

    Returns:
        dict: Parsed configuration content.
    """
    # Define the configuration file path.
    config_file_name = f'{Program_Name}_config.json'
    config_path = os.path.join(script_dir, config_file_name)

    # Create config file with default values if it does not exist.
    if not os.path.exists(config_path):
        # Persist defaults so operators can edit them later.
        default_config = default_config 
        with open(config_path, 'w') as new_config_file:
            json.dump(default_config, new_config_file, indent=4)

    # Load configuration
    with open(config_path, 'r') as config_file:
        config = json.load(config_file)
    
    return config

# ----------------------- Loguru Logging Setup -----------------------
def Loguru_Logging(config, Program_Name, Program_Version):
    """Initialize Loguru sinks per configuration.

    Sinks:
    - Console (optional): enabled when `Log_Console` is truthy.
    - File: `<script_dir>/logs/<Program_Name>_<Program_Version>.log` with
      size-based rotation and day-based retention.

    Args:
        config: The configuration dict returned by `Load_Config`.
        Program_Name: Application name (used in file naming and banner).
        Program_Version: Application version (used in file naming and banner).

    Returns:
        loguru.Logger: Configured logger instance ready for use.
    """
    logger.remove()

    log_Backup = int(config.get('log_Backup', 90))
    Log_Size = config.get('Log_Size', '10 MB').upper()
    log_Level = config.get('log_Level', 'DEBUG').upper()

    log_dir = os.path.join(script_dir, 'logs')
    os.makedirs(log_dir, exist_ok=True)

    log_file_name = f'{Program_Name}_{Program_Version}.log'
    log_file = os.path.join(log_dir, log_file_name)

    if config.get('Log_Console', 0) == 1:
        logger.add(
            sys.stdout, 
            level=log_Level, 
            format="<green>{time}</green> | <blue>{level}</blue> | <cyan>{thread.id}</cyan> | <magenta>{function}</magenta> | {message}"
        )

    logger.add(
        log_file,
        format="{time} | {level} | {thread.id} | {function} | {message}",
        level=log_Level,
        rotation=Log_Size,
        retention=f"{log_Backup} days",
        compression="zip"
    )

    logger.info('-' * 117)
    logger.info(f"Start {Program_Name} Version {Program_Version}")
    logger.info('-' * 117)

    return logger