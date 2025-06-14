import logging
import logging.config
from pathlib import Path
from datetime import datetime
import yaml
from src.stock.src.RaiseOnErrorHandler import RaiseOnErrorHandler


def setup_logging():
    print("Setting up logger...")

    # Path to the YAML configuration file
    config_path = Path(__file__).parent.joinpath('resources', 'logger.conf.yaml')

    # Directory for log files
    log_dir = Path(__file__).parent.joinpath('logs')
    log_dir.mkdir(exist_ok=True)  # Create the logs directory if it does not exist

    # Dynamically generate the log file name with a timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = log_dir.joinpath(f"log_{timestamp}.txt")

    # Load the YAML configuration and update the log file path
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    config['handlers']['file']['filename'] = str(log_file)

    # Apply the logging configuration
    logging.config.dictConfig(config)

    # Return the root logger
    return logging.getLogger()


# TODO: make it application specific
LOGGER = setup_logging()

# Configure the logger for yfinance
logger = logging.getLogger("yfinance")
logger.setLevel(logging.ERROR)
error_handler = RaiseOnErrorHandler(flush_delay=1.5)  # Wait 1.5s before raising
logger.addHandler(error_handler)

