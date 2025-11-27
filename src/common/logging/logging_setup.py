import logging
import logging.config
import yaml
from pathlib import Path
from datetime import datetime
from src.common.paths import LOGS_DIR


def setup_logging_for_app(
    app_name: str,
    config_path: Path | None = None,
    logs_base_dir: Path | None = None,
    configure_yfinance: bool = False,
) -> logging.Logger:
    """
    Initialize logging for a specific application.

    - app_name: logical name of the app ("autoscout", "quotes", "stocks", ...)
    - config_path: optional path to YAML logging config
    - logs_base_dir: optional base directory for logs (default: project_root/logs)
    - configure_yfinance: attach RaiseOnErrorHandler to 'yfinance' logger if True
    """

    print(f"Setting up logger for app: {app_name}...")

    # Default config path (you can adjust this relative position as needed)
    if config_path is None:
        config_path = Path(__file__).parent.joinpath("logger.conf.yaml")

    # Use project-level logs dir by default
    if logs_base_dir is None:
        logs_base_dir = LOGS_DIR

    # Each app logs in its own subfolder: logs/<app_name>/
    app_log_dir = logs_base_dir.joinpath(app_name)
    app_log_dir.mkdir(parents=True, exist_ok=True)

    # Dynamic filename: <app_name>_YYYYMMDD_HHMMSS.log
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = app_log_dir.joinpath(f"{app_name}_{timestamp}.log")

    # Load YAML config
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    # Update file handlers to point to the app-specific log file
    # (adjust this if you have multiple file handlers)
    if "handlers" in config and "file" in config["handlers"]:
        config["handlers"]["file"]["filename"] = str(log_file)

    # Apply config
    logging.config.dictConfig(config)

    # app logger
    root_logger = logging.getLogger(app_name)

    # Optional: configure yfinance logger globally
    if configure_yfinance:
        from src.stock.src.RaiseOnErrorHandler import RaiseOnErrorHandler
        yf_logger = logging.getLogger("yfinance")
        yf_logger.setLevel(logging.ERROR)
        error_handler = RaiseOnErrorHandler(flush_delay=1.5)
        yf_logger.addHandler(error_handler)

    return root_logger
