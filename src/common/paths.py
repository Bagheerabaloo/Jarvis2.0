# src/common/paths.py
from pathlib import Path

# Adjust the number of parents depending on where this file lives
PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOGS_DIR = PROJECT_ROOT / "logs"
