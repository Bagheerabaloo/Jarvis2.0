from dotenv import load_dotenv
from pathlib import Path
import sys
import os

# __ add src directory to PYTHONPATH __
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

env_path = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=env_path)


from quotes import QuotesManager

quotes_manager = QuotesManager()
quotes_manager.start()
