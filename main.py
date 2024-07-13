import sys
import os

from quotes import QuotesManager

# __ add src directory to PYTHONPATH __
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

quotes_manager = QuotesManager()
quotes_manager.start()
