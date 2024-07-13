import sys
import os

# __ add src directory to PYTHONPATH __
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from quotes import QuotesManager

quotes_manager = QuotesManager()
quotes_manager.start()
