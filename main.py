import sys
import os

from quotes import QuotesManager

print("Current working directory:", os.getcwd())
print("PYTHONPATH:", sys.path)

# __ add src directory to PYTHONPATH __
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))
print("Updated PYTHONPATH:", sys.path)

quotes_manager = QuotesManager()
quotes_manager.start()
