class YFinanceDataError(Exception):
    """Custom exception for yfinance data retrieval errors."""
    def __init__(self, ticker, message="Failed to retrieve data"):
        self.ticker = ticker
        self.message = f"Error for {ticker}: {message}"
        super().__init__(self.message)