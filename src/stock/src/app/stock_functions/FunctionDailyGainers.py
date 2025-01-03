from dataclasses import dataclass
import yfinance as yf
import pandas as pd

from stock.src.app.stock_functions.StockFunction import StockFunction


@dataclass
class FunctionDailyGainers(StockFunction):
    name: str = 'daily_gainers'

    async def state_1(self):
        tickers = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']
        data = yf.download(tickers, period="5d", interval="1d")["Adj Close"]
        daily_returns = data.pct_change().iloc[-1]
        sorted_gainers = daily_returns.sort_values(ascending=False)
        print("Top 5 daily gainers:")
        print(sorted_gainers.head(5))
        print("\nTop 5 daily losers:")
        print(sorted_gainers.tail(5))

        await self.send_message(chat_id=self.chat.chat_id, text='Daily Gainers and Losers', default_keyboard=True)
        self.close_function()