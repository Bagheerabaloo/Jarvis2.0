import asyncio
import yfinance as yf
import os
import sys
import datetime
import pandas_market_calendars as mcal

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from src.common.telegram_manager.TelegramBot import TelegramBot
from src.common.file_manager.FileManager import FileManager
from src.stock.src.database import session_local
from stock.src.indexes.sp500.sp500Handler import SP500Handler


def is_us_market_open():
    # Get the current date and time
    now = datetime.datetime.now()

    # Configure the market calendar for the US stock market (NYSE)
    nyse = mcal.get_calendar('NYSE')

    # Retrieve the market schedule for the current date
    schedule = nyse.schedule(start_date=now.date(), end_date=now.date())

    if schedule.empty:
        # The current date is not a trading day
        return False

    # If the schedule exists, it means the market is open on this date
    return True


def get_daily_gainers_losers(_session):
    # __ get the S&P 500 symbols __
    sp500_handler = SP500Handler()
    tickers = sp500_handler.get_sp500_from_wikipedia()

    data = yf.download(tickers, period="5d", interval="1d")["Adj Close"]
    daily_returns = data.pct_change().iloc[-1].reset_index()
    daily_returns.rename(columns={daily_returns.columns[1]: 'Daily Return'}, inplace=True)
    sorted_gainers = daily_returns.sort_values(by=["Daily Return"], ascending=False)
    sorted_gainers["Daily Return"] = sorted_gainers["Daily Return"] * 100
    print("Top 5 daily gainers:")
    print(sorted_gainers.head(5))
    print("\nTop 5 daily losers:")
    print(sorted_gainers.tail(5))

    telegram_token_key = "TELEGRAM_TOKEN"
    config_manager = FileManager()
    token = config_manager.get_telegram_token(database_key=telegram_token_key)
    admin_info = config_manager.get_admin()
    telegram_bot = TelegramBot(token=token)

    text1 = "Top 5 daily gainers:\n" + '\n'.join([f"{row['Ticker']}:    {round(row['Daily Return'],2)}%" for index, row in sorted_gainers.head(5).iterrows()])
    text2 = "Top 5 daily losers:\n" + '\n'.join([f"{row['Ticker']}:    {round(row['Daily Return'],2)}%" for index, row in sorted_gainers.tail(5).sort_values(by=["Daily Return"]).iterrows()])
    asyncio.run(telegram_bot.send_message(chat_id=admin_info["chat"], text=f"{text1}\n\n{text2}"))

# __ sqlAlchemy __ create new session
is_market_open = is_us_market_open()

if is_market_open:
    session = session_local()
    get_daily_gainers_losers(_session=session)

# while True:
#     eta_1 = build_eta(target_hour=10, target_minute=1)
#     eta_2 = build_eta(target_hour=15, target_minute=00)
#     eta_3 = build_eta(target_hour=20, target_minute=00)
#     eta_4 = build_eta(target_hour=21, target_minute=50)
#     eta_5 = build_eta(target_hour=22, target_minute=1)
#     next_eta = min(eta_1, eta_2, eta_3, eta_4, eta_5)
#     print(f"Next run in {seconds_to_time_str(next_eta)}")
#     sleep(next_eta)
#     get_daily_gainers_losers(_session=session)

