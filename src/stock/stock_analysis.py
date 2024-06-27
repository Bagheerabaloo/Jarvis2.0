from dataclasses import dataclass, field
import pandas as pd
import pandas_ta as ta
import talib
import numpy as np
from time import time
from typing import List

from stock.database import session_local
from sqlalchemy import func, select
from sqlalchemy.orm import aliased
# from TickerServiceBase import Ticker
from stock.models import CandleAnalysisDay, CandleDataDay, Ticker


if __name__ == "__main__":

    # __ sqlAlchemy __ create new session
    session = session_local()

    #  __ subquery to find the latest available date for each ticker
    latest_date_subquery = (
        session.query(
            CandleDataDay.ticker_id,  # Select ticker_id
            func.max(CandleDataDay.date).label('latest_date')  # Find the maximum date for each ticker_id
        )
        .group_by(CandleDataDay.ticker_id)  # Group results by ticker_id
    ).subquery()

    # __ aliasing CandleDataDay for clearer joins
    candle_data_alias = aliased(CandleDataDay)

    # __ main query to join tables and get the required details
    query = (
        session.query(
            CandleAnalysisDay,  # Select all columns from CandleAnalysisDay
            Ticker.symbol,  # Select the symbol from Ticker
            candle_data_alias.date  # Select the date from the CandleDataDay alias
        )
        .join(candle_data_alias, CandleAnalysisDay.candle_data_day_id == candle_data_alias.id)  # Join with CandleDataDay on candle_data_day_id
        .join(Ticker, candle_data_alias.ticker_id == Ticker.id)  # Join with Ticker on ticker_id
        .join(latest_date_subquery, (candle_data_alias.ticker_id == latest_date_subquery.c.ticker_id) &
              (candle_data_alias.date == latest_date_subquery.c.latest_date))  # Join with subquery to get the latest date for each ticker
    )

    # __ execute the query and fetch all results
    results = query.all()

    # __ convert the query results to a list of dictionaries
    results_as_dicts = [
        {**result[0].__dict__, 'symbol': result[1], 'latest_date': result[2]} for result in results
    ]

    # Remove the '_sa_instance_state' from each dictionary
    for record in results_as_dicts:
        record.pop('_sa_instance_state', None)

    # Create a DataFrame from the list of dictionaries
    df = pd.DataFrame(results_as_dicts)

    # Display the DataFrame
    print(df)




