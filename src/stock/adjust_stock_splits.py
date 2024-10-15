from stock.src.database import session_local
from sqlalchemy import desc, asc
# from TickerServiceBase import Ticker
from stock.src.models import Action, CandleDataDay, Ticker
from datetime import datetime


if __name__ == "__main__":

    # __ sqlAlchemy __ create new session
    session = session_local()

    # __ query to get the oldest 'last_update' __
    oldest_last_update = (session
                          .query(CandleDataDay)
                          .order_by(asc(CandleDataDay.last_update))
                          .first()
                          )
    oldest_last_update = oldest_last_update.last_update

    print(oldest_last_update)

    # __ query to get the latest stock splits
    latest_stock_splits = (
        session
        .query(Action)
        .filter(
            Action.stock_splits.isnot(None),
            Action.stock_splits > 0, # Filter to exclude stock splits that are 0
            Action.date >= oldest_last_update  # Filter to get stock splits after the oldest update
            )
        .order_by(desc(Action.date))
        .all()
    )

    for stock_split in latest_stock_splits:
        # __ fetch the ticker_id from the ticker symbol __
        ticker = session.query(Ticker).filter(Ticker.id == stock_split.ticker_id).first()

        # __ query to get CandleDataDay entries where last_update is less than the latest stock split date
        updated_rows = (
            session
            .query(CandleDataDay)
            .filter(
                CandleDataDay.last_update < stock_split.date,  # Compare last_update with the latest stock split date
                CandleDataDay.ticker_id == ticker.id
            )
            .update({
                CandleDataDay.open: CandleDataDay.open / stock_split.stock_splits,
                CandleDataDay.high: CandleDataDay.high / stock_split.stock_splits,
                CandleDataDay.low: CandleDataDay.low / stock_split.stock_splits,
                CandleDataDay.close: CandleDataDay.close / stock_split.stock_splits,
                CandleDataDay.volume: CandleDataDay.volume * stock_split.stock_splits,
                CandleDataDay.adj_close: CandleDataDay.adj_close / stock_split.stock_splits,
                CandleDataDay.last_update: datetime.now()  # Update the last_update to the current
            }, synchronize_session=False)
        )

        print(f"{ticker.symbol} (id:{ticker.id}) - Updated {updated_rows} rows with a stock split of {stock_split.stock_splits}")

    session.commit()

    print("Stock splits adjusted successfully.")