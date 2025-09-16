import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import pandas as pd

from src.stock.src.db.database import session_local

from src.stock.src.db.models import InsiderTransactions
from src.stock.src.insider_txs_utils import add_state_and_price, LOGGER

# ---- tune these ----
CHUNK_SIZE = 100_000                         # adjust to your RAM
STAGING = "insider_transactions_staging"     # temp/staging table name

def backup_and_bulk_update():
    session = session_local()
    try:
        # 0) Basic info (row count)
        total = session.execute(text("SELECT COUNT(*) FROM insider_transactions")).scalar()
        LOGGER.info(f"Rows to process: {total}")

        # 1) Backup table (CTAS)
        ts = datetime.datetime.now().strftime("%Y%m%d%H%M")
        backup_table = f"insider_transactions_backup_{ts}"
        with session.bind.begin() as conn:
            conn.execute(text(f"CREATE TABLE {backup_table} AS TABLE insider_transactions"))
        LOGGER.info(f"Backup created: {backup_table}")

        # 2) Create empty staging table
        with session.bind.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {STAGING}"))
            conn.execute(text(f"""
                CREATE TABLE {STAGING} (
                    ticker_id   INTEGER NOT NULL,
                    insider     VARCHAR(255) NOT NULL,
                    start_date  DATE NOT NULL,
                    last_update TIMESTAMP WITHOUT TIME ZONE NOT NULL,
                    shares      BIGINT NOT NULL,
                    value       NUMERIC,
                    price       VARCHAR(64),
                    avg_price   NUMERIC(18,6),
                    state       VARCHAR(24)
                )
            """))
        LOGGER.info(f"Staging table ready: {STAGING}")

        # 3) Stream rows in chunks, compute price/state/avg_price, append to staging
        #    We pull only the columns we need.
        base_sql = """
            SELECT ticker_id, insider, start_date, last_update, shares, value, text
            FROM insider_transactions
            ORDER BY ticker_id, start_date, last_update, insider, shares, value
        """

        rows_processed = 0
        for chunk in pd.read_sql_query(base_sql, session.bind, chunksize=CHUNK_SIZE):
            # compute columns using your existing function
            enriched = add_state_and_price(
                chunk.copy(),
                text_col="text", price_col="price", state_col="state"
            )

            # keep only key + new columns to shrink memory/IO
            out = enriched[[
                "ticker_id", "insider", "start_date", "last_update", "shares", "value",
                "price", "avg_price", "state"
            ]]

            # fast append to staging (single transaction per chunk)
            out.to_sql(STAGING, session.bind, if_exists="append", index=False, method="multi", chunksize=20_000)

            rows_processed += len(out)
            LOGGER.info(f"Staged: {rows_processed}/{total}")

        # 4) Add indexes on staging to speed up the join
        with session.bind.begin() as conn:
            conn.execute(text(f"CREATE INDEX ix_stg_key ON {STAGING} (ticker_id, insider, start_date, last_update, shares, value)"))
            conn.execute(text(f"ANALYZE {STAGING}"))

        # 5) Single set-based UPDATE using JOIN
        #    Match on your composite primary key and update only the 3 new columns.
        with session.bind.begin() as conn:
            conn.execute(text(f"""
                UPDATE insider_transactions AS t
                SET price = s.price,
                    avg_price = s.avg_price,
                    state = s.state
                FROM {STAGING} AS s
                WHERE t.ticker_id = s.ticker_id
                  AND t.insider = s.insider
                  AND t.start_date = s.start_date
                  AND t.last_update = s.last_update
                  AND t.shares = s.shares
                  AND (
                        (t.value IS NULL AND s.value IS NULL)
                     OR (t.value = s.value)
                  )
            """))

        LOGGER.info("Set-based UPDATE completed.")

        # 6) (Optional) Clean up staging
        with session.bind.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {STAGING}"))

        session.commit()
        LOGGER.info("All done.")

    except Exception as e:
        session.rollback()
        LOGGER.error(f"Error during bulk update: {e}")
        # keep staging/backup for inspection on error
        raise
    finally:
        session.close()

if __name__ == "__main__":
    backup_and_bulk_update()