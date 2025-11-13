# -*- coding: utf-8 -*-
import re
from io import StringIO
from datetime import date, datetime
from typing import Dict, List, Set, Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup
from sqlalchemy import text, select

from src.stock.src.db.database import session_local
from src.stock.src.db.models import SP500Historical, SP500Changes, Ticker

# -------------------- Wikipedia helpers --------------------

WIKI_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36"
)

def _clean_symbol_cell(s: str) -> str:
    """Remove footnote markers like [1], trim whitespace, and normalize unicode dashes."""
    if not isinstance(s, str):
        return ""
    s = re.sub(r"\[[^\]]*\]", "", s)  # drop footnotes [1]
    s = s.replace("–", "-").replace("—", "-").strip()
    return s

def _yf_symbol(sym: str) -> str:
    """Yahoo Finance US symbols mostly map '.' -> '-' (e.g., BRK.B -> BRK-B)."""
    return sym.replace(".", "-")

def _fetch_sp500_constituents_from_wikipedia() -> pd.DataFrame:
    """
    Return a DataFrame with at least the column 'Symbol' from the S&P 500 constituents table.
    We explicitly locate the constituents table (not the 'Selected changes...' table).
    """
    headers = {"User-Agent": USER_AGENT}
    resp = requests.get(WIKI_URL, headers=headers, timeout=30)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "lxml")
    # Find the first wikitable whose header contains 'Symbol'
    table = None
    for tbl in soup.find_all("table", {"class": "wikitable"}):
        header_text = " ".join(th.get_text(" ", strip=True) for th in tbl.find_all("th"))
        if re.search(r"\bSymbol\b", header_text, flags=re.I):
            table = tbl
            break
    if table is None:
        raise RuntimeError("Could not locate the S&P 500 constituents table on Wikipedia.")

    df = pd.read_html(StringIO(str(table)))[0]
    # Normalize columns (handles single/multi index)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [str(c[-1]).strip() for c in df.columns.to_flat_index()]
    else:
        df.columns = [str(c).strip() for c in df.columns]

    if "Symbol" not in df.columns:
        # Fallback: try case-insensitive match
        sym_col = next((c for c in df.columns if c.lower() == "symbol"), None)
        if not sym_col:
            raise RuntimeError(f"Parsed table without 'Symbol' column. Columns: {df.columns}")
        df.rename(columns={sym_col: "Symbol"}, inplace=True)

    # Clean symbol values
    df["Symbol"] = df["Symbol"].astype(str).map(_clean_symbol_cell).str.upper()
    return df[["Symbol"]].copy()


# -------------------- DB helpers --------------------

def _get_last_snapshot_date_and_ids(session) -> Tuple[date, Set[int]]:
    """Return (last_date, set_of_ticker_ids) from sp_500_historical. If empty, (None, empty set)."""
    last_date = session.execute(text("SELECT MAX(date) FROM sp_500_historical;")).scalar()
    if not last_date:
        return None, set()

    rows = session.execute(
        text("SELECT ticker_id FROM sp_500_historical WHERE date = :d;"),
        {"d": last_date},
    ).fetchall()
    return last_date, {r[0] for r in rows}

def _get_changes_after(session, dt: date) -> pd.DataFrame:
    """Fetch sp_500_changes strictly after a given date. Returns DataFrame with index=date and cols: ticker_id, add, remove."""
    rows = session.execute(
        text("""
            SELECT date, ticker_id, add, remove
            FROM sp_500_changes
            WHERE date > :d
            ORDER BY date ASC, ticker_id ASC;
        """),
        {"d": dt},
    ).fetchall()
    if not rows:
        return pd.DataFrame(columns=["date", "ticker_id", "add", "remove"]).set_index("date")

    df = pd.DataFrame(rows, columns=["date", "ticker_id", "add", "remove"])
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df.set_index("date", inplace=True)
    return df

def _group_changes_by_date(changes_df: pd.DataFrame) -> Dict[date, Dict[str, Set[int]]]:
    """Group changes by date into {'add': set(ids), 'remove': set(ids)} per day."""
    if changes_df.empty:
        return {}
    grouped: Dict[date, Dict[str, Set[int]]] = {}
    for dt, grp in changes_df.groupby(changes_df.index):
        adds = set(grp.loc[grp["add"] == True, "ticker_id"].tolist())
        rems = set(grp.loc[grp["remove"] == True, "ticker_id"].tolist())
        grouped[dt] = {"add": adds, "remove": rems}
    return dict(sorted(grouped.items(), key=lambda kv: kv[0]))

def _apply_daily_changes(prev_ids: Set[int], daily_ops: Dict[str, Set[int]]) -> Set[int]:
    """
    Apply one day's membership changes to the previous set of ticker_ids.
    Order: (renames would go first, but we don't model them here) -> removes -> adds.
    """
    cur = set(prev_ids)
    # 1) removes (true exits)
    cur.difference_update(daily_ops.get("remove", set()))
    # 2) adds (true entries)
    cur.update(daily_ops.get("add", set()))
    return cur

def _prefetch_symbol_maps(session, ids: Set[int]) -> Tuple[Dict[int, str], Dict[int, str]]:
    """
    Return mappings:
      id -> current ticker.symbol
      id -> yfinance symbol (derived as '.' -> '-')
    """
    if not ids:
        return {}, {}
    q = select(Ticker.id, Ticker.symbol).where(Ticker.id.in_(ids))
    data = session.execute(q).fetchall()
    id2sym = {r[0]: r[1].upper() for r in data}
    id2yf = {tid: _yf_symbol(sym) for tid, sym in id2sym.items()}
    return id2sym, id2yf

def _bulk_upsert_historical(session, snapshots: List[Tuple[date, Set[int]]]) -> int:
    """
    Insert snapshots into sp_500_historical using INSERT ... ON CONFLICT DO NOTHING.
    For each (day, set_of_ids), we insert one row per ticker_id with:
        ticker (legacy) = current Ticker.symbol
        symbol_at_date  = current Ticker.symbol
        ticker_yfinance = derived YF symbol (dot->dash)
    Returns number of attempted inserts (not necessarily number of rows actually inserted).
    """
    if not snapshots:
        return 0

    # Prefetch symbol maps for all involved ids in one shot
    all_ids: Set[int] = set()
    for _, s in snapshots:
        all_ids.update(s)
    id2sym, id2yf = _prefetch_symbol_maps(session, all_ids)

    # Build values list (skip ids we couldn't resolve for some reason)
    values = []
    now_ts = datetime.utcnow()
    for dt_day, idset in snapshots:
        for tid in sorted(idset):
            sym = id2sym.get(tid)
            if not sym:
                continue
            values.append(
                {
                    "date": dt_day,
                    "tid": tid,
                    "ticker": sym,
                    "symbol_at_date": sym,
                    "ticker_yfinance": id2yf.get(tid, _yf_symbol(sym)),
                    "last_update": now_ts,
                }
            )

    if not values:
        return 0

    # Upsert with ON CONFLICT DO NOTHING on PK (date, ticker_id)
    session.execute(
        text("""
            INSERT INTO sp_500_historical
                (date, ticker_id, ticker, symbol_at_date, ticker_yfinance, last_update)
            VALUES
                (:date, :tid, :ticker, :symbol_at_date, :ticker_yfinance, :last_update)
            ON CONFLICT (date, ticker_id) DO NOTHING;
        """),
        values,
    )
    session.commit()
    return len(values)


# -------------------- Public entrypoint --------------------

def update_sp500_historical_from_change(verify_with_wikipedia: bool = True) -> None:
    """
    Build and persist new S&P 500 snapshots from sp_500_changes after the last historical date.
    - Works with the new PK (date, ticker_id).
    - Populates legacy 'ticker' and 'symbol_at_date' for display/audit.
    - Populates 'ticker_yfinance' (used downstream to call yfinance).
    - Optionally verifies the latest snapshot against Wikipedia's current constituents.

    Notes:
    * If sp_500_historical is empty, this function seeds using the current Wikipedia set (today's date).
    * Renames do not appear in sp_500_changes (membership-only), so they do not affect the set size.
    """
    session = session_local()
    try:
        # 0) Determine starting point
        last_date, last_ids = _get_last_snapshot_date_and_ids(session)

        if last_date is None:
            # Seed from Wikipedia (first-time initialization)
            print("[init] No historical snapshots found. Seeding from Wikipedia...")
            wiki_df = _fetch_sp500_constituents_from_wikipedia()
            # Map symbols to ticker_id
            rows = session.execute(
                text("SELECT id, UPPER(symbol) FROM ticker WHERE UPPER(symbol) = ANY(:syms)"),
                {"syms": list(wiki_df["Symbol"].unique())},
            ).fetchall()
            sym2id = {r[1]: r[0] for r in rows}
            missing = sorted(set(wiki_df["Symbol"]) - set(sym2id.keys()))
            if missing:
                # If you want, you can auto-insert stubs here; for now we fail loudly.
                raise RuntimeError(f"[init] Missing symbols in 'ticker': {missing[:10]}... (total {len(missing)})")

            today = date.today()
            snapshots = [(today, set(sym2id[s] for s in wiki_df["Symbol"]))]
            inserted = _bulk_upsert_historical(session, snapshots)
            print(f"[init] Seeded {inserted} rows for {today}.")
            return

        # 1) Gather membership changes strictly after the last snapshot date
        changes_df = _get_changes_after(session, last_date)
        if changes_df.empty:
            print(f"[ok] No changes found after {last_date}. Nothing to do.")
            if verify_with_wikipedia:
                _verify_against_wikipedia(session, last_date)
            return

        # 2) Roll forward day by day using set algebra on ticker_id
        grouped = _group_changes_by_date(changes_df)
        snapshots: List[Tuple[date, Set[int]]] = []
        current_ids = set(last_ids)

        for day, ops in grouped.items():
            current_ids = _apply_daily_changes(current_ids, ops)
            snapshots.append((day, set(current_ids)))

        # 3) Persist new snapshots with proper text fields and yf symbol
        attempted = _bulk_upsert_historical(session, snapshots)
        print(f"[ok] Processed {len(snapshots)} snapshot days since {last_date} "
              f"({attempted} rows attempted, PK will skip existing).")

        # 4) Optional verification against Wikipedia (only last snapshot)
        if verify_with_wikipedia:
            _verify_against_wikipedia(session, snapshots[-1][0])

        # 5) Refresh materialized views if needed
        session.execute(text("REFRESH MATERIALIZED VIEW CONCURRENTLY mv_sp_500_latest_date;"))
        session.commit()
        print("[ok] Materialized views refreshed.")
    finally:
        session.close()


def _verify_against_wikipedia(session, snapshot_date: date) -> bool:
    """
    Compare local snapshot (using ticker_yfinance) vs Wikipedia list normalized to Yahoo style.
    Treats dot/dash variants as equivalent by converting '.' -> '-'.
    """
    # Local: get all yfinance symbols for the snapshot date
    local_yf = session.execute(
        text("""
            SELECT ticker_yfinance
            FROM sp_500_historical
            WHERE date = :d
        """),
        {"d": snapshot_date},
    ).scalars().all()
    local_set = {str(x).upper() for x in local_yf}

    # Wikipedia: fetch symbols and normalize to Yahoo style
    wiki_df = _fetch_sp500_constituents_from_wikipedia()
    wiki_yf = [s.replace(".", "-").upper() for s in wiki_df["Symbol"].tolist()]
    wiki_set = set(wiki_yf)

    if local_set == wiki_set:
        print(f"[verify] Snapshot {snapshot_date} matches Wikipedia ({len(local_set)} constituents).")
        return True

    # Show diffs (already in YF-normalized form)
    missing = sorted(wiki_set - local_set)
    extra   = sorted(local_set - wiki_set)
    print(f"[verify:DIFF] Snapshot {snapshot_date} differs from Wikipedia (YF-normalized).")
    if missing:
        print(f"  - Missing locally (present on Wikipedia): {missing[:20]}{' ...' if len(missing) > 20 else ''}")
    if extra:
        print(f"  - Extra locally (absent on Wikipedia):   {extra[:20]}{' ...' if len(extra) > 20 else ''}")
    return False


if __name__ == "__main__":
    update_sp500_historical_from_change(verify_with_wikipedia=True)