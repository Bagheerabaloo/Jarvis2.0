import re
import pandas as pd
import requests
from datetime import datetime, date
from sqlalchemy import text
from bs4 import BeautifulSoup
from io import StringIO

from src.stock.src.db.database import session_local
from src.stock.src.db.models import SP500Changes

# ----------- CONFIG -----------
WIKI_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36"
)
# ------------------------------

def _get_last_change_date(session) -> date:
    """Return the latest date present in sp_500_changes (or 1900-01-01 if empty)."""
    q = text("SELECT MAX(date) FROM sp_500_changes;")
    res = session.execute(q).scalar()
    return res or date(1900, 1, 1)

def _fetch_selected_changes_table() -> pd.DataFrame:
    """
    Download Wikipedia and return the 'Selected changes...' table as DataFrame.
    It explicitly locates the table under the 'Selected changes...' section via BeautifulSoup,
    then parses only that table HTML to avoid picking the wrong one.
    """
    headers = {"User-Agent": USER_AGENT}
    resp = requests.get(WIKI_URL, headers=headers, timeout=30)
    resp.raise_for_status()

    # Parse the DOM to find the section "Selected changes to the list of S&P 500 components"
    soup = BeautifulSoup(resp.text, "lxml")

    # Find the heading for the selected changes section (h2 or h3 with the specific span id)
    target_span_ids = {
        "Selected_changes_to_the_list_of_S&P_500_components",
        "Selected_changes_to_the_list_of_S.26P_500_components",  # historical encoding variant
    }
    heading = None
    for tag in soup.find_all(["h2", "h3"]):
        span = tag.find("span", {"id": True})
        if span and span.get("id") in target_span_ids:
            heading = tag
            break

    if not heading:
        # Fallback: search by text content if id is not present (Wikipedia sometimes renames anchors)
        for tag in soup.find_all(["h2", "h3"]):
            if "Selected changes" in tag.get_text(strip=True):
                heading = tag
                break

    if not heading:
        raise RuntimeError("Could not locate the 'Selected changes' section heading on Wikipedia.")

    # The table is usually the first <table class="wikitable"> after the heading
    table = heading.find_next("table", {"class": "wikitable"})
    if table is None:
        # Fallback: pick the first wikitable that contains a header cell named 'Added' and 'Removed'
        for tbl in soup.find_all("table", {"class": "wikitable"}):
            header_text = " ".join(th.get_text(" ", strip=True).lower() for th in tbl.find_all("th"))
            if "added" in header_text and "removed" in header_text and "date" in header_text:
                table = tbl
                break

    if table is None:
        raise RuntimeError("Could not find the 'Selected changes' table near the section heading.")

    # Read only this table's HTML with pandas, wrapping in StringIO to avoid FutureWarning
    html_str = str(table)
    dfs = pd.read_html(StringIO(html_str))
    if not dfs:
        raise RuntimeError("pandas could not parse the 'Selected changes' table.")

    df = dfs[0]
    # NEW: coerce schema robustly instead of strict equality check
    df = _coerce_selected_changes_schema(df)
    return df

_TICKER_RE = re.compile(r"\(([A-Z][A-Z0-9\.\-]{0,9})\)")  # captures ABC, BRK.B, BF.B, META, etc.

def _extract_symbol(cell: str | float) -> str | None:
    """Extract a ticker symbol from various Wikipedia cell formats.
    Works with '(ABC)', 'ABC', 'BRK.B', and strips footnote markers like [1]."""
    if cell is None or (not isinstance(cell, str) and not isinstance(cell, (int, float))):
        return None
    s = str(cell).strip()
    if not s or s in {"—", "-", "nan", "NaN"}:
        return None
    # Drop footnote markers like [1], [a]
    s = re.sub(r"\[[^\]]*\]", "", s).strip()
    # Case 1: symbol inside parentheses
    m = _TICKER_RE.search(s)
    if m:
        return m.group(1)
    # Case 2: the whole cell is the ticker (e.g., 'AAPL', 'BRK.B')
    s_plain = s.replace("–", "-").replace("—", "-").strip()
    if re.fullmatch(r"[A-Z][A-Z0-9.\-]{0,9}", s_plain):
        return s_plain
    return None

def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Return a copy with flattened, lowercase, stripped column names."""
    if isinstance(df.columns, pd.MultiIndex):
        # Use the last level that usually contains 'Date', 'Added', 'Removed'
        cols = [str(t[-1]).strip().lower() for t in df.columns.to_flat_index()]
    else:
        cols = [str(c).strip().lower() for c in df.columns]
    df = df.copy()
    df.columns = cols
    return df

def _coerce_selected_changes_schema(df: pd.DataFrame) -> pd.DataFrame:
    """
    Coerce any 'Selected changes' wikitable variant to columns:
    ['date', 'added', 'removed', 'reason?'].

    Handles two common schemas:
    A) ['date','added','removed','reason?']
    B) ['effective date','ticker','security','ticker','security','reason']
       where the first (ticker,security) refers to Added and the second to Removed.
    """
    df = _normalize_columns(df)

    cols = [str(c).strip().lower() for c in df.columns]

    # --- Schema A: already has date/added/removed ---
    if "date" in cols and "added" in cols and "removed" in cols:
        # Select and rename minimal set
        keep = ["date", "added", "removed"]
        if "reason" in cols:
            keep.append("reason")
        out = df[keep].copy()
        out.columns = keep  # normalized already
        # Clean strings
        for c in ["added", "removed", "reason"]:
            if c in out.columns:
                out[c] = out[c].astype(str).str.replace(r"\[[^\]]*\]", "", regex=True).str.strip()
        return out

    # --- Schema B: 'effective date', 'ticker', 'security', 'ticker', 'security', 'reason' ---
    def idx_of(name_sub: str, start_from: int = 0):
        for i in range(start_from, len(cols)):
            if name_sub in cols[i]:
                return i
        return None

    eff_idx = idx_of("effective date") or idx_of("date")
    first_ticker_idx = idx_of("ticker", 0)
    second_ticker_idx = idx_of("ticker", (first_ticker_idx + 1) if first_ticker_idx is not None else 0)
    reason_idx = idx_of("reason")

    if eff_idx is not None and first_ticker_idx is not None and second_ticker_idx is not None:
        # Select the relevant columns by POSITION and reset index to avoid duplicate-index reindexing
        pos_list = [eff_idx, first_ticker_idx, second_ticker_idx] + ([reason_idx] if reason_idx is not None else [])
        tmp = df.iloc[:, pos_list].copy().reset_index(drop=True)

        # Build output without alignment (same fresh index for every Series)
        out = pd.DataFrame({
            "date": pd.to_datetime(tmp.iloc[:, 0], errors="coerce").dt.date,
            "added": tmp.iloc[:, 1].apply(_extract_symbol),
            "removed": tmp.iloc[:, 2].apply(_extract_symbol),
        })

        if reason_idx is not None:
            out["reason"] = (
                tmp.iloc[:, 3].astype(str)
                .str.replace(r"\[[^\]]*\]", "", regex=True)
                .str.strip()
            )

        # Drop rows that have neither added nor removed tickers parsed
        out = out[(out["added"].notna()) | (out["removed"].notna())].reset_index(drop=True)
        return out

    # If neither schema matched, raise with observed headers for debugging
    raise RuntimeError(f"Could not map columns. Seen headers: {df.columns.tolist()}")

def _rows_from_table(df: pd.DataFrame) -> list[dict]:
    """Transform the Wikipedia table into a list of {date, added, removed} dicts."""
    rows = []
    for _, r in df.iterrows():
        try:
            d = pd.to_datetime(str(r["date"]), errors="coerce").date()
        except Exception:
            continue
        add_sym = _extract_symbol(r.get("added"))
        rem_sym = _extract_symbol(r.get("removed"))
        if add_sym or rem_sym:
            rows.append({"date": d, "add": add_sym, "remove": rem_sym})
    return rows

def _records_after(last_date: date, rows: list[dict]) -> list[dict]:
    """Keep only rows strictly after last_date and split into atomic add/remove records."""
    out: list[dict] = []
    for r in rows:
        if r["date"] <= last_date:
            continue
        if r["add"]:
            out.append({"date": r["date"], "ticker": r["add"], "add": True, "remove": False})
        if r["remove"]:
            out.append({"date": r["date"], "ticker": r["remove"], "add": False, "remove": True})
    # Sort by date then by ticker for deterministic output
    out.sort(key=lambda x: (x["date"], x["ticker"]))
    return out

def _build_sql_inserts(records: list[dict]) -> str:
    """Build plain SQL INSERT statements for sp_500_changes (one per row)."""
    if not records:
        return "-- No new S&P 500 changes to insert."
    lines = ["-- New S&P 500 changes from Wikipedia"]
    for r in records:
        d = r["date"].strftime("%Y-%m-%d")
        t = r["ticker"].replace("'", "''")
        a = "true" if r["add"] else "false"
        rm = "true" if r["remove"] else "false"
        lines.append(
            f"INSERT INTO sp_500_changes (date, ticker, add, remove, last_update) "
            f"VALUES ('{d}', '{t}', {a}, {rm}, NOW()) "
            f"ON CONFLICT DO NOTHING;"
        )
    return "\n".join(lines)

def _insert_records_orm(session, records: list[dict]) -> int:
    """Insert records via ORM with ON CONFLICT DO NOTHING behavior (best-effort)."""
    if not records:
        return 0
    # Use bulk_save_objects; if you have a unique index (date, ticker, add, remove) add ON CONFLICT in DB.
    objs = [
        SP500Changes(
            date=r["date"],
            ticker=r["ticker"],
            add=r["add"],
            remove=r["remove"],
            last_update=datetime.utcnow(),
        )
        for r in records
    ]
    try:
        session.bulk_save_objects(objs)
        session.commit()
        print(f"Inserted {len(objs)} new S&P 500 change records via ORM.")
        return len(objs)
    except Exception:
        session.rollback()
        # Fallback row-by-row with ON CONFLICT DO NOTHING to avoid duplicates
        # inserted = 0
        # for r in records:
        #     stmt = text(
        #         "INSERT INTO sp_500_changes (date, ticker, add, remove, last_update) "
        #         "VALUES (:date, :ticker, :add, :remove, NOW()) "
        #         "ON CONFLICT DO NOTHING"
        #     )
        #     session.execute(stmt, r)
        #     inserted += 1
        # session.commit()
        # return inserted

def prepare_sp500_changes_updates(run_db_insert: bool = False) -> str:
    """
    Main entrypoint:
    - Reads last change date from DB
    - Scrapes Wikipedia 'Selected changes...' table
    - Builds atomic add/remove records strictly after last date
    - Optionally inserts them in DB
    - Returns a string with SQL INSERT statements to run/copy if desired
    """
    session = session_local()
    try:
        last_date = _get_last_change_date(session)
        table = _fetch_selected_changes_table()
        rows = _rows_from_table(table)
        records = _records_after(last_date, rows)
        if run_db_insert and records:
            _insert_records_orm(session, records)
        sql = _build_sql_inserts(records)
        # Also print a short summary for quick visibility
        print(f"Last date in DB: {last_date} | New records: {len(records)}")
        print(sql)
        return sql
    finally:
        session.close()

# Example CLI usage:
if __name__ == "__main__":
    # Set run_db_insert=True if you also want to write to DB immediately
    prepare_sp500_changes_updates(run_db_insert=True)
