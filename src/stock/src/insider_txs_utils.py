import re
import logging
from typing import Optional, Tuple
import numpy as np
import pandas as pd
from logger_setup import LOGGER

# --------------------------------------------------------------------------------------
# PRECOMPILED REGEX
# --------------------------------------------------------------------------------------
# Price range: "at $25.64 - $26.33 per share", "at price 0.00 - 45.00 per share"
PRICE_RANGE_RE = re.compile(
    r"(?:\bat\b\s+(?:price\s+)?)?\$?\s*(\d+(?:\.\d+)?)\s*[-–—]\s*\$?\s*(\d+(?:\.\d+)?)\s*per\s+share\b",
    re.IGNORECASE,
)

#Single price: "at $5.94 per share", "at price 12.85 per share", "price 97.02 per share"
PRICE_SINGLE_RE = re.compile(
    r"(?:\b(?:at\s+(?:price\s+)?)|(?<!at\s)price\s)\$?\s*(\d+(?:\.\d+)?)\s*per\s+share\b",
    re.IGNORECASE,
)

# --------------------------------------------------------------------------------------
# PATTERNS FOR "STATE" CLASSIFICATION
# Order: first exclude false positives (e.g. "Automatic Sale" is passive),
# then sales/outs, then purchases/ins, then everything else as passive.
# --------------------------------------------------------------------------------------

PAT_PASSIVE = [
    re.compile(r"\bautomatic sale\b", re.I),
    re.compile(r"\bacquisition\s*\(non open market\)", re.I),
    re.compile(r"\bacquisition or disposition by gift\b", re.I),
    re.compile(r"\bcarried out privately\b", re.I),
    re.compile(r"\bin the public market\b", re.I),  # dicitura ambigua → passiva
    re.compile(r"\btake[- ]over bid|merger|acquisition", re.I),
    re.compile(r"\bunder a prospectus\b", re.I),
    re.compile(r"\bprospectus exemption\b", re.I),
    re.compile(r"\bunder a purchase/ownership plan\b", re.I),
    re.compile(r"\bbuy back\b", re.I),   # riacquisto societario
    re.compile(r"\bchange in nature of ownership\b", re.I),
    re.compile(r"\bcompensation for services\b", re.I),
    re.compile(r"\bconversion of exercise of derivative security\b", re.I),
    re.compile(r"\bconversion or exchange\b", re.I),
    re.compile(r"\bdisposition\s*\(non open market\)", re.I),
    re.compile(r"\bexercise for cash\b", re.I),
    re.compile(r"\bexercise of (option|options|rights|warrants)\b", re.I),
    re.compile(r"\bexpiration of options\b", re.I),
    re.compile(r"\bgrant of (options|rights|warrants)\b", re.I),
    re.compile(r"\boption exercise\b", re.I),
    re.compile(r"\bother at\b", re.I),
    re.compile(r"\bredemption, retraction, cancel(l)?ation, repurchase\b", re.I),
    re.compile(r"\bstatement of ownership\b", re.I),
    re.compile(r"\bstock award\(grant\)\b", re.I),          # current working
    re.compile(r"\bstock award\s*\(grant\)", re.I),         # handles optional space + trailing text
    re.compile(r"\bstock award\b", re.I),                   # catches plain "Stock Award" without (Grant)
    re.compile(r"\bstock dividend\b", re.I),
    re.compile(r"\bstock gift\b", re.I),
    re.compile(r"\bstock split or consolidation\b", re.I),
]

PAT_OUT = [
    re.compile(r"^\s*sold\b", re.I),
    re.compile(r"^\s*sale\b", re.I),
    re.compile(r"\bshort sale\b", re.I),
    re.compile(r"^\s*decrease at\b", re.I),
]

PAT_IN = [
    re.compile(r"^\s*acquisition at\b", re.I),
    re.compile(r"^\s*bought\b", re.I),
    re.compile(r"^\s*increase at\b", re.I),
    re.compile(r"^\s*purchase at\b", re.I),
]

ZERO_LIKE = 0.01  #threshold to treat ~0 prices as assignments/plans (passive)


def extract_price_fields(text: str) -> Tuple[Optional[float], Optional[float], Optional[str]]:
    """
    Returns (price_min, price_max, prezzo_str):
    - Se range: price_min, price_max (float), e "min-max" come stringa.
    - Se singolo: price_min==price_max==valore, e "valore" come stringa.
    - Se assente: (None, None, None)
    """
    if not isinstance(text, str):
        return None, None, None

    m = PRICE_RANGE_RE.search(text)
    if m:
        p1 = float(m.group(1))
        p2 = float(m.group(2))
        lo, hi = (p1, p2) if p1 <= p2 else (p2, p1)
        return lo, hi, f"{lo:g}-{hi:g}"

    m = PRICE_SINGLE_RE.search(text)
    if m:
        p = float(m.group(1))
        return p, p, f"{p:g}"

    return None, None, None


def classify_state(text: str, pmin: Optional[float]) -> Optional[str]:
    """
    Classifies the 'State' into: 'Entry' | 'Exit' | 'Passive/Scheduled'
    Returns None if not recognized (a warning will be logged).
    Applies override: entry with price ~0 → 'Passive/Scheduled'.
    """

    if not isinstance(text, str) or not text.strip():
        return None

    # Explicit passive patterns first (to exclude false positives)
    for pat in PAT_PASSIVE:
        if pat.search(text):
            return "Passive/Scheduled"

    # Active exit
    for pat in PAT_OUT:
        if pat.search(text):
            return "Exit"

    # Active entry
    for pat in PAT_IN:
        if pat.search(text):
            # Override if price ~0 → it's very likely a plan/assignment
            if pmin is not None and pmin <= ZERO_LIKE:
                return "Passive/Scheduled"
            return "Entry"

    # Not recognized
    return None


def add_state_and_price(df: pd.DataFrame, text_col: str = "Text",
                        price_col: str = "Price", state_col: str = "State") -> pd.DataFrame:
    """
    Adds 'Stato' and 'Prezzo' columns to the DataFrame based on text analysis.
    - state col: classification into 3 states
    - price col: price as string (single or range "min-max"), if present
    Generates WARNING if a row is not classified.
    Does not modify other columns and does not remove rows.
    """

    if text_col not in df.columns:
        raise KeyError(f"Column '{text_col}' not found in the DataFrame.")

    texts = df[text_col].astype(str)

    # vectorial price extraction
    # 1) range
    pr = texts.str.extract(PRICE_RANGE_RE)
    pr = pr.rename(columns={0: "p_lo", 1: "p_hi"})
    # 2) single price
    ps = texts.str.extract(PRICE_SINGLE_RE).rename(columns={0: "p_single"})

    # Merge
    temp = pd.concat([pr, ps], axis=1)
    for col in ["p_lo", "p_hi", "p_single"]:
        temp[col] = pd.to_numeric(temp[col], errors="coerce")

    # Calculate pmin/pmax + price string
    pmin = temp["p_lo"].combine_first(temp["p_single"])
    pmax = temp["p_hi"].combine_first(temp["p_single"])

    prezzo_str = []
    for lo, hi in zip(pmin, pmax):
        if pd.isna(lo) or pd.isna(hi):
            prezzo_str.append(None)
        elif lo == hi:
            prezzo_str.append(f"{lo:g}")
        else:
            lo2, hi2 = (lo, hi) if lo <= hi else (hi, lo)
            prezzo_str.append(f"{lo2:g}-{hi2:g}")

    df[price_col] = prezzo_str

    # Classification STATE (with logging for unrecognized)
    stati = []
    for t, lo in zip(texts, pmin):
        state = classify_state(t, None if pd.isna(lo) else float(lo))
        if state is None:
            # LOGGER.warning("Transaction NOT found: %s", t)
            state = "Passive/Scheduled"  # fallback prudente
        stati.append(state)

    df[state_col] = stati

    df["avg_price"] = df[price_col].apply(
        lambda s: (
            (lambda a, b: (a + b) / 2)(
                *map(float, s.replace("–", "-").replace("—", "-").split("-"))
            )
            if "-" in s.replace("–", "-").replace("—", "-")
            else float(s)
        ) if isinstance(s, str) and s.strip() != "" else np.nan  # NaN solo se il prezzo manca
    )

    # LOGGER.info(f"Added columns '{state_col}' and '{price_col}' to DataFrame.")
    return df

