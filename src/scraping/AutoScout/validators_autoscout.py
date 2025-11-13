# validators_autoscout.py
import re
from typing import Tuple, List, Dict
from src.scraping.AutoScout.db.models import ListingSummary


def _norm(s: str | None) -> str:
    return (s or "").strip().lower()

def _extract_year(first_registration: str | None) -> int | None:
    """Extract year from strings like '01/2017', '2017', '01-2017'."""
    if not first_registration:
        return None
    m = re.search(r"(20\d{2}|19\d{2})", first_registration)
    return int(m.group(1)) if m else None

def validate_listing(ls: ListingSummary,
                     min_year: int = 2015,
                     max_price: int = 9000,
                     max_mileage: int = 100_000,
                     required_seller: str = "Privato",
                     allowed_fuels: list[str] | None = None,
                     strict: bool = True) -> Tuple[bool, List[str]]:
    """
    Return (is_valid, reasons[]) for a single listing.
    If strict=True, missing values count as invalid.
    """
    reasons: List[str] = []

    # year
    year = _extract_year(ls.first_registration)
    if year is None:
        if strict: reasons.append("anno mancante")
    elif year < min_year:
        reasons.append(f"anno < {min_year} (trovato {year})")

    # price
    if ls.price_eur_num is None:
        if strict: reasons.append("prezzo mancante")
    elif ls.price_eur_num > max_price:
        reasons.append(f"prezzo > {max_price} (trovato {ls.price_eur_num})")

    # mileage
    if ls.mileage_num is None:
        if strict: reasons.append("chilometraggio mancante")
    elif ls.mileage_num > max_mileage:
        reasons.append(f"km > {max_mileage} (trovato {ls.mileage_num})")

    # seller
    seller = (ls.seller_type or "").strip().lower()
    if not seller:
        if strict: reasons.append("venditore mancante")
    elif required_seller and seller != required_seller.lower():
        reasons.append(f"venditore != {required_seller} (trovato {ls.seller_type})")

    # fuel (if a whitelist is provided)
    if allowed_fuels is not None:
        fuel = _norm(ls.fuel_text)
        allowed = {_norm(x) for x in allowed_fuels}
        if not fuel:
            if strict: reasons.append("carburante mancante")
        elif fuel not in allowed:
            reasons.append(f"carburante non consentito (trovato {ls.fuel_text})")

    # Note: raggio/ZIP non verificabili senza geocoding -> si assume ok
    return (len(reasons) == 0, reasons)

def filter_listings_for_request(rows: List[ListingSummary],
                                min_year: int = 2015,
                                max_price: int = 9000,
                                max_mileage: int = 100_000,
                                required_seller: str = "Privato",
                                allowed_fuels: list[str] | None = None,
                                strict: bool = True) -> Tuple[List[ListingSummary]]:
    """Split into valid and rejected with reasons."""
    valids: List[ListingSummary] = []
    rejected: List[Dict] = []
    for ls in rows:
        ok, why = validate_listing(
            ls, min_year, max_price, max_mileage, required_seller,
            allowed_fuels=allowed_fuels,
            strict=strict
        )
        if ok:
            valids.append(ls)
        else:
            rejected.append({"listing_id": ls.listing_id, "reasons": why, "title": ls.title})

    # Log (or print) rejected items with reasons
    for r in rejected:
        print(f"[SKIP] {r['listing_id']}: {', '.join(r['reasons'])}  | {r.get('title', '')}")

    return valids
