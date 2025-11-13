from __future__ import annotations
import json, hashlib
import re
from datetime import datetime
from typing import Dict, Any
import sqlalchemy as sa
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert as pg_insert
from src.scraping.AutoScout.db.models import ListingDetail, ListingSummary

DETAIL_HASH_FIELDS = [
    "title","model_version","price_text","price_label","location_text","seller_type",
    "overview_mileage","overview_gearbox","overview_year","overview_fuel","overview_power",
    "main_image_url","carfax_url",
    "fin_auto_price","fin_down_payment","fin_duration","fin_amount","fin_total_due","fin_taeg","fin_tan","fin_installment",
    "basic_body","basic_vehicle_type","basic_seats","basic_doors","basic_neopatentati",
    "hist_mileage","hist_year","hist_last_service","hist_owners","hist_service_book","hist_non_smoker",
    "tech_power","tech_gearbox","tech_displacement","tech_cylinders","tech_weight",
    "env_emission_class","env_fuel","env_consumption",
    "equip_comfort_json","equip_media_json","equip_safety_json","equip_extra_json",
    "seller_notes","seller_email",
]

def _compute_detail_hash(row: Dict[str, Any]) -> str:
    payload = {k: row.get(k) for k in DETAIL_HASH_FIELDS}
    blob = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


# --- Parse helpers (robust against formats like "09/2018", "2018", "89.000 km", "1.598 cm³")
def _parse_first_reg(s: str | None) -> tuple[int | None, int | None]:
    """Return (year, month) from 'MM/YYYY' or 'YYYY'. If missing, (None, None)."""
    if not s:
        return None, None
    s = s.strip()
    m = re.search(r'(?:(\d{1,2})\s*[-/])?\s*(\d{4})', s)
    if not m:
        return None, None
    month = int(m.group(1)) if m.group(1) else 6  # default June if only year
    year = int(m.group(2))
    return year, month

def _years_since(year: int | None, month: int | None) -> float | None:
    if not year:
        return None
    month = month or 6
    now = datetime.utcnow()
    months = (now.year - year) * 12 + (now.month - month)
    return max(0.0, months / 12.0)

def _parse_int(s: str | None) -> int | None:
    if not s:
        return None
    digits = re.sub(r"[^\d]", "", s)
    return int(digits) if digits else None

def _label_bonus(label: str | None) -> float:
    """Map AutoScout price labels to a small bonus/malus."""
    if not label:
        return 0.0
    lbl = label.lower()
    # adjust to what you actually see in your dataset
    if "super prezzo" in lbl:
        return 0.15
    if "ottimo prezzo" in lbl:
        return 0.10
    if "buon prezzo" in lbl or "buon" in lbl:
        return 0.05
    if "nella media" in lbl:
        return 0.0
    if "caro" in lbl or "alto" in lbl:
        return -0.10
    return 0.0

def compute_kpi_light(summary, detail) -> float | None:
    """
    KPI Light in [0..1] using:
    - Price (lower is better, cap at 9k)
    - Age (newer is better, 0..10 years scale)
    - Mileage (lower is better, 0..100k scale)
    + Price label bonus

    Weights (sum to ~1 before bonus):
      price  0.45
      age    0.25
      km     0.30
    """
    # --- inputs
    price = summary.price_eur_num
    mileage = summary.mileage_num
    y, m = _parse_first_reg(summary.first_registration or (detail.overview_year if detail else None))
    age_years = _years_since(y, m)

    # --- normalize (clamped 0..1, higher is better)
    # price: 0 at 9k or more, 1 near 0
    if price is None:
        price_score = None
    else:
        price_score = max(0.0, min(1.0, 1.0 - (price / 9000.0)))

    # age: 1 at 0 years, 0 at 10+ years
    if age_years is None:
        age_score = None
    else:
        age_score = max(0.0, min(1.0, 1.0 - (age_years / 10.0)))

    # mileage: 1 at 0 km, 0 at 100k+ km
    if mileage is None:
        km_score = None
    else:
        km_score = max(0.0, min(1.0, 1.0 - (mileage / 100_000.0)))

    # if everything missing, skip
    parts = [x for x in (price_score, age_score, km_score) if x is not None]
    if not parts:
        return None

    # weighted average (renormalize weights ignoring missing parts)
    weights = []
    vals = []
    if price_score is not None:
        weights.append(0.45); vals.append(price_score)
    if age_score is not None:
        weights.append(0.25); vals.append(age_score)
    if km_score is not None:
        weights.append(0.30); vals.append(km_score)

    if not weights:
        return None

    wsum = sum(weights)
    base = sum(v * w for v, w in zip(vals, weights)) / wsum

    # price label bonus/malus
    bonus = _label_bonus(detail["price_label"] or summary.price_label)
    score = max(0.0, min(1.0, base + bonus))

    return round(score, 4)

def upsert_listing_detail(session: Session, row: Dict[str, Any]) -> None:
    if not row.get("listing_id"):
        return
    r = dict(row)
    r["source_hash"] = _compute_detail_hash(r)
    r["last_scraped_at"] = datetime.utcnow()

    # --- compute KPI Light before upsert ---
    try:
        summary_obj = session.get(ListingSummary, r["listing_id"])
        r["kpi_light"] = compute_kpi_light(summary_obj, r) if summary_obj else None
    except Exception as e:
        print(f"Exception computing kpi_light: {e}")
        r["kpi_light"] = None

    tbl = ListingDetail.__table__
    stmt = (
        pg_insert(tbl)
        .values(r)
        .on_conflict_do_update(
            index_elements=[tbl.c.listing_id],
            set_={
                "title": r.get("title"),
                "model_version": r.get("model_version"),
                "price_text": r.get("price_text"),
                "price_label": r.get("price_label"),
                "location_text": r.get("location_text"),
                "maps_href": r.get("maps_href"),
                "seller_phone": r.get("seller_phone"),
                "seller_type": r.get("seller_type"),
                "overview_mileage": r.get("overview_mileage"),
                "overview_gearbox": r.get("overview_gearbox"),
                "overview_year": r.get("overview_year"),
                "overview_fuel": r.get("overview_fuel"),
                "overview_power": r.get("overview_power"),
                "main_image_url": r.get("main_image_url"),
                "carfax_url": r.get("carfax_url"),
                "fin_auto_price": r.get("fin_auto_price"),
                "fin_down_payment": r.get("fin_down_payment"),
                "fin_duration": r.get("fin_duration"),
                "fin_amount": r.get("fin_amount"),
                "fin_total_due": r.get("fin_total_due"),
                "fin_taeg": r.get("fin_taeg"),
                "fin_tan": r.get("fin_tan"),
                "fin_installment": r.get("fin_installment"),
                "basic_body": r.get("basic_body"),
                "basic_vehicle_type": r.get("basic_vehicle_type"),
                "basic_seats": r.get("basic_seats"),
                "basic_doors": r.get("basic_doors"),
                "basic_neopatentati": r.get("basic_neopatentati"),
                "hist_mileage": r.get("hist_mileage"),
                "hist_year": r.get("hist_year"),
                "hist_last_service": r.get("hist_last_service"),
                "hist_owners": r.get("hist_owners"),
                "hist_service_book": r.get("hist_service_book"),
                "hist_non_smoker": r.get("hist_non_smoker"),
                "tech_power": r.get("tech_power"),
                "tech_gearbox": r.get("tech_gearbox"),
                "tech_displacement": r.get("tech_displacement"),
                "tech_cylinders": r.get("tech_cylinders"),
                "tech_weight": r.get("tech_weight"),
                "env_emission_class": r.get("env_emission_class"),
                "env_fuel": r.get("env_fuel"),
                "env_consumption": r.get("env_consumption"),
                "equip_comfort_json": r.get("equip_comfort_json"),
                "equip_media_json": r.get("equip_media_json"),
                "equip_safety_json": r.get("equip_safety_json"),
                "equip_extra_json": r.get("equip_extra_json"),
                "seller_notes": r.get("seller_notes"),
                "seller_email": r.get("seller_email"),
                "source_hash": r.get("source_hash"),
                "last_scraped_at": r.get("last_scraped_at"),
                "kpi_light": r.get("kpi_light"),
            }
        )
    )
    session.execute(stmt)
