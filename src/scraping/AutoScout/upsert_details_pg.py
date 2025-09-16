from __future__ import annotations
import json, hashlib
from datetime import datetime
from typing import Dict, Any
import sqlalchemy as sa
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert as pg_insert
from src.scraping.AutoScout.db.models import ListingDetail

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

def upsert_listing_detail(session: Session, row: Dict[str, Any]) -> None:
    if not row.get("listing_id"):
        return
    r = dict(row)
    r["source_hash"] = _compute_detail_hash(r)
    r["last_scraped_at"] = datetime.utcnow()

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
            }
        )
    )
    session.execute(stmt)
