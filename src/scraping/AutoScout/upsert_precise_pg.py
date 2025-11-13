from __future__ import annotations
import json, hashlib
import re
from datetime import datetime
from typing import List, Dict, Any

import sqlalchemy as sa
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy import func
from src.scraping.AutoScout.db.models import ListingSummary  # il tuo ORM model (tab listing_summary)

SUMMARY_HASH_FIELDS = [
    "make","model","title","subtitle",
    "price_eur_num","price_text","price_label",
    "mileage_num","mileage_text",
    "gearbox","first_registration","fuel_code","fuel_text","power_text",
    "zip_code","seller_type","location_text",
    "image_url","detail_url",
]

def _compute_hash(row: Dict[str, Any]) -> str:
    payload = {}
    for k in SUMMARY_HASH_FIELDS:
        val = row.get(k)
        if k == "image_url" and val is None:
            val = ""  # normalizziamo None a stringa vuota
        payload[k] = val
    blob = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()

def _prep_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    now = datetime.utcnow()
    out = []
    for r in rows:
        lid = r.get("listing_id")
        if not lid:  # skip malformed
            continue
        r2 = dict(r)

        # --- normalize numeric fields: non-numeric -> None ---
        def _to_int_or_none(val):
            if val is None:
                return None
            if isinstance(val, (int, float)):
                return int(val)
            if isinstance(val, str):
                s = val.strip().lower()
                if s in ("", "unknown", "n.d.", "nd", "none", "null"):
                    return None
                # keep only digits
                digits = re.sub(r"[^\d]", "", s)
                return int(digits) if digits else None
            return None

        r2["position"] = _to_int_or_none(r2.get("position"))
        r2["price_eur_num"] = _to_int_or_none(r2.get("price_eur_num"))
        r2["mileage_num"] = _to_int_or_none(r2.get("mileage_num"))

        r2["source_hash"] = _compute_hash(r2)
        r2.setdefault("first_seen_at", now)
        r2.setdefault("last_seen_at", now)
        r2.setdefault("change_count", 0)
        r2.setdefault("is_active", True)
        r2.setdefault("is_available", True)
        out.append(r2)
    return out

def upsert_listings_summary_precise(session: Session, rows: List[Dict[str, Any]], chunk_size: int = 500):
    """
    Precise upsert on PostgreSQL that returns exactly which rows were inserted, updated, or unchanged.
    Steps per chunk:
      1) INSERT .. ON CONFLICT DO NOTHING RETURNING listing_id  -> inserted_ids
      2) UPDATE (JOIN VALUES) WHERE table.source_hash <> values.source_hash  -> updated_ids
      3) UPDATE (JOIN VALUES) WHERE table.source_hash  = values.source_hash  -> unchanged_ids (touch last_seen_at)
    """
    data = _prep_rows(rows)
    if not data:
        return {"inserted_ids": [], "updated_ids": [], "unchanged_ids": []}

    tbl = ListingSummary.__table__
    inserted_ids_all: List[str] = []
    updated_ids_all:  List[str] = []
    unchanged_ids_all: List[str] = []

    # columns we maintain from the list-page
    colnames = [
        "listing_id", "make", "model", "title", "subtitle",
        "price_eur_num", "price_text", "price_label",
        "mileage_num", "mileage_text",
        "gearbox", "first_registration", "fuel_code", "fuel_text", "power_text",
        "zip_code", "seller_type", "location_text",
        "image_url", "detail_url",
        "source_hash",
    ]

    for i in range(0, len(data), chunk_size):
        chunk = data[i:i+chunk_size]

        # -------- 1) INSERT new (do nothing on conflict) and capture the inserted ids
        ins_stmt = (
            pg_insert(tbl)
            .values(chunk)
            .on_conflict_do_nothing(index_elements=[tbl.c.listing_id])
            .returning(tbl.c.listing_id)
        )
        inserted_ids = [row[0] for row in session.execute(ins_stmt).all()]
        inserted_set = set(inserted_ids)
        inserted_ids_all.extend(inserted_ids)

        # Build a VALUES virtual table for the whole chunk (ids + new values + hash)
        # This lets us do set-based UPDATEs with joins, efficiently.
        values_rows = [
            tuple(r.get(k) for k in colnames)
            for r in chunk
            if r["listing_id"] not in inserted_set  # only for existing rows
        ]
        if not values_rows:
            continue

        values_cols = [sa.column(c) for c in colnames]
        v = sa.values(*values_cols, name="v").data(values_rows)

        # --- generic diffs for any column (parametric) ---
        DIFF_FIELDS = [c for c in colnames if c not in ("listing_id", "source_hash")]

        select_cols = [tbl.c.listing_id]
        for c in DIFF_FIELDS:
            select_cols.append(getattr(tbl.c, c).label(f"old_{c}"))
            select_cols.append(getattr(v.c, c).label(f"new_{c}"))

        sel_diffs = (
            sa.select(*select_cols)
            .where(tbl.c.listing_id == v.c.listing_id)
            .where(tbl.c.source_hash != v.c.source_hash)
        )

        for r in session.execute(sel_diffs):
            lid = r.listing_id
            for c in DIFF_FIELDS:
                old = getattr(r, f"old_{c}")
                new = getattr(r, f"new_{c}")

                # Skip position updates (not relevant)
                if c == "position":
                    continue

                # Skip regressions where image_url disappears
                if c == "image_url" and old is not None and new is None:
                    continue

                if old != new:
                    print(f"[DIFF] {lid} {c}: {old} -> {new}")

        # -------- 2) UPDATE changed rows (hash differs): set new values, bump change_count, touch last_seen
        upd_changed = (
            sa.update(tbl)
            .where(tbl.c.listing_id == v.c.listing_id)
            .where(tbl.c.source_hash != v.c.source_hash)
            .values({
                "make": v.c.make,
                "model": v.c.model,
                "title": v.c.title,
                "subtitle": v.c.subtitle,
                "price_eur_num": v.c.price_eur_num,
                "price_text": v.c.price_text,
                "price_label": v.c.price_label,
                "mileage_num": v.c.mileage_num,
                "mileage_text": v.c.mileage_text,
                "gearbox": v.c.gearbox,
                "first_registration": v.c.first_registration,
                "fuel_code": v.c.fuel_code,
                "fuel_text": v.c.fuel_text,
                "power_text": v.c.power_text,
                "zip_code": v.c.zip_code,
                "seller_type": v.c.seller_type,
                "location_text": v.c.location_text,
                "image_url": sa.case(
                    (v.c.image_url.isnot(None), v.c.image_url),  # if new is not None → update
                    else_=tbl.c.image_url                               # else → keep old
                ),
                "detail_url": v.c.detail_url,
                "source_hash": v.c.source_hash,
                "last_seen_at": func.now(),
                "change_count": tbl.c.change_count + 1,
                "is_active": True,
            })
            .returning(tbl.c.listing_id)
        )
        updated_ids = [row[0] for row in session.execute(upd_changed).all()]
        updated_set = set(updated_ids)
        updated_ids_all.extend(updated_ids)

        # -------- 3) UPDATE unchanged rows (hash equal): only touch last_seen_at + is_active
        upd_unchanged = (
            sa.update(tbl)
            .where(tbl.c.listing_id == v.c.listing_id)
            .where(tbl.c.source_hash == v.c.source_hash)
            .values({
                "last_seen_at": func.now(),
                "is_active": True,
            })
            .returning(tbl.c.listing_id)
        )
        unchanged_ids = [row[0] for row in session.execute(upd_unchanged).all()]
        unchanged_ids_all.extend(unchanged_ids)

    session.commit()
    return {
        "inserted_ids": inserted_ids_all,
        "updated_ids": updated_ids_all,
        "unchanged_ids": unchanged_ids_all,
    }
