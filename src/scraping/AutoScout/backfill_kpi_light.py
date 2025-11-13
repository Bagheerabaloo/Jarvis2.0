"""
Aggiorna kpi_light per tutti i ListingDetail dove è NULL.

Usage:
    python -m src.scraping.AutoScout.scripts.backfill_kpi_light
"""

from __future__ import annotations

import math
from typing import Dict, Any

import sqlalchemy as sa
from sqlalchemy.orm import Session

from src.scraping.AutoScout.db.database import session_local
from src.scraping.AutoScout.db.models import ListingDetail, ListingSummary
# riusa la funzione già definita nell'upsert (così la logica resta unica)
from src.scraping.AutoScout.upsert_details_pg import compute_kpi_light


BATCH_SIZE = 500


def _detail_as_dict(det: ListingDetail) -> Dict[str, Any]:
    """Minimo indispensabile per compute_kpi_light (puoi aggiungere altri campi se in futuro servono)."""
    return {
        "price_label": det.price_label,
        "overview_year": det.overview_year,
    }


def backfill_kpi_light(batch_size: int = BATCH_SIZE) -> None:
    total_updated = 0

    with session_local() as session:  # type: Session
        # conta quanti sono da backfill
        total_null = session.scalar(sa.select(sa.func.count()).select_from(
            sa.select(1).select_from(ListingDetail).where(ListingDetail.kpi_light.is_(None)).subquery()
        )) or 0

        if total_null == 0:
            print("✅ Nessun record da aggiornare: kpi_light già popolato ovunque.")
            return

        print(f"🧮 Da aggiornare: {total_null} ListingDetail con kpi_light NULL.")
        num_batches = math.ceil(total_null / batch_size)

        last_seen_id = None
        for b in range(num_batches):
            # prendi un batch ordinato per PK per evitare salti/duplicati
            stmt = (
                sa.select(ListingDetail, ListingSummary)
                .join(ListingSummary, ListingSummary.listing_id == ListingDetail.listing_id)
                .where(ListingDetail.kpi_light.is_(None))
                .order_by(ListingDetail.listing_id)
                .limit(batch_size)
            )
            if last_seen_id:
                stmt = stmt.where(ListingDetail.listing_id > last_seen_id)

            rows = session.execute(stmt).all()
            if not rows:
                break

            batch_updated = 0
            for det, summ in rows:
                # calcola KPI
                kpi = compute_kpi_light(summ, _detail_as_dict(det))
                det.kpi_light = kpi
                batch_updated += 1
                last_seen_id = det.listing_id  # avanza il cursore

            session.commit()
            total_updated += batch_updated
            print(f"✓ Batch {b+1}/{num_batches}: aggiornati {batch_updated} (totale {total_updated})")

    print(f"🎉 Completato. KPI Light popolato su {total_updated} record.")


if __name__ == "__main__":
    backfill_kpi_light()
