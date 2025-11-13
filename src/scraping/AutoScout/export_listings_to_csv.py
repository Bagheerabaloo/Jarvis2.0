"""
Esporta in CSV con colonne pulite e ordinate:
- Filtra Prezzo <= 9000 e Alimentazione in (Benzina, GPL)
- Ordina per Prima Volta Visto (decrescente)
- Converte KM e Cilindrata in valori numerici interi
"""

from pathlib import Path
import csv
import re
import sqlalchemy as sa
from datetime import datetime, timezone

from src.scraping.AutoScout.db.database import session_local
from src.scraping.AutoScout.db.models import ListingSummary, ListingDetail, ListingDistance


def _parse_number(text: str) -> int | None:
    """Estrae solo le cifre da una stringa, restituendo un int o None."""
    if not text:
        return None
    clean = re.sub(r"[^0-9]", "", str(text))
    return int(clean) if clean else None


def export_clean_csv(output_file: str = "autoscout_export_clean.csv") -> None:
    out_path = Path(output_file)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with session_local() as session:
        # Query con join, filtri e ordinamento
        stmt = (
            sa.select(ListingSummary, ListingDetail, ListingDistance)
            .join(ListingDetail, ListingDetail.listing_id == ListingSummary.listing_id, isouter=True)
            .join(ListingDistance, ListingDistance.listing_id == ListingSummary.listing_id, isouter=True)
            .where(
                ListingSummary.price_eur_num.isnot(None),
                ListingSummary.price_eur_num <= 9000,
                ListingSummary.fuel_text.in_(["Benzina", "GPL"]),
            )
            .order_by(ListingSummary.first_seen_at.desc())
        )
        rows = session.execute(stmt).all()

        # Mappa colonne (niente "Valutazione Prezzo Dettaglio", niente "Cambio")
        COL_MAP = {
            # Summary
            "Venditore": ("summary", "seller_type"),
            "Make": ("summary", "make"),
            "Model": ("summary", "model"),
            "Titolo": ("summary", "title"),
            "Prezzo": ("summary", "price_eur_num"),
            "Valutazione Prezzo": ("summary", "price_label"),
            "KM": ("summary", "mileage_text"),
            "Primo Immatricolazione": ("summary", "first_registration"),
            "Score Light": ("detail", "kpi_light"),
            "Disponibile": ("summary", "is_available"),
            "Giorni Disponibile": ("computed", "days_out"),
            "Alimentazione": ("summary", "fuel_text"),
            # Detail / Distance
            "Ubicazione Dettaglio": ("detail", "location_text"),
            "Distanza Km Area": ("distance", "air_distance_km"),
            "Classe Emissioni": ("detail", "env_emission_class"),
            "Potenza": ("detail", "power_text"),
            "Cilindrata": ("detail", "tech_displacement"),
            "Consumo": ("detail", "env_consumption"),
            "CAP": ("summary", "zip_code"),
            "Versione Modello": ("detail", "model_version"),
            "Carrozzeria": ("detail", "basic_body"),
            "Posti": ("detail", "basic_seats"),
            "Porte": ("detail", "basic_doors"),
            "Telefono": ("detail", "seller_phone"),
            "Note Venditore": ("detail", "seller_notes"),
            "Prima Volta Visto": ("summary", "first_seen_at"),
            "Ultima Volta Visto": ("summary", "last_seen_at"),
            "Link Dettaglio": ("summary", "detail_url"),
            "Sottotitolo": ("summary", "subtitle"),
            "Equip Comfort": ("detail", "equip_comfort_json"),
            "Equip Media": ("detail", "equip_media_json"),
            "Equip Sicurezza": ("detail", "equip_safety_json"),
            "Equip Extra": ("detail", "equip_extra_json"),
            "Non Disponibile Da": ("summary", "unavailable_at"),
        }

        fieldnames = list(COL_MAP.keys())

        with out_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()

            for summary, detail, distance in rows:
                # compute "days_out" only for unavailable listings
                days_out = None
                fs = summary.first_seen_at
                if fs.tzinfo is None:
                    fs = fs.replace(tzinfo=timezone.utc)

                if getattr(summary, "is_available", None) is False and getattr(summary, "unavailable_at", None):
                    ua = summary.unavailable_at
                    # normalize to aware datetime to avoid tz errors
                    if ua.tzinfo is None:
                        ua = ua.replace(tzinfo=timezone.utc)
                    days_out = (ua - fs).total_seconds() / 86400
                elif getattr(summary, "is_available", None):
                    now = datetime.now(timezone.utc)
                    days_out = (now - fs).total_seconds() / 86400

                rec = {}
                for col_csv, (tbl, attr) in COL_MAP.items():
                    val = None
                    if tbl == "computed" and attr == "days_out":
                        val = days_out
                    elif tbl == "summary":
                        val = getattr(summary, attr, None)
                        if col_csv == "KM":
                            val = _parse_number(val)  # "89.000 km" -> 89000
                        if col_csv == "Disponibile":
                            if val is True:
                                val = "Yes"
                            elif val is False:
                                val = "No"
                    elif tbl == "detail":
                        val = getattr(detail, attr, None) if detail else None
                        if col_csv == "Cilindrata":
                            val = _parse_number(val)  # es. "1.398 cm³" → 1398
                    elif tbl == "distance":
                        val = getattr(distance, attr, None) if distance else None
                    rec[col_csv] = val
                writer.writerow(rec)

    print(f"✅ Esportati {len(rows)} record in {out_path.resolve()}")


if __name__ == "__main__":
    export_clean_csv("autoscout_export_clean.csv")
