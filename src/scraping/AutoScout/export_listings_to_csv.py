"""
Esporta in CSV con nomi colonne puliti:
Summary, Detail e Distance selezionati.
"""

from pathlib import Path
import csv
import sqlalchemy as sa

from src.scraping.AutoScout.db.database import session_local
from src.scraping.AutoScout.db.models import ListingSummary, ListingDetail, ListingDistance


def export_clean_csv(output_file: str = "autoscout_export_clean.csv") -> None:
    out_path = Path(output_file)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with session_local() as session:
        stmt = (
            sa.select(ListingSummary, ListingDetail, ListingDistance)
            .join(ListingDetail, ListingDetail.listing_id == ListingSummary.listing_id, isouter=True)
            .join(ListingDistance, ListingDistance.listing_id == ListingSummary.listing_id, isouter=True)
        )

        rows = session.execute(stmt).all()

        # Mappa dei campi richiesti: chiave = nome CSV, valore = (tabella, nome attributo)
        COL_MAP = {
            # Summary
            "Make": ("summary", "make"),
            "Model": ("summary", "model"),
            "Titolo": ("summary", "title"),
            "Prezzo_EUR": ("summary", "price_eur_num"),
            "Valutazione_Prezzo": ("summary", "price_label"),
            "Km_Riepilogo": ("summary", "mileage_text"),
            "Cambio": ("summary", "gearbox"),
            "Primo_Immatricolazione": ("summary", "first_registration"),
            "Alimentazione": ("summary", "fuel_text"),
            "Sottotitolo": ("summary", "subtitle"),
            "CAP": ("summary", "zip_code"),
            "Venditore": ("summary", "seller_type"),
            "Ubicazione": ("summary", "location_text"),
            "Link_Dettaglio": ("summary", "detail_url"),
            "Prima_Volta_Visto": ("summary", "first_seen_at"),
            "Ultima_Volta_Visto": ("summary", "last_seen_at"),
            # Detail
            "Valutazione_Prezzo_Dettaglio": ("detail", "price_label"),
            "Classe_Emissioni": ("detail", "env_emission_class"),
            "Potenza": ("detail", "power_text"),
            "Cilindrata": ("detail", "tech_displacement"),
            "Consumo": ("detail", "env_consumption"),
            "Versione_Modello": ("detail", "model_version"),
            "Ubicazione_Dettaglio": ("detail", "location_text"),
            "Anno_Dettaglio": ("detail", "overview_year"),
            "Telefono": ("detail", "seller_phone"),
            "Carrozzeria": ("detail", "basic_body"),
            "Posti": ("detail", "basic_seats"),
            "Porte": ("detail", "basic_doors"),
            "Equip_Comfort": ("detail", "equip_comfort_json"),
            "Equip_Media": ("detail", "equip_media_json"),
            "Equip_Sicurezza": ("detail", "equip_safety_json"),
            "Equip_Extra": ("detail", "equip_extra_json"),
            "Note_Venditore": ("detail", "seller_notes"),
            # Distance
            "Lat_Destinazione": ("distance", "dest_lat"),
            "Lon_Destinazione": ("distance", "dest_lon"),
            "Distanza_Km_Area": ("distance", "air_distance_km"),
        }

        fieldnames = list(COL_MAP.keys())

        with out_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()

            for summary, detail, distance in rows:
                rec = {}
                for col_csv, (tbl, attr) in COL_MAP.items():
                    if tbl == "summary":
                        rec[col_csv] = getattr(summary, attr, None)
                    elif tbl == "detail":
                        rec[col_csv] = getattr(detail, attr, None) if detail else None
                    elif tbl == "distance":
                        rec[col_csv] = getattr(distance, attr, None) if distance else None
                writer.writerow(rec)

    print(f"✅ Esportati {len(rows)} record in {out_path.resolve()}")


if __name__ == "__main__":
    export_clean_csv("autoscout_export_clean.csv")
