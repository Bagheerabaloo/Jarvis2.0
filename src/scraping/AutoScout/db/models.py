# db.py
from __future__ import annotations
from datetime import datetime
from typing import Optional
from sqlalchemy import Column, Integer, String, Text, Boolean, Numeric, ForeignKey, Index, UniqueConstraint
from sqlalchemy import Float, BigInteger, Date, DateTime, UUID as SA_UUID, text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.scraping.AutoScout.db.database import Base


class ListingSummary(Base):
    """Summary info captured from the list page."""
    __tablename__ = "listing_summary"

    listing_id: Mapped[str] = mapped_column(String(64), primary_key=True)  # article/@id
    position:   Mapped[Optional[int]] = mapped_column(Integer)

    make:       Mapped[Optional[str]] = mapped_column(String(64))
    model:      Mapped[Optional[str]] = mapped_column(String(128))
    title:      Mapped[Optional[str]] = mapped_column(String(512))
    subtitle:   Mapped[Optional[str]] = mapped_column(String(1024))

    price_eur_num: Mapped[Optional[int]] = mapped_column(Integer)
    price_text:    Mapped[Optional[str]] = mapped_column(String(64))
    price_label:   Mapped[Optional[str]] = mapped_column(String(64))

    mileage_num: Mapped[Optional[int]] = mapped_column(Integer)
    mileage_text: Mapped[Optional[str]] = mapped_column(String(64))

    gearbox:    Mapped[Optional[str]] = mapped_column(String(64))
    first_registration: Mapped[Optional[str]] = mapped_column(String(16))
    fuel_code:  Mapped[Optional[str]] = mapped_column(String(8))
    fuel_text:  Mapped[Optional[str]] = mapped_column(String(64))
    power_text: Mapped[Optional[str]] = mapped_column(String(64))

    zip_code:   Mapped[Optional[str]] = mapped_column(String(16))
    seller_type: Mapped[Optional[str]] = mapped_column(String(16))  # 'Privato'/'Rivenditore'
    location_text: Mapped[Optional[str]] = mapped_column(String(128))

    image_url:  Mapped[Optional[str]] = mapped_column(Text)
    detail_url: Mapped[Optional[str]] = mapped_column(Text)

    # bookkeeping
    source_hash:  Mapped[str] = mapped_column(String(64), index=True)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    last_seen_at:  Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    change_count:  Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    is_active:     Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)

    # detail: Mapped["ListingDetail"] = relationship(back_populates="summary", uselist=False, cascade="all, delete-orphan")

    __table_args__ = (
        Index("idx_summary_make_model", "make", "model"),
        Index("idx_summary_zip", "zip_code"),
        Index("idx_summary_seller_type", "seller_type"),
    )

# REPLACE ListingDetail with this richer schema
class ListingDetail(Base):
    __tablename__ = "listing_detail"

    listing_id: Mapped[str] = mapped_column(
        String(64), ForeignKey("listing_summary.listing_id", ondelete="CASCADE"), primary_key=True
    )

    # Stage (già dalla prima parte)
    title:          Mapped[Optional[str]] = mapped_column(String(512))
    model_version:  Mapped[Optional[str]] = mapped_column(String(512))
    price_text:     Mapped[Optional[str]] = mapped_column(String(64))
    price_label:    Mapped[Optional[str]] = mapped_column(String(64))
    location_text:  Mapped[Optional[str]] = mapped_column(String(128))
    maps_href:      Mapped[Optional[str]] = mapped_column(Text)
    seller_type:    Mapped[Optional[str]] = mapped_column(String(32))
    main_image_url: Mapped[Optional[str]] = mapped_column(Text)


    # Overview (riquadri in alto)
    overview_mileage: Mapped[Optional[str]] = mapped_column(String(64))
    overview_gearbox: Mapped[Optional[str]] = mapped_column(String(64))
    overview_year:    Mapped[Optional[str]] = mapped_column(String(32))
    overview_fuel:    Mapped[Optional[str]] = mapped_column(String(64))
    overview_power:   Mapped[Optional[str]] = mapped_column(String(64))

    seller_phone: Mapped[Optional[str]] = mapped_column(String(32))

    # Carfax / partner
    carfax_url:     Mapped[Optional[str]] = mapped_column(Text)

    # Finanziamento (tabella esempio) – testo per semplicità
    fin_auto_price:       Mapped[Optional[str]] = mapped_column(String(64))
    fin_down_payment:     Mapped[Optional[str]] = mapped_column(String(64))
    fin_duration:         Mapped[Optional[str]] = mapped_column(String(64))
    fin_amount:           Mapped[Optional[str]] = mapped_column(String(64))
    fin_total_due:        Mapped[Optional[str]] = mapped_column(String(64))
    fin_taeg:             Mapped[Optional[str]] = mapped_column(String(64))
    fin_tan:              Mapped[Optional[str]] = mapped_column(String(64))
    fin_installment:      Mapped[Optional[str]] = mapped_column(String(64))

    # Dati di base
    basic_body:           Mapped[Optional[str]] = mapped_column(String(64))
    basic_vehicle_type:   Mapped[Optional[str]] = mapped_column(String(64))
    basic_seats:          Mapped[Optional[str]] = mapped_column(String(32))
    basic_doors:          Mapped[Optional[str]] = mapped_column(String(32))
    basic_neopatentati:   Mapped[Optional[str]] = mapped_column(String(32))

    # Cronologia veicolo
    hist_mileage:         Mapped[Optional[str]] = mapped_column(String(64))
    hist_year:            Mapped[Optional[str]] = mapped_column(String(32))
    hist_last_service:    Mapped[Optional[str]] = mapped_column(String(32))
    hist_owners:          Mapped[Optional[str]] = mapped_column(String(32))
    hist_service_book:    Mapped[Optional[str]] = mapped_column(String(32))
    hist_non_smoker:      Mapped[Optional[str]] = mapped_column(String(32))

    # Dati tecnici
    tech_power:           Mapped[Optional[str]] = mapped_column(String(64))
    tech_gearbox:         Mapped[Optional[str]] = mapped_column(String(64))
    tech_displacement:    Mapped[Optional[str]] = mapped_column(String(64))
    tech_cylinders:       Mapped[Optional[str]] = mapped_column(String(64))
    tech_weight:          Mapped[Optional[str]] = mapped_column(String(64))

    # Ambiente
    env_emission_class:   Mapped[Optional[str]] = mapped_column(String(64))
    env_fuel:             Mapped[Optional[str]] = mapped_column(String(64))
    env_consumption:      Mapped[Optional[str]] = mapped_column(String(64))

    # Equipaggiamenti (JSON per categoria)
    equip_comfort_json:   Mapped[Optional[str]] = mapped_column(Text)
    equip_media_json:     Mapped[Optional[str]] = mapped_column(Text)
    equip_safety_json:    Mapped[Optional[str]] = mapped_column(Text)
    equip_extra_json:     Mapped[Optional[str]] = mapped_column(Text)

    # Descrizione venditore
    seller_notes:         Mapped[Optional[str]] = mapped_column(Text)
    seller_email:         Mapped[Optional[str]] = mapped_column(String(256))

    # bookkeeping
    source_hash:          Mapped[Optional[str]] = mapped_column(String(64))
    last_scraped_at:      Mapped[Optional[datetime]] = mapped_column(DateTime)

    __table_args__ = (Index("idx_detail_seller_type", "seller_type"),)

class GeoCache(Base):
    __tablename__ = "geo_cache"

    # Store the exact text we geocoded, normalized by our code
    query = Column(String, primary_key=True)
    lat = Column(Float, nullable=False)
    lon = Column(Float, nullable=False)
    source = Column(String, nullable=False, default="osm")  # 'osm' for Nominatim
    quality = Column(String, nullable=True)  # 'address' | 'zip' | 'city' | None
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, server_default=text("CURRENT_TIMESTAMP"))

class ListingDistance(Base):
    __tablename__ = "listing_distance"

    # one row per listing (latest distance snapshot)
    listing_id = Column(String, ForeignKey("listing_summary.listing_id"), primary_key=True)
    dest_query = Column(String, nullable=False)          # the string we geocoded for destination
    dest_lat = Column(Float, nullable=True)              # may be null if geocoding failed
    dest_lon = Column(Float, nullable=True)
    air_distance_km = Column(Float, nullable=True)       # haversine distance km
    geocode_source = Column(String, nullable=True)       # 'osm'
    geocode_quality = Column(String, nullable=True)      # 'address'|'zip'|'city'|None
    computed_at = Column(DateTime, nullable=False, default=datetime.utcnow, server_default=text("CURRENT_TIMESTAMP"))
