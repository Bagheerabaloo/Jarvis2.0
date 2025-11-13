# geo_utils.py
from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Iterable, Dict, Any, Optional, Tuple, List
from urllib.parse import urlparse, parse_qs, unquote_plus

from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from geopy.distance import geodesic
from sqlalchemy.orm import Session

from src.scraping.AutoScout.db.models import GeoCache, ListingDistance

LOGGER = logging.getLogger(__name__)

# ----------------------------
# Helpers for normalization
# ----------------------------

_IT_WORDS_STREET = ("via", "viale", "piazza", "corso", "largo", "vicolo", "strada", "p.za", "pzza")
_SPACES_RE = re.compile(r"\s+")


def _clean(s: str) -> str:
    """Collapse spaces, strip, remove weird unicode spaces."""
    s = (s or "").replace("\xa0", " ").strip()
    return _SPACES_RE.sub(" ", s)


def _normalize_zip(zip_code: Optional[str]) -> Optional[str]:
    """Normalize Italian CAP like 'IT-20147' -> '20147'."""
    if not zip_code:
        return None
    z = _clean(zip_code)
    z = re.sub(r"(?i)^IT[-\s]?","", z)        # remove 'IT-' prefix
    z = re.sub(r"[^\d]", "", z)               # keep digits only
    return z or None


def _guess_quality(query: str, had_zip: bool, had_street: bool) -> str:
    """Return a coarse quality flag for the geocode query."""
    if had_street:
        return "address"
    if had_zip:
        return "zip"
    return "city"


def _from_maps_href(maps_href: str) -> Optional[str]:
    """
    Extract 'q' from a Google Maps link (e.g., '...maps?q=20149+Milano%2C+IT').
    """
    try:
        q = parse_qs(urlparse(maps_href).query).get("q", [None])[0]
        return _clean(unquote_plus(q)) if q else None
    except Exception:
        return None


def build_geocode_query(location_text: Optional[str],
                        zip_code: Optional[str],
                        country_fallback: str = "IT",
                        maps_href: Optional[str] = None) -> Optional[Tuple[str, str]]:
    """
    Build a geocode query string and a quality hint ('address'|'zip'|'city').
    Priority:
      1) maps_href -> q=...
      2) zip_code + location_text
      3) location_text
      4) zip_code
    Returns: (query, quality_hint) or None
    """
    # 1) try maps href
    if maps_href:
        q = _from_maps_href(maps_href)
        if q:
            txt = q
            had_zip = bool(re.search(r"\b\d{4,6}\b", txt))
            had_street = any(w in txt.lower() for w in _IT_WORDS_STREET)
            qual = _guess_quality(txt, had_zip, had_street)
            return txt, qual

    # 2) CAP + città
    city = _clean(location_text) if location_text else None
    cap = _normalize_zip(zip_code)
    if cap and city:
        txt = f"{cap} {city}, {country_fallback}"
        return txt, _guess_quality(txt, had_zip=True, had_street=False)

    # 3) solo città
    if city:
        txt = f"{city}, {country_fallback}"
        return txt, _guess_quality(txt, had_zip=False, had_street=False)

    # 4) solo CAP
    if cap:
        txt = f"{cap}, {country_fallback}"
        return txt, _guess_quality(txt, had_zip=True, had_street=False)

    return None


# ----------------------------
# Geocoder + Cache
# ----------------------------

@dataclass
class GeoCfg:
    user_agent: str = "autoscout-distance-bot"
    min_delay_seconds: float = 1.2     # Nominatim fair use
    swallow_exceptions: bool = True
    timeout: int = 10               # per-request timeout in seconds
    error_wait_seconds: float = 2.5     # wait between retries on errors
    country_codes: str = "it"           # restrict to Italy to speed up and reduce ambiguity


class GeoService:
    """
    Thin wrapper around Nominatim with SQL-backed cache in GeoCache.
    """
    def __init__(self, cfg: GeoCfg | None = None):
        self.cfg = cfg or GeoCfg()
        self._geocoder = Nominatim(
            user_agent=self.cfg.user_agent,
            timeout=self.cfg.timeout
        )

        # Pre-bind country/fields to reduce work and latency
        def _geo_call(q: str):
            return self._geocoder.geocode(
                q,
                country_codes=self.cfg.country_codes,  # narrow search to Italy
                addressdetails=False,
                limit=1
            )

        # Respect fair use and back off on transient errors
        self._geocode_rl = RateLimiter(
            _geo_call,
            min_delay_seconds=self.cfg.min_delay_seconds,
            swallow_exceptions=self.cfg.swallow_exceptions,
            max_retries=3,
            error_wait_seconds=self.cfg.error_wait_seconds,
        )

    def geocode_with_cache(self, session: Session, query: str) -> Optional[Tuple[float, float]]:
        """
        Return (lat, lon) for 'query'. Use GeoCache table. Insert on miss.
        """
        q = _clean(query)
        if not q:
            return None

        # 1) Cache hit?
        cached = session.get(GeoCache, q)
        if cached:
            return cached.lat, cached.lon

        # 2) Call Nominatim
        try:
            loc = self._geocode_rl(q)
        except Exception as e:
            LOGGER.warning("Geocode error for '%s': %s", q, e)
            loc = None

        if not loc:
            return None

        lat, lon = float(loc.latitude), float(loc.longitude)

        # 3) Store in cache
        try:
            session.merge(GeoCache(query=q, lat=lat, lon=lon, source="osm", quality=None))
            session.commit()
        except Exception as e:
            session.rollback()
            LOGGER.warning("Failed to cache geocode '%s': %s", q, e)

        return lat, lon


# ----------------------------
# Distance (air)
# ----------------------------

def _row_get(d: Any, key: str, default=None):
    """
    Safe extractor that works with dicts and ORM objects.
    """
    if isinstance(d, dict):
        return d.get(key, default)
    return getattr(d, key, default)


def _listing_id(row: Any) -> Optional[str]:
    return _row_get(row, "listing_id")


def _build_dest_query_from_row(row: Any) -> Optional[Tuple[str, str]]:
    """
    Construct the geocode query from either detail or summary fields.
    Tries, in order:
      - maps_href (if present in 'row' dict)
      - summary: zip_code + location_text
      - detail : location_text (if you pass a detail row)
      - summary: zip_code
    Returns (query, quality_hint) or None.
    """
    maps_href = _row_get(row, "maps_href")
    loc_text = _row_get(row, "location_text")
    zip_code = _row_get(row, "zip_code")
    return build_geocode_query(loc_text, zip_code, country_fallback="IT", maps_href=maps_href)


def _compute_distance_km(a: Tuple[float, float], b: Tuple[float, float]) -> float:
    """
    Geodesic distance in km (air distance).
    """
    return float(geodesic(a, b).km)


def _upsert_listing_distance(session: Session,
                             listing_id: str,
                             dest_query: str,
                             dest_lat: float,
                             dest_lon: float,
                             air_km: float,
                             geocode_source: str = "osm",
                             geocode_quality: Optional[str] = None) -> None:
    """
    Simple ORM upsert into listing_distance.
    """
    obj = session.get(ListingDistance, listing_id)
    if obj is None:
        obj = ListingDistance(
            listing_id=listing_id,
            dest_query=dest_query,
            dest_lat=dest_lat,
            dest_lon=dest_lon,
            air_distance_km=air_km,
            geocode_source=geocode_source,
            geocode_quality=geocode_quality,
        )
        session.add(obj)
    else:
        obj.dest_query = dest_query
        obj.dest_lat = dest_lat
        obj.dest_lon = dest_lon
        obj.air_distance_km = air_km
        obj.geocode_source = geocode_source
        obj.geocode_quality = geocode_quality
    session.commit()


# NEW: prefer detail fields (maps_href, location_text) over summary
def _build_dest_query_from_row_with_detail(row: Any, detail: Optional[Dict[str, Any]]):
    # 1) if detail has a Google Maps href, extract "q" and use it
    maps_href = (detail or {}).get("maps_href") or _row_get(row, "maps_href")
    loc_text  = (detail or {}).get("location_text") or _row_get(row, "location_text")
    zip_code  = _row_get(row, "zip_code")

    if maps_href:
        q = _from_maps_href(maps_href)
        if q:
            had_zip = bool(re.search(r"\b\d{4,6}\b", q))
            had_street = any(w in q.lower() for w in _IT_WORDS_STREET)
            return q, _guess_quality(q, had_zip, had_street)

    # 2) fallback: detail/location_text + summary zip_code if present
    return build_geocode_query(loc_text, zip_code, country_fallback="IT", maps_href=None)


def compute_air_distance_for_rows(session: Session,
                                  rows: Iterable[Any],
                                  detailed_rows: Dict[str, Any] | None,
                                  base_address: str,
                                  force_recompute: bool = False) -> Dict[str, float]:
    """
    Compute air distance (km) for each listing in 'rows' relative to 'base_address'.
    - rows: iterable of dicts OR ORM objects that include at least:
        listing_id, location_text (optional), zip_code (optional), maps_href (optional)
    - base_address: e.g. "Via Primaticcio, Milano"
    - force_recompute: if False, skip rows that already exist in listing_distance

    Returns: {listing_id: distance_km} for those successfully computed.
    """
    service = GeoService()

    # 1) Geocode base once (with cache)
    base = service.geocode_with_cache(session, base_address)
    if not base:
        LOGGER.error("Failed to geocode base address '%s' — aborting distance computation.", base_address)
        return {}
    base_latlon = (base[0], base[1])

    # 2) Optionally fetch already computed IDs
    existing_ids: set[str] = set()
    if not force_recompute:
        q = session.query(ListingDistance.listing_id).all()
        existing_ids = {lid for (lid,) in q}

    results: Dict[str, float] = {}

    # 3) Iterate rows
    for row in rows:
        lid = _listing_id(row)
        if not lid:
            continue
        if not force_recompute and lid in existing_ids:
            # already computed; skip
            continue

        # q_and_quality = _build_dest_query_from_row(row)
        # if not q_and_quality:
        #     LOGGER.debug("No geocode query for %s (missing location).", lid)
        #     continue
        # dest_query, qual = q_and_quality

        detail = (detailed_rows or {}).get(lid)

        dest_q = _build_dest_query_from_row_with_detail(row, detail)
        if not dest_q:
            LOGGER.debug("No geocode query for %s (missing location).", lid)
            continue
        dest_query, qual = dest_q

        dest = service.geocode_with_cache(session, dest_query)
        if not dest:
            LOGGER.debug("Geocode failed for '%s' (lid=%s).", dest_query, lid)
            continue

        dest_latlon = (dest[0], dest[1])
        dist_km = _compute_distance_km(base_latlon, dest_latlon)

        try:
            _upsert_listing_distance(
                session=session,
                listing_id=lid,
                dest_query=dest_query,
                dest_lat=float(dest_latlon[0]),
                dest_lon=float(dest_latlon[1]),
                air_km=dist_km,
                geocode_source="osm",
                geocode_quality=qual,
            )
            results[lid] = dist_km
        except Exception as e:
            session.rollback()
            LOGGER.warning("Failed to upsert listing_distance for %s: %s", lid, e)

    return results
