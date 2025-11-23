import datetime
import io
import requests
import asyncio
import os
import json

from collections import defaultdict
from datetime import datetime, timedelta
import pytz
import sqlalchemy as sa


from src.common.tools.library import *

from src.scraping.AutoScout.db.models import ListingSummary, ListingDetail, ListingDistance
from src.scraping.AutoScout.validators_autoscout import filter_listings_for_request

from src.common.telegram_manager.telegram_manager import TelegramBot
from src.common.file_manager.FileManager import FileManager
from typing import Tuple, List, Dict, Optional


def set_up_telegram_bot(keys, is_raspberry) -> Tuple[List[dict], TelegramBot]:
    telegram_token_key = "TELEGRAM_TOKEN"

    if is_raspberry:
        # --- Raspberry: tutto da variabili d'ambiente ---
        token = os.environ.get(telegram_token_key)
        if not token:
            raise RuntimeError(f"TELEGRAM_TOKEN non trovato nelle variabili d'ambiente.")

        admins: List[dict] = []
        for env_key in keys:
            raw = os.environ.get(env_key)
            if not raw:
                raise RuntimeError(f"Variabile d'ambiente {env_key} non trovata per admin Telegram.")
            try:
                admin = json.loads(raw)
            except json.JSONDecodeError as e:
                raise RuntimeError(f"Valore non valido in {env_key}: deve essere JSON. Errore: {e}")
            admins.append(admin)
    else:
        # --- PC: usa FileManager come prima ---
        config_manager = FileManager()
        token = config_manager.get_telegram_token(database_key=telegram_token_key)
        admins = [config_manager.get_admin(database_key=x) for x in keys]

    print(f"Telegram bot set with users: {[a.get('name') for a in admins]}")
    print(token)
    telegram_bot = TelegramBot(token=token)
    return admins, telegram_bot


def build_listing_text(ls: ListingSummary, air_km: float | None = None,
                       price_label: str | None = None,
                       tech_displacement: str | None = None,
                       env_consumption: str | None = None,
                       env_emission_class: str | None = None,
                       seller_phone: str | None = None,
                       kpi_light: str | None = None) -> str:
    """Return a Markdown caption for a listing."""

    def _fmt_eur(n: int | None) -> str:
        if n is None:
            return "n.d."
        return f"{n:,.0f}".replace(",", ".")  # 12.345

    def _compute_age(first_registration: str) -> str:
        """
        Convert a 'MM-YYYY' or 'YYYY' string to a textual age like '5 anni e 3 mesi'.
        Returns '' if not computable.
        """
        if not first_registration:
            return ""
        try:
            # AutoScout tipicamente ha formato 'MM/YYYY' oppure 'MM-YYYY' oppure solo 'YYYY'
            parts = first_registration.replace('/', '-').split('-')
            if len(parts) == 2:
                month = int(parts[0])
                year = int(parts[1])
            elif len(parts) == 1:
                month = 6  # se non c'è il mese, assumiamo giugno per un'approssimazione
                year = int(parts[0])
            else:
                return ""

            now = datetime.now()
            years = now.year - year
            months = now.month - month
            if months < 0:
                years -= 1
                months += 12
            return f"{years} anni e {months} mesi"
        except Exception:
            return ""

    title = ls.title or f"{(ls.make or '').title()} {(ls.model or '').title()}".strip()
    subtitle = f"_{ls.subtitle}_" if ls.subtitle else ""
    price = _fmt_eur(ls.price_eur_num)
    air_km = f"{air_km:.1f}" if air_km is not None else "-"
    age_text = _compute_age(ls.first_registration)

    location = ls.location_text or (ls.zip_code or "n.d.")
    link = ls.detail_url or f"https://www.autoscout24.it/annunci/{ls.listing_id}"

    lines = [
        # "🚗 *NUOVO ANNUNCIO*",
        # f"*{title}*",
        # subtitle,
        f"*{title}*\n",
        f"🚗 {(ls.make or '').title()} {(ls.model or '').title()}".strip(),
        f"👤 {ls.seller_type or 'n.d.'}",
        f"📍 {location}",
        f"📏 Distanza (aria): {air_km} km",
        f"📆 Anno: {ls.first_registration or 'n.d.'} • *{age_text}*",
        f"🛣️ Km: *{ls.mileage_text or 'n.d.'}*",
        # f"Carburante: {ls.fuel_text or 'n.d.'} • Cambio: {ls.gearbox or 'n.d.'}",
        f"⚙️ Cilindrata: {tech_displacement}",
        f"⛽ Carburante: {ls.fuel_text or 'n.d.'}",
        f"💧 Consumo: {env_consumption}",
        f"🌿 Classe Emissioni: {env_emission_class}",
        f"💶 Prezzo: *{price} €* • {price_label}",
        f"🏆 Score: {kpi_light}" if kpi_light is not None else None,
        (f"📞 {seller_phone}" if seller_phone is not None else None)
        # link,
    ]
    return "\n".join([l for l in lines if l])


def rome_day_window(day: datetime | None = None):
    """Return (start, end) of the 'Rome day' defined as 03:00→03:00 local time.

    - Input `day` is interpreted in Europe/Rome:
      * If None: use current time in Rome.
      * If naive: treat it as Europe/Rome local.
      * If timezone-aware: convert to Europe/Rome.
    - Output is UTC *naive* datetimes suitable for DB timestamps assumed saved in UTC naive.
    - Handles DST transitions correctly by constructing 03:00 local on each date, then localizing.
    """
    tz = pytz.timezone("Europe/Rome")

    # Determine the reference "now" in Europe/Rome
    if day is None:
        now_rome = datetime.now(tz)
    else:
        if day.tzinfo is None:
            # Treat naive input as local Rome time
            now_rome = tz.localize(day, is_dst=None)
        else:
            # Convert any aware datetime to Rome time
            now_rome = day.astimezone(tz)

    # If time is before 03:00, consider it part of the previous 'Rome day'
    anchor_date = now_rome.date()
    if now_rome.hour < 3:
        anchor_date = (now_rome - timedelta(days=1)).date()

    # Build 03:00 local for start and the next day's 03:00 for end, then localize
    start_naive_local = datetime(anchor_date.year, anchor_date.month, anchor_date.day, 3, 0, 0)
    end_naive_local = start_naive_local + timedelta(days=1)

    # Localize with is_dst=None to be strict about DST rules
    start_rome = tz.localize(start_naive_local, is_dst=None)
    end_rome = tz.localize(end_naive_local, is_dst=None)

    # Convert to UTC and drop tzinfo to return naive UTC datetimes
    start_utc = start_rome.astimezone(pytz.UTC).replace(tzinfo=None)
    end_utc = end_rome.astimezone(pytz.UTC).replace(tzinfo=None)
    return start_utc, end_utc

def format_eur(val: float | None) -> str:
    return f"{int(val):,} €".replace(",", ".") if val is not None else "n.d."

def age_years_from_first_reg(first_reg: str | None, today: datetime | None = None) -> str | None:
    """
    Convert 'MM/YYYY' or 'YYYY' into a string like '6.7 anni'.
    Returns None if not computable.
    """
    if not first_reg:
        return None

    first_reg = first_reg.strip()
    year = month = None

    try:
        if "/" in first_reg:
            mm, yyyy = first_reg.split("/", 1)
            month = int(mm)
            year = int(yyyy)
        elif "-" in first_reg:
            mm, yyyy = first_reg.split("-", 1)
            month = int(mm)
            year = int(yyyy)
        else:
            year = int(first_reg)
            month = 1
    except Exception:
        return None

    if not (1900 <= year <= datetime.utcnow().year + 1):
        return None
    if not (1 <= month <= 12):
        month = 1

    ref = today or datetime.utcnow()
    start = datetime(year, month, 1)
    delta_days = (ref - start).days
    if delta_days < 0:
        return None

    # Convert days to years with one decimal precision
    years = round(delta_days / 365.25, 1)
    return f"{years:.1f} anni"

def build_daily_summary_text(new_rows: list, withdrawn_rows: list) -> str:
    """
    new_rows: List[ListingSummary] dei NUOVI del giorno, filtrati
    withdrawn_rows: List[ListingSummary] diventati 'unavailable' oggi
    """
    # Group new listings by make -> model
    groups = defaultdict(lambda: defaultdict(list))
    for s in new_rows:
        groups[s.make or "n.d."][s.model or "n.d."].append(s)

    lines = []
    lines.append("*📬 Riepilogo AutoScout24 (oggi)*")

    # --- New listings ---
    total_new = len(new_rows)
    lines.append(f"\n*🆕 Nuovi annunci*: *{total_new}*\n")
    if total_new == 0:
        lines.append("_Nessun nuovo annuncio oggi._")
    else:
        # Sort for readability: by make, model, then price (None last)
        def sort_key(s):
            return (
                s.price_eur_num is None,
                s.price_eur_num or 0,
                (s.make or "n.d."),
                (s.model or "n.d."),
            )

        for s in sorted(new_rows, key=sort_key):
            # Build a safe title fallback
            title = s.title or f"{(s.make or '').strip()} {(s.model or '').strip()}".strip() or "Titolo n.d."
            price = format_eur(s.price_eur_num)
            mileage = s.mileage_text or "km n.d."
            age_txt = age_years_from_first_reg(s.first_registration)
            year_and_age = f"{age_txt}" if age_txt else ""

            # One flat line per listing (no grouping)
            lines.append(f"— *{title}*\n      ↳ _{price} | {mileage} | {year_and_age}_\n")

    # --- Withdrawn ---
    total_gone = len(withdrawn_rows)
    lines.append(f"\n*🗑️ Annunci ritirati*: *{total_gone}*\n")
    # if total_gone == 0:
    #     lines.append("_Nessun ritiro oggi._")
    # else:
    #     # Sort for readability: by make, model, then price (None last)
    #     def sort_key(s):
    #         return (
    #             s.price_eur_num is None,
    #             s.price_eur_num or 0,
    #             (s.make or "n.d."),
    #             (s.model or "n.d."),
    #         )
    #
    #     # list max 10
    #     for s in sorted(withdrawn_rows, key=sort_key)[:5]:
    #         # Build a safe title fallback
    #         title = s.title or f"{(s.make or '').strip()} {(s.model or '').strip()}".strip() or "Titolo n.d."
    #         price = format_eur(s.price_eur_num)
    #         mileage = s.mileage_text or "km n.d."
    #         age_txt = age_years_from_first_reg(s.first_registration)
    #         year_and_age = f"{age_txt}" if age_txt else ""
    #
    #         # One flat line per listing (no grouping)
    #         lines.append(f"— *{title}*\n      ↳ _{price} | {mileage} | {year_and_age}_\n")
    #
    #     if total_gone > 5:
    #         lines.append(f"… e altri {total_gone-5}")

    return "\n".join(lines)


async def _send_batch(valid_rows, telegram_bot, admin_info, dist_by_id, detailed_rows):
    # Send sequentially (or with internal Semaphore) within ONE event loop
    for ls in valid_rows:
        air_km = (dist_by_id or {}).get(ls.listing_id)
        det = (detailed_rows or {}).get(ls.listing_id, {})
        caption = build_listing_text(
            ls,
            air_km=air_km,
            price_label=det.get("price_label"),
            tech_displacement=det.get("tech_displacement"),
            env_consumption=det.get("env_consumption"),
            env_emission_class=det.get("env_emission_class"),
            seller_phone=det.get("seller_phone"),
            kpi_light=det.get("kpi_light"),
        )
        link = ls.detail_url or f"https://www.autoscout24.it/annunci/{ls.listing_id}"
        keyboard = {'text': 'Apri su AutoScout24', 'url': link}

        for info in admin_info:
            sent = False
            if ls.image_url:
                try:
                    resp = requests.get(ls.image_url, timeout=10)
                    resp.raise_for_status()
                    buf = io.BytesIO(resp.content)
                    buf.seek(0)
                    # Use the send_photo that is semaphore+retry protected
                    await telegram_bot.send_photo(
                        chat_id=info["chat"],
                        photo=buf,
                        caption=caption,
                        inline_keyboard=keyboard,
                        parse_mode="Markdown",
                    )
                    sent = True
                except Exception as e:
                    print(f"Failed to send photo for {ls.listing_id}: {e}")

            if not sent:
                await telegram_bot.send_message(
                    chat_id=info["chat"],
                    text=caption,
                    inline_keyboard=keyboard,
                    parse_mode="Markdown",
                )


async def notify_inserted_listings_via_telegram(valid_rows, telegram_bot, admin_info: list[dict],
                                          dist_by_id: dict[str, float] | None = None,
                                          detailed_rows: dict[str, dict] | None = None):
    if len(valid_rows) == 0:
        return
    elif len(valid_rows) > 50:
        print(f"Too many new listings ({len(valid_rows)}), skipping Telegram notification.")
        chat_id = next((info["chat"] for info in admin_info if info.get("is_admin")), None)
        # Single run for the tiny batch
        await telegram_bot.send_message(chat_id=chat_id, text=f"", parse_mode="Markdown")
        return

    # Single run for the whole batch (no inner runs)
    await _send_batch(valid_rows, telegram_bot, admin_info, dist_by_id, detailed_rows)



