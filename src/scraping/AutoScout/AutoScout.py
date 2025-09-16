import json
import requests
import io
import datetime
import re
import io
import requests

import asyncio
from math import ceil

from threading import Thread, Lock
from math import ceil
from enum import Enum
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, date
from time import time, sleep
from dataclasses import dataclass
from copy import deepcopy
from typing import Union
from typing import List, Dict, Any
from sqlalchemy import select

from src.common.tools.library import *
from src.common.web_driver.ChromeDriver import ChromeDriver
from src.common.web_driver.FirefoxDriver import FirefoxDriver

from src.scraping.AutoScout.upsert_precise_pg import upsert_listings_summary_precise
from src.scraping.AutoScout.upsert_details_pg import upsert_listing_detail
from src.scraping.AutoScout.geo_utils import compute_air_distance_for_rows
from src.scraping.AutoScout.db.database import session_local
from src.scraping.AutoScout.db.models import ListingSummary, ListingDetail, ListingDistance
from src.scraping.AutoScout.validators_autoscout import filter_listings_for_request

from src.common.telegram_manager.telegram_manager import TelegramBot
from src.common.file_manager.FileManager import FileManager
from typing import Tuple, List, Dict, Optional


def set_up_telegram_bot():
    telegram_token_key = "TELEGRAM_TOKEN"
    config_manager = FileManager()
    token = config_manager.get_telegram_token(database_key=telegram_token_key)
    admin_info_1 = config_manager.get_admin()
    admin_info_2 = config_manager.get_admin(database_key="TELEGRAM_MARINELLA")
    telegram_bot = TelegramBot(token=token)
    return [admin_info_1, admin_info_2], telegram_bot


MAX_PRICE = "7.000 €"
RADIUS = "100 km"

FILTER = True


class Browser(str, Enum):
    firefox = 'Firefox'
    chrome = 'Chrome'


class AutoScout:
    reject_cookies_class = ['_consent-settings_1lphq_103']
    lost_password_text = ['Hai perso la password?']

    def __init__(self, browser: Browser, headless: bool = False, **kwargs):
        self.driver = None
        self.headless = headless
        self.browser = browser
        self.lock = Lock()

    # __ initialization __
    def init_driver(self):
        # __ init webdriver __
        if self.browser == Browser.chrome:
            self.driver = ChromeDriver()
        elif self.browser == Browser.firefox:
            self.driver = FirefoxDriver(headless=self.headless, selenium_profile=True)
        else:
            self.driver = None

    # _____ closing ______
    def close(self):
        self.close_driver()

    def close_driver(self):
        self.lock.acquire()
        if self.driver.driver:
            self.driver.close_driver()
        self.lock.release()

    # __ main __
    def main(self):
        # __ set up web driver __
        self.init_driver()
        self.driver.init_driver()

        # __ set up telegram bot __
        admin_info, telegram_bot = set_up_telegram_bot()

        # __ Get Booking search results page __ #
        self._get_starting_page()
        print("navigator.webdriver =", self.driver.driver.execute_script("return navigator.webdriver"))
        print("navigator.hardwareConcurrency =", self.driver.driver.execute_script("return navigator.hardwareConcurrency"))
        print("navigator.deviceMemory =", self.driver.driver.execute_script("return navigator.deviceMemory"))

        sleep(5)
        # __ Reject cookies __
        safe_execute(None, self._reject_cookies)
        sleep(5)
        self.driver.click_link_by_class("hf-searchmask-form__detail-search")
        sleep(5)
        self.fill_requested_filters()
        sleep(8)

        btn = self.driver.wait_until_clickable_by_xpath("//button[contains(@class,'DetailSearchPage_button__')]")
        self.driver.scroll_into_view(btn, block="center")
        self.driver.click_element(btn)

        sleep(5)

        # rows = self.scrape_pages(2)
        rows = self.scrape_all_pages() # TODO: add ordering by latest

        # __ save to DB with upsert __
        with session_local() as session:
            result = upsert_listings_summary_precise(session, rows)

        rows = self.load_new_listings(result["inserted_ids"])
        if not rows:
            return

        # --- validate against your search filters ---
        if FILTER:
            valid_rows, rejected = filter_listings_for_request(
                rows,
                min_year=2015,
                max_price=int(MAX_PRICE.replace(".", "").replace("€", "").strip()),
                max_mileage=100_000,
                required_seller="Privato",
                allowed_fuels=["Elettrica/Benzina", "Benzina", "Metano", "GPL"],
                strict=True,  # if a field is missing, we reject
            )
        else:
            valid_rows = rows
            rejected = []

        # Log (or print) rejected items with reasons
        for r in rejected:
            print(f"[SKIP] {r['listing_id']}: {', '.join(r['reasons'])}  | {r.get('title', '')}")

        detailed_rows = self.scrape_and_store_details_for_new(valid_rows)

        with session_local() as s:
            dist_by_id = compute_air_distance_for_rows(s, valid_rows,
                                                       detailed_rows=detailed_rows,
                                                       base_address="Via Primaticcio, Milano",
                                                       )

        print("=== Run completed ===")
        print("Inserted:", len(result["inserted_ids"]))
        print("Updated:", len(result["updated_ids"]))
        print("Unchanged:", len(result["unchanged_ids"]))

        self.notify_inserted_listings_via_telegram(
            valid_rows=valid_rows,
            telegram_bot=telegram_bot,
            admin_info=admin_info,
            dist_by_id=dist_by_id,
            detailed_rows=detailed_rows)

        # close the driver
        self.close_driver()

    def _get_starting_page(self) -> None:
        base_url = "https://www.autoscout24.it/"
        self.driver.get_url(base_url, add_slash=True)

    def _reject_cookies(self):
        self.driver.find_element_by_xpath(xpath=f"//button[@class='{self.reject_cookies_class[-1]}']").click()
        sleep(2)
        self.driver.find_element_by_xpath(xpath=f"//button[@class='scr-button scr-button--secondary']").click()
        return True

    # 1) DB -> load ListingSummary rows for inserted_ids
    def load_new_listings(self, inserted_ids: list[str]) -> list[ListingSummary]:
        """Fetch newly inserted listings from DB."""
        if not inserted_ids:
            return []
        with session_local() as s:
            return (
                s.execute(
                    select(ListingSummary).where(ListingSummary.listing_id.in_(inserted_ids))
                )
                .scalars()
                .all()
            )

    # 2) Build Telegram message text (Markdown-friendly)
    def _fmt_eur(self, n: int | None) -> str:
        if n is None:
            return "n.d."
        return f"{n:,.0f}".replace(",", ".")  # 12.345

    def build_listing_text(self, ls: ListingSummary, air_km: float | None = None,
                           price_label: str | None = None,
                           tech_displacement: str | None = None,
                           env_consumption: str | None = None,
                           env_emission_class: str | None = None,
                           seller_phone: str | None = None) -> str:
        """Return a Markdown caption for a listing."""

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
        price = self._fmt_eur(ls.price_eur_num)
        air_km = f"{air_km:.1f}" if air_km is not None else "-"
        age_text = _compute_age(ls.first_registration)

        location = ls.location_text or (ls.zip_code or "n.d.")
        link = ls.detail_url or f"https://www.autoscout24.it/annunci/{ls.listing_id}"

        lines = [
            "🚗 *NUOVO ANNUNCIO*",
            f"*{title}*",
            subtitle,
            f"👤 {ls.seller_type or 'n.d.'}",
            f"📍 {location}",
            f"📏 Distanza (aria): *{air_km} km*",
            f"📆 Anno: {ls.first_registration or 'n.d.'} • {age_text}",
            f"🛣️ Km: {ls.mileage_text or 'n.d.'}",
            # f"Carburante: {ls.fuel_text or 'n.d.'} • Cambio: {ls.gearbox or 'n.d.'}",
            f"⚙️ Cilindrata: {tech_displacement}",
            f"⛽ Carburante: {ls.fuel_text or 'n.d.'}",
            f"💧 Consumo: {env_consumption}",
            f"🌿 Classe Emissioni: {env_emission_class}",
            f"💶 Prezzo: *{price} €* • {price_label}",
            (f"📞 {seller_phone}" if seller_phone is not None else None)
            # link,
        ]
        return "\n".join([l for l in lines if l])

    def notify_inserted_listings_via_telegram(self, valid_rows, telegram_bot, admin_info: list[dict],
                                              dist_by_id: dict[str, float] | None = None,
                                              detailed_rows: dict[str, dict] | None = None):
        """Load new listings and send them to Telegram (photo if available, else text)."""
        if len(valid_rows) == 0:
            return

        for ls in valid_rows:
            air_km = (dist_by_id or {}).get(ls.listing_id)
            price_label = (detailed_rows or {}).get(ls.listing_id, {}).get("price_label")
            tech_displacement = (detailed_rows or {}).get(ls.listing_id, {}).get("tech_displacement")
            env_consumption = (detailed_rows or {}).get(ls.listing_id, {}).get("env_consumption")
            env_emission_class = (detailed_rows or {}).get(ls.listing_id, {}).get("env_emission_class")
            seller_phone = (detailed_rows or {}).get(ls.listing_id, {}).get("seller_phone")

            caption = self.build_listing_text(ls, air_km=air_km, price_label=price_label,
                                              tech_displacement=tech_displacement, env_consumption=env_consumption,
                                              env_emission_class=env_emission_class,
                                              seller_phone=seller_phone)

            link = ls.detail_url or f"https://www.autoscout24.it/annunci/{ls.listing_id}"
            keyboard = {'text': 'Apri su AutoScout24', 'url': link}
            #     {'text': "Non mostrare più", 'data': f"as24_hide@{ls.listing_id}"},
            # ]

            for info in admin_info:
                sent = False
                if ls.image_url:
                    try:
                        resp = requests.get(ls.image_url, timeout=10)
                        resp.raise_for_status()
                        buf = io.BytesIO(resp.content)
                        buf.seek(0)
                        asyncio.run(
                            telegram_bot.send_photo_async(
                                chat_id=info["chat"],
                                photo=buf,
                                caption=caption,
                                inline_keyboard=keyboard,
                                parse_mode="Markdown",
                            )
                        )
                        sent = True
                    except Exception as e:
                        print(f"Failed to send photo for {ls.listing_id}: {e}")

                if not sent:
                    asyncio.run(
                        telegram_bot.send_message(
                            chat_id=info["chat"],
                            text=f"{caption}",
                            inline_keyboard=keyboard,
                            parse_mode="Markdown",
                        )
                    )

    # _________ Listing Details __________
    def scrape_and_store_details_for_new(self, new_rows: List[ListingSummary]) -> Dict[str, dict]:
        """Open each new listing in a new tab, parse details, upsert, and return an enriched payload per listing.

        Returns:
            Dict[str, dict]: one dict per listing with summary + selected detail fields,
                        ready to be sent to Telegram.
        """
        if len(new_rows) == 0:
            return

        results: Dict[str, dict] = {}
        with session_local() as session:
            for ls in new_rows:
                url = ls.detail_url or f"https://www.autoscout24.it/annunci/{ls.listing_id}"
                try:
                    # --- open detail ---
                    self._open_detail_in_new_tab(url)
                    self.driver.web_driver_wait(10)
                    sleep(5)  # small pause for async chunks

                    # --- parse details ---
                    data = self._parse_detail_page() or {}
                    data["listing_id"] = ls.listing_id

                    # --- persist details ---
                    upsert_listing_detail(session, data)
                    session.commit()

                    # --- build enriched payload for notify ---
                    results[ls.listing_id] = {
                        "location_text": data.get("location_text") or ls.location_text,
                        "maps_href": data.get("maps_href"),
                        "price_label": data.get("price_label") or ls.price_label,
                        "tech_displacement": data.get("tech_displacement"),  # e.g. "1.598 cm³"
                        "env_consumption": data.get("env_consumption"),  # e.g. "3,9 l/100 km (comb.)"
                        "env_emission_class": data.get("env_emission_class"),  # e.g. "Euro 6d"
                        "seller_phone": data.get("seller_phone"),
                    }
                except Exception as e:
                    print(f"[detail] fail {ls.listing_id}: {e}")
                finally:
                    # close detail tab and go back to list
                    try:
                        self._close_current_tab_and_back()
                    except Exception:
                        pass
        return results

    def _parse_detail_page(self) -> Dict[str, Any]:
        """Parse the currently open detail page into a dict for ListingDetail."""
        # prova ad espandere blocchi con 'Di Più' (non rompe se già espansi)
        for sec in ["finance-section", "equipment-section", "seller-notes-section", "price-category-section"]:
            self._expand_if_collapsed(sec)

        # --- Try to reveal phone number if the "Mostra numero" button is present
        try:
            # Use only your generic WebDriver helpers
            btn = None
            try:
                # if your wrapper has this method; small timeout to avoid waiting when absent
                btn = self.driver.wait_until_clickable_by_id("call-desktop-button")
            except Exception:
                btn = None

            if btn:
                self.driver.scroll_into_view(btn, block="center")  # generic helper
                self.driver.click_element(btn)  # generic helper
                self.driver.web_driver_wait(1.5)  # tiny pause: number gets injected dynamically
        except Exception:
            # best-effort: if it fails, we just won't have the phone
            pass

        soup = self.driver.get_response()
        get_txt = lambda sel: (soup.select_one(sel).get_text(" ", strip=True) if soup.select_one(sel) else None)

        # --- tiny helper to get any attribute from a CSS selector (BeautifulSoup)
        def get_attr(sel: str, attr: str):
            el = soup.select_one(sel)
            return el.get(attr) if el and el.has_attr(attr) else None

        # --- Extract phone number (after trying to reveal it)
        def _extract_phone(_soup):
            # 1) common pattern: <a href="tel:+39...">
            a_tel = _soup.select_one("a[href^='tel:']")
            if a_tel:
                href = a_tel.get("href", "")
                # keep formatting; normalize minimally
                return href.replace("tel:", "").strip() or None

            # 2) sometimes the button itself gets replaced with the number text
            btn = _soup.select_one("#call-desktop-button")
            if btn:
                import re
                txt = btn.get_text(" ", strip=True)
                # loose match: Italian numbers with optional +39, separators allowed
                m = re.search(r"(?:\+39\s*)?(?:\d[\s\-.]*){7,}", txt)
                if m:
                    # return as “pretty” text (keep separators)
                    return m.group(0).strip()

            # 3) fallback: search any tel link in CTAs area
            ctas = _soup.select_one(".CTAs_emailAndPhoneContainer__970KH")
            if ctas:
                a_tel2 = ctas.select_one("a[href^='tel:']")
                if a_tel2:
                    return a_tel2.get("href", "").replace("tel:", "").strip() or None
            return None

        # --- Stage basics
        title = get_txt(".StageTitle_title__ROiR4")
        model_version = get_txt(".StageTitle_modelVersion__Yof2Z")
        price_text = get_txt("[data-testid='price-section'] .PriceInfo_price__XU0aF")
        price_label = get_txt(".Price_priceLabelButton__w2Qt_ p")
        location_text = get_txt(".LocationWithPin_locationItem__tK1m5")
        maps_href = get_attr(".LocationWithPin_locationItem__tK1m5", "href")
        seller_phone = _extract_phone(soup)

        # Overview items
        ov_map = {}
        for it in soup.select(".VehicleOverview_itemContainer__XSLWi"):
            k = it.select_one(".VehicleOverview_itemTitle__S2_lb")
            v = it.select_one(".VehicleOverview_itemText__AI4dA")
            if k and v:
                ov_map[k.get_text(" ", strip=True).lower()] = v.get_text(" ", strip=True)

        overview_mileage = ov_map.get("chilometraggio")
        overview_gearbox = ov_map.get("tipo di cambio")
        overview_year = ov_map.get("anno")
        overview_fuel = ov_map.get("carburante")
        overview_power = ov_map.get("potenza")
        seller_type = ov_map.get("venditore")

        # main image
        main_img = soup.select_one(".image-gallery-slide picture img") or soup.select_one(".image-gallery-thumbnail-image")
        main_image_url = main_img["src"] if main_img and main_img.has_attr("src") else None

        # Carfax link
        carfax = soup.select_one(".CarReports_button__AkcyE")
        carfax_url = carfax["href"] if carfax and carfax.has_attr("href") else None

        # --- Finanziamento (dl)
        def dl_to_dict(section_id: str) -> dict:
            sec = soup.select_one(f"section#{section_id}")
            out = {}
            if not sec:
                return out
            dts = sec.select("dt")
            dds = sec.select("dd")
            for dt, dd in zip(dts, dds):
                k = dt.get_text(" ", strip=True).lower()
                v = dd.get_text(" ", strip=True)
                out[k] = v
            return out

        fin = dl_to_dict("finance-section")
        fin_auto_price = fin.get("prezzo auto")
        fin_down_payment = fin.get("anticipo")
        fin_duration = fin.get("durata")
        fin_amount = fin.get("importo finanziato")
        fin_total_due = fin.get("importo totale dovuto")
        fin_taeg = fin.get("taeg")
        fin_tan = fin.get("tan fisso")
        fin_installment = fin.get("rata")

        # Dati di base
        basic = dl_to_dict("basic-details-section")
        basic_body = basic.get("carrozzeria")
        basic_vehicle_type = basic.get("tipo di veicolo")
        basic_seats = basic.get("posti")
        basic_doors = basic.get("porte")
        basic_neopatentati = basic.get("per neopatentati")

        # Cronologia veicolo
        hist = dl_to_dict("listing-history-section")
        hist_mileage = hist.get("chilometraggio")
        hist_year = hist.get("anno")
        hist_last_service = hist.get("ultimo tagliando")
        hist_owners = hist.get("proprietari")
        hist_service_book = hist.get("tagliandi certificati")
        hist_non_smoker = hist.get("veicolo non fumatori")

        # Dati tecnici
        tech = dl_to_dict("technical-details-section")
        tech_power = tech.get("potenza")
        tech_gearbox = tech.get("tipo di cambio")
        tech_displacement = tech.get("cilindrata")
        tech_cylinders = tech.get("cilindri")
        tech_weight = tech.get("peso a vuoto")

        # Ambiente
        env = dl_to_dict("environment-details-section")
        env_emission_class = env.get("classe emissioni")
        env_fuel = env.get("carburante")
        env_consumption = env.get("consumo di carburante")

        # Equipaggiamenti (per categoria)
        def equip_json() -> tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
            # Ogni categoria è un dt seguito da dd con <ul><li>...</li></ul>
            sec = soup.select_one("section#equipment-section")
            cats = {"comfort": None, "media": None, "safety": None, "extra": None}
            if not sec:
                return None, None, None, None

            blocks = sec.select("dl")
            items = {"comfort": [], "media": [], "safety": [], "extra": []}
            for dl in blocks:
                dt_dd = list(zip(dl.select("dt"), dl.select("dd")))
                for dt, dd in dt_dd:
                    k = dt.get_text(" ", strip=True).lower()
                    lst = [li.get_text(" ", strip=True) for li in dd.select("li")]
                    if "comfort" in k:
                        items["comfort"] += lst
                    elif "intrattenimento" in k or "media" in k:
                        items["media"] += lst
                    elif "sicurezza" in k:
                        items["safety"] += lst
                    elif "extra" in k:
                        items["extra"] += lst
            import json as _json
            return (
                _json.dumps(items["comfort"], ensure_ascii=False) if items["comfort"] else None,
                _json.dumps(items["media"], ensure_ascii=False) if items["media"] else None,
                _json.dumps(items["safety"], ensure_ascii=False) if items["safety"] else None,
                _json.dumps(items["extra"], ensure_ascii=False) if items["extra"] else None,
            )

        equip_comfort_json, equip_media_json, equip_safety_json, equip_extra_json = equip_json()

        # Descrizione venditore + email (regex)
        seller_notes_el = soup.select_one("#sellerNotesSection .SellerNotesSection_content__te2EB")
        seller_notes = seller_notes_el.get_text("\n", strip=True) if seller_notes_el else None
        seller_email = None
        if seller_notes:
            import re
            m = re.search(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", seller_notes, re.I)
            seller_email = m.group(0) if m else None

        return {
            "title": title, "model_version": model_version, "price_text": price_text, "price_label": price_label,
            "location_text": location_text, "maps_href": maps_href, "seller_type": seller_type,
            "overview_mileage": overview_mileage, "overview_gearbox": overview_gearbox, "overview_year": overview_year,
            "overview_fuel": overview_fuel, "overview_power": overview_power,
            "main_image_url": main_image_url, "carfax_url": carfax_url,
            "fin_auto_price": fin_auto_price, "fin_down_payment": fin_down_payment, "fin_duration": fin_duration,
            "fin_amount": fin_amount, "fin_total_due": fin_total_due, "fin_taeg": fin_taeg, "fin_tan": fin_tan,
            "fin_installment": fin_installment,
            "basic_body": basic_body, "basic_vehicle_type": basic_vehicle_type, "basic_seats": basic_seats,
            "basic_doors": basic_doors, "basic_neopatentati": basic_neopatentati,
            "hist_mileage": hist_mileage, "hist_year": hist_year, "hist_last_service": hist_last_service,
            "hist_owners": hist_owners, "hist_service_book": hist_service_book, "hist_non_smoker": hist_non_smoker,
            "tech_power": tech_power, "tech_gearbox": tech_gearbox, "tech_displacement": tech_displacement,
            "tech_cylinders": tech_cylinders, "tech_weight": tech_weight,
            "env_emission_class": env_emission_class, "env_fuel": env_fuel, "env_consumption": env_consumption,
            "equip_comfort_json": equip_comfort_json, "equip_media_json": equip_media_json,
            "equip_safety_json": equip_safety_json, "equip_extra_json": equip_extra_json,
            "seller_notes": seller_notes, "seller_email": seller_email, "seller_phone": seller_phone,
        }

    def _open_detail_in_new_tab(self, url: str):
        """Open detail URL in a new tab and switch to it."""
        self.driver.driver.execute_script("window.open(arguments[0], '_blank');", url)
        self.driver.driver.switch_to.window(self.driver.driver.window_handles[-1])

    def _close_current_tab_and_back(self):
        """Close current tab and switch back to previous one."""
        cur = self.driver.driver.current_window_handle
        self.driver.driver.close()
        # switch back to the last remaining
        self.driver.driver.switch_to.window(self.driver.driver.window_handles[-1])

    def _expand_if_collapsed(self, section_id: str):
        """If a section has a collapsed 'Di Più' button, click it (best-effort)."""
        try:
            xp = f"//section[@id='{section_id}']//button[@aria-label='Di Più' and @aria-expanded='false']"
            btn = self.driver.find_by_xpath(xp)
            if btn:
                self.driver.scroll_into_view(btn, block="center")
                self.driver.click_element(btn)
                self.driver.web_driver_wait(2)
        except Exception:
            pass

    # ---------- scenario 1 ----------
    def fill_requested_filters(self):
        """Fill all requested filters in one go."""
        self.set_fuel_types(["Elettrica/Benzina", "Benzina", "Metano", "GPL"])  # Carburante: Elettrica/Benzina, Benzina, Metano, GPL
        self.set_year_from(2015)
        self.select_acquista()
        self.set_price_to(MAX_PRICE)
        self.set_city_zip("20147 Milano")
        self.set_radius(RADIUS)
        self.set_mileage_to("100.000")
        self.select_seller_privato()

    # ---------- low-level helpers (page-specific, but only via self.driver API) ----------
    def _open_combo(self, button_id: str):
        """Open a 'button-trigger' combobox by id and return the suggestion container id."""
        trigger = self.driver.wait_until_clickable_by_id(button_id)
        self.driver.scroll_into_view(trigger, block="center")     # uses WebDriver generic helper
        self.driver.click_element(trigger)                        # uses WebDriver generic helper

        sug_id = f"{button_id}-suggestions"
        self.driver.wait_until_present_by_id(sug_id)              # uses WebDriver generic helper
        return sug_id

    def _select_from_combo(self, button_id: str, option_text: str, contains: bool = False):
        """
        Select an option inside a 'button-trigger' combobox by visible text.
        Tries exact match, then contains, then numeric-core contains.
        """
        sug_id = self._open_combo(button_id)

        norm_txt = option_text.strip()
        numeric_core = re.sub(r"[^\d]", "", norm_txt)  # e.g. '9.000 €' -> '9000'

        # Build XPath candidates, all searched via WebDriver helpers
        candidates = []
        if not contains:
            candidates.append(f"//*[@id='{sug_id}']//*[normalize-space()='{norm_txt}']")
        candidates.append(f"//*[@id='{sug_id}']//*[contains(normalize-space(), '{norm_txt}')]")
        if numeric_core:
            # Normalize text removing spaces and common unit symbols before matching digits
            candidates.append(
                f"//*[@id='{sug_id}']//*[contains(translate(normalize-space(.), ' .€kKmM', ''), '{numeric_core}')]"
            )

        option_el = None
        last_err = None
        for xp in candidates:
            try:
                option_el = self.driver.wait_until_clickable_by_xpath(xp)   # uses WebDriver generic helper
                break
            except Exception as e:
                last_err = e

        if not option_el:
            raise RuntimeError(f"Option '{option_text}' not found in '{sug_id}': {last_err}")

        self.driver.scroll_into_view(option_el, block="center")   # uses WebDriver generic helper
        self.driver.click_element(option_el)                      # uses WebDriver generic helper

    # ---------- high-level actions ----------
    def select_acquista(self):
        """Ensure 'Acquista' radio is selected."""
        try:
            radio = self.driver.wait_until_clickable_by_id("price")
            if not radio.is_selected():
                self.driver.click_element(radio)
        except Exception:
            label = self.driver.find_element_by_xpath("//label[@for='price']")
            self.driver.click_element(label)

    def set_year_from(self, year: int):
        """Set 'Anno da'."""
        self._select_from_combo("firstRegistrationFrom-input", str(year))

    def set_price_to(self, euro_text: str):
        """Set 'Prezzo fino a (€)'. Accepts '9000', '9.000', '9.000 €' etc."""
        self._select_from_combo("price-to", euro_text, contains=True)

    def set_city_zip(self, value: str):
        """Set 'Città o CAP' and confirm the autosuggest entry."""
        self.driver.insert_by_id("zipCode-input", value)

        try:
            # Wait for suggestions container
            self.driver.wait_until_all_elements_by_class("//*[@id='zipCode-input-suggestions']")

            # Try exact match
            exact = f"//*[@id='zipCode-input-suggestions']//*[normalize-space()='{value}']"
            try:
                el = self.driver.find_element_by_xpath(exact)
                self.driver.click_element(el)
                return
            except Exception:
                # Fallback: first option
                first = "//*[@id='zipCode-input-suggestions']//*[self::li or self::div][1]"
                el = self.driver.find_element_by_xpath(first)
                self.driver.click_element(el)
                return

        except Exception:
            # Fallback: generic ARROW_DOWN + ENTER
            self.driver.select_first_suggestion("zipCode-input")

    def set_radius(self, km_text: str):
        """Set 'Raggio (Km)'."""
        self._select_from_combo("radius-input", km_text, contains=True)

    def set_mileage_to(self, km_text: str):
        """Set 'Chilometraggio max'."""
        self._select_from_combo("mileageTo-input", km_text, contains=True)

    def select_seller_privato(self):
        """Select 'Venditore: Privato'."""
        try:
            radio = self.driver.wait_until_clickable_by_id("seller-type_privato_radio")
            self.driver.click_element(radio)
        except Exception:
            label = self.driver.find_element_by_xpath("//label[@for='seller-type_privato_radio']")
            self.driver.click_element(label)

    # --- fuel helpers (use only self.driver generic API) ---
    def _open_multiselect_dropdown(self, trigger_id: str):
        """Open a MultiSelect dropdown (like 'Carburante') and return the dropdown root XPath."""
        trigger = self.driver.wait_until_clickable_by_id(trigger_id)
        self.driver.scroll_into_view(trigger, block="center")
        self.driver.click_element(trigger)
        # Dropdown is the sibling <div> of the trigger button inside the MultiSelect container
        dd_xpath = f"//*[@id='{trigger_id}']/following-sibling::div[contains(@class,'MultiSelect_dropdown__')]"
        self.driver.wait_until_present_by_xpath(dd_xpath)
        return dd_xpath

    def _ensure_checkbox_checked(self, checkbox_el):
        """Click the checkbox input if not already selected."""
        try:
            if not checkbox_el.is_selected():
                self.driver.click_element(checkbox_el)
        except Exception:
            # fallback to JS click
            self.driver.js_click(checkbox_el)

    def set_fuel_types(self, names: list[str]):
        """
        Select multiple fuel types in the 'Carburante' MultiSelect using stable IDs.
        Works by clicking the <label for="..."> associated to each hidden <input>.
        """
        if not names:
            return

        # map visible names -> checkbox ids (from your HTML)
        id_map = {
            "Elettrica/Benzina": "checkbox-fuel-type-select-2",
            "Benzina": "checkbox-fuel-type-select-B",
            "Metano": "checkbox-fuel-type-select-C",
            "GPL": "checkbox-fuel-type-select-L",
            # (altri se ti servono)
        }

        # open dropdown and get its xpath scope
        dropdown_xpath = self._open_multiselect_dropdown("fuel-type-select")

        for label_text in names:
            cid = id_map.get(label_text)
            if not cid:
                print(f"[fuel] unknown label '{label_text}' (no id mapping)")
                continue

            # click the <label for="cid"> (more reliable than clicking the <input>)
            label_xpath = f"{dropdown_xpath}//label[@for='{cid}']"
            try:
                lbl = self.driver.wait_until_clickable_by_xpath(label_xpath)
            except Exception:
                # if dropdown auto-closed, re-open and try again once
                dropdown_xpath = self._open_multiselect_dropdown("fuel-type-select")
                lbl = self.driver.wait_until_clickable_by_xpath(label_xpath)

            self.driver.scroll_into_view(lbl, block="center")
            try:
                self.driver.click_element(lbl)
            except Exception:
                # hard fallback: JS click on input
                try:
                    inp = self.driver.wait_until_present_by_id(cid)
                    self.driver.js_click(inp)
                except Exception as e:
                    print(f"[fuel] cannot select '{label_text}' ({cid}): {e}")

        # close dropdown (best-effort)
        try:
            trigger = self.driver.wait_until_clickable_by_id("fuel-type-select")
            self.driver.click_element(trigger)
        except Exception:
            pass

    # ---------- parsing helpers ----------
    def _text_or_none(self, el):
        """Return element text stripped or None."""
        return el.get_text(" ", strip=True) if el else None

    def parse_listings_on_page(self) -> List[Dict[str, Any]]:
        """
        Parse all car cards on the current results page.
        Uses the page HTML via BeautifulSoup (self.driver.get_response()).
        Returns a list of dictionaries (one per listing).
        """
        soup = self.driver.get_response()  # BeautifulSoup of current DOM
        cards = soup.find_all("article", attrs={"data-testid": "list-item"})
        results = []

        for art in cards:
            # --- data-* attributes (fast and robust) ---
            listing_id = art.get("id")
            position = art.get("data-position")
            price_num = art.get("data-price")
            price_label = art.get("data-price-label")
            make = art.get("data-make")
            model = art.get("data-model")
            first_reg = art.get("data-first-registration")
            mileage_num = art.get("data-mileage")
            fuel_code = art.get("data-fuel-type")
            zip_code = art.get("data-listing-zip-code")

            # --- textual fields inside the card ---
            title_wrap = art.select_one(".ListItem_title__ndA4s h2")
            title = self._text_or_none(title_wrap)
            subtitle = self._text_or_none(art.select_one(".ListItem_subtitle__VEw08"))

            # Price text (UI)
            price_text = self._text_or_none(art.select_one("[data-testid='regular-price']"))

            # Spec table items by data-testid
            mileage_text = self._text_or_none(art.select_one("[data-testid='VehicleDetails-mileage_road']"))
            gearbox_text = self._text_or_none(art.select_one("[data-testid='VehicleDetails-gearbox']"))
            year_text = self._text_or_none(art.select_one("[data-testid='VehicleDetails-calendar']"))
            fuel_text = self._text_or_none(art.select_one("[data-testid='VehicleDetails-gas_pump']"))
            power_text = self._text_or_none(art.select_one("[data-testid='VehicleDetails-speedometer']"))

            # Image (first picture on the slider)
            img = art.select_one("picture img")
            image_url = img.get("src") if img else None

            # Seller/Location text (for private sellers)
            location_text = self._text_or_none(art.select_one(".PrivateSellerInfo_private__u71ah"))

            # --- link: try anchor tag ---
            link_tag = art.select_one(".ListItem_title__ndA4s a[href]")
            if link_tag:
                detail_url = link_tag["href"]
                # ensure absolute URL
                if detail_url.startswith("/"):
                    detail_url = "https://www.autoscout24.it" + detail_url
            else:
                # fallback: build URL from listing id
                detail_url = f"https://www.autoscout24.it/annunci/{listing_id}" if listing_id else None

            # --- seller type ---
            seller_code = art.get("data-seller-type")  # "p" or "d"
            if seller_code == "p":
                seller_type = "Privato"
            elif seller_code == "d":
                seller_type = "Rivenditore"
            else:
                # fallback from visible text
                txt = self._text_or_none(art.select_one(".PrivateSellerInfo_private__u71ah"))
                seller_type = "Privato" if txt and "Privato" in txt else "Rivenditore"

            results.append({
                "listing_id": listing_id,
                "position": int(position) if position and position.isdigit() else position,
                "make": make,
                "model": model,
                "title": title,
                "subtitle": subtitle,
                "price_eur_num": int(price_num) if price_num and price_num.isdigit() else price_num,
                "price_text": price_text,
                "price_label": price_label,               # e.g., 'top-price', 'good-price', etc.
                "mileage_num": int(mileage_num) if mileage_num and mileage_num.isdigit() else mileage_num,
                "mileage_text": mileage_text,             # e.g., '78.500 km'
                "gearbox": gearbox_text,                  # e.g., 'Manuale'
                "first_registration": first_reg or year_text,  # data-attr or UI text (e.g., '01/2016')
                "fuel_code": fuel_code,                   # e.g., 'b'
                "fuel_text": fuel_text,                   # e.g., 'Benzina'
                "power_text": power_text,                 # e.g., '66 kW (90 CV)'
                "zip_code": zip_code,
                "seller_type": seller_type,               # 'p' (Privato) or others
                "location_text": location_text,           # e.g., 'Privato, IT-20861 Brugherio'
                "image_url": image_url,
                "detail_url": detail_url,
            })

        return results

    # ---------- pagination ----------
    def go_to_next_page(self) -> bool:
        """
        Navigate to the next results page using the pagination widget.
        Returns True if navigation was triggered, False if we are at the last page or control not found.
        """
        # --- Scope pagination root to avoid accidental matches outside the pager ---
        root = "//div[@data-testid='listpage-pagination']"

        # 1) Try the explicit "Successivo" button if it exists and is not disabled
        try:
            # Ensure the 'next' li is not disabled
            next_btn_xpath = (
                f"{root}//li[contains(@class,'prev-next') and not(contains(@class,'disabled'))]"
                f"//button[@aria-label='Vai alla pagina successiva']"
            )
            next_btn = self.driver.wait_until_clickable_by_xpath(next_btn_xpath)
            self.driver.scroll_into_view(next_btn, block="center")
            self.driver.click_element(next_btn)
            # wait for new content and refresh soup
            self.driver.web_driver_wait()
            self.driver.get_response()
            return True
        except Exception:
            pass

        # 2) Compute next page number and click it (robust fallback)
        try:
            # Find current page button
            cur_btn = self.driver.find_element_by_xpath(
                f"{root}//li[contains(@class,'pagination-item') and contains(@class,'active')]//button[@aria-current='page']"
            )
            cur_txt = cur_btn.text.strip()
            cur_num = int(cur_txt)
            next_num = cur_num + 1

            # Target button by aria-label "Vai alla pagina {next_num}"
            target_xpath = f"{root}//button[@aria-label='Vai alla pagina {next_num}']"
            target = self.driver.wait_until_clickable_by_xpath(target_xpath)
            self.driver.scroll_into_view(target, block="center")
            self.driver.click_element(target)

            self.driver.web_driver_wait()
            self.driver.get_response()
            return True
        except Exception:
            # Likely at the last page: no "Successivo" and no higher page number available
            return False

    def get_total_pages(self) -> int | None:
        """
        Read total pages from the pagination indicator 'X / N'.
        Returns an int or None if the indicator is not found.
        """
        soup = self.driver.get_response()
        # look for the page indicator container
        indicator = soup.select_one(
            "div[data-testid='listpage-pagination'] li.pagination-item--disabled.pagination-item--page-indicator span"
        )
        if not indicator:
            return None
        text = indicator.get_text(strip=True)  # e.g., "1 / 20"
        m = re.search(r"/\s*(\d+)", text)
        return int(m.group(1)) if m else None

    def scrape_all_pages(self, max_pages: int | None = None) -> List[Dict[str, Any]]:
        """
        Scrape the current page and all subsequent pages until no 'next' is available.
        If max_pages is provided, stop after that many pages (useful for testing).
        """
        all_rows: List[Dict[str, Any]] = []

        # Optionally detect total pages up-front (not mandatory to proceed)
        total_pages = self.get_total_pages()

        page_index = 0
        while True:
            page_index += 1

            # Lazy-load content: scroll to bottom to ensure all ~20 cards are in DOM
            try:
                self.driver.scroll_down(max_wait=0.5)
            except Exception:
                pass

            # sleep
            sleep(5)

            # Parse current page
            rows = self.parse_listings_on_page()
            all_rows.extend(rows)

            # Stop conditions:
            if max_pages is not None and page_index >= max_pages:
                break
            if total_pages is not None and page_index >= total_pages:
                break

            # Try to go to next page; if not possible, we are done
            moved = self.go_to_next_page()
            if not moved:
                break

        return all_rows

    # (optional) keep this for backward compatibility; now it just limits pages
    def scrape_pages(self, pages: int) -> List[Dict[str, Any]]:
        """
        Scrape a fixed number of pages (wrapper over scrape_all_pages with a limit).
        """
        return self.scrape_all_pages(max_pages=pages)


if __name__ == '__main__':
    brwsr = Browser.firefox
    app = AutoScout(browser=brwsr, headless=False, sslmode='disable')
    while True:
        hour = datetime.now().hour
        if 9 <= hour < 24:
            app.main()
        else:
            print("⏸ Pausa notturna: nessuna esecuzione tra 00:00 e 09:00")

        sleep(30 * 60)  # wait 15 minutes before next run