import re
import platform
import os
import asyncio

from threading import Thread, Lock, Event
from typing import List, Dict, Any
from sqlalchemy import select
from datetime import datetime, timezone
from typing import Tuple, List, Dict, Optional

from src.common.web_driver.ChromeDriver import ChromeDriver
from src.common.web_driver.FirefoxDriver import FirefoxDriver
from src.common.web_driver.Browser import Browser

from src.scraping.AutoScout.set_up_logger import LOGGER
from src.scraping.AutoScout.db.database import session_local
from src.scraping.AutoScout.upsert_precise_pg import upsert_listings_summary_precise
from src.scraping.AutoScout.upsert_details_pg import upsert_listing_detail
from src.scraping.AutoScout.geo_utils import compute_air_distance_for_rows
from src.scraping.AutoScout.telegram_notifications import *
from src.scraping.AutoScout.db.models import ListingSummary, ListingDetail, ListingDistance
from src.scraping.AutoScout.validators_autoscout import filter_listings_for_request
import re

MAX_PRICE = "9.000 €"
RADIUS = "50 km"
MAX_MILEAGE_KM = "100.000"
PRICE_MAX = 9_000
MILEAGE_MAX = 100_000
REQUIRED_SELLER = "Privato"
FORCE_RUN = True

HEADLESS = False
FILTER = True
SEND_WITHDRAWN_ALERTS = False  # whether to notify also about withdrawn listings


class AutoScout:
    reject_cookies_class = ['_consent-settings_1lphq_103']
    lost_password_text = ['Hai perso la password?']

    def __init__(
            self,
            browser: Browser,
            headless: bool = False,
            stop_event: Event | None = None,
            is_raspberry: bool = False,
            **kwargs
    ):
        self.driver = None
        self.is_raspberry = is_raspberry
        self.headless = headless
        self.browser = browser
        self.lock = Lock()
        self.filter = FILTER
        self.stop_event = stop_event

    # __ helpers __
    def _should_stop(self) -> bool:
        """Return True if a stop has been requested."""
        return bool(self.stop_event and self.stop_event.is_set())

    def _sleep_or_stop(self, seconds: float) -> bool:
        """Sleep in small chunks and abort early if stop is requested."""
        step = 0.5
        remaining = seconds
        while remaining > 0:
            if self._should_stop():
                return False
            s = step if remaining > step else remaining
            sleep(s)
            remaining -= s
        return True

    async def _async_sleep_or_stop(self, seconds: float) -> bool:
        """Async version: use only in async functions."""
        step = 0.5
        remaining = seconds
        while remaining > 0:
            if self._should_stop():
                return False
            s = step if remaining > step else remaining
            await asyncio.sleep(s)
            remaining -= s
        return True

    def _parse_km_int(self, s: str | None) -> int | None:
        # Extract digits from strings like "131.000 km" / "131,000 km"
        if not s:
            return None
        digits = re.sub(r"[^\d]", "", s)
        return int(digits) if digits else None

    # __ initialization __
    def init_driver(self):
        # __ init webdriver __
        if self.browser == Browser.chrome:
            self.driver = ChromeDriver()
            self.driver.init_driver()
        elif self.browser == Browser.firefox:
            # Su Linux (Raspberry) usiamo os_environ=True,
            # su Windows restiamo con os_environ=False.
            self.driver = FirefoxDriver(
                os_environ=self.is_raspberry,      # True su Raspberry, False su Windows
                headless=self.headless,
                selenium_profile=True,    # profilo Selenium su entrambi
            )
            self.driver.init_driver()
        else:
            self.driver = None

    # _____ closing ______
    def close(self):
        self.close_driver()

    def close_driver(self):
        self.lock.acquire()
        if self.driver and self.driver.driver:
            self.driver.close_driver()
            LOGGER.info("Web driver closed")
        self.lock.release()
        self.driver = None

    # __ main __
    async def main(self, telegram_bot: TelegramBot, admin_info: list[dict]) -> None:
        # __ set up web driver __
        self.init_driver() if not self.driver else None

        # __ Get Booking search results page __ #
        await self._get_starting_page()
        LOGGER.info(f'navigator.webdriver = {self.driver.driver.execute_script("return navigator.webdriver")}')
        LOGGER.info(f'navigator.hardwareConcurrency = {self.driver.driver.execute_script("return navigator.hardwareConcurrency")}')
        LOGGER.info(f'navigator.deviceMemory = {self.driver.driver.execute_script("return navigator.deviceMemory")}')

        # __ Reject cookies __
        await self._reject_cookies()

        # __ Fill search form and submit __
        if not await self.fill_search_form_and_submit():
            LOGGER.error("Failed to fill and submit search form.")
            # self.close_driver()
            return

        # rows = self.scrape_pages(2)
        rows = await self.scrape_all_pages() # TODO: add ordering by latest

        if self._should_stop():
            return

        # __ save to DB with upsert __
        with session_local() as session:
            result = upsert_listings_summary_precise(session, rows)

        # __ load newly inserted listings from DB __
        rows = self.load_new_listings(result["inserted_ids"])
        if len(rows) > 0:
            # __ validate against your search filters __
            valid_rows = self.validate_listing(rows)

            # __ scrape details for the new and valid ones __
            detailed_rows = await self.scrape_and_store_details_for_new(valid_rows)

            # __ compute air distances for the valid ones __
            with session_local() as s:
                dist_by_id = compute_air_distance_for_rows(s, valid_rows, detailed_rows=detailed_rows, base_address="Via Primaticcio, Milano")

            # __ notify via Telegram __
            await notify_inserted_listings_via_telegram(
                valid_rows=valid_rows,
                telegram_bot=telegram_bot,
                admin_info=admin_info,
                dist_by_id=dist_by_id,
                detailed_rows=detailed_rows)

        LOGGER.info("=== Run completed ===")
        LOGGER.info(f'Inserted: {len(result["inserted_ids"])}')
        LOGGER.info(f'Updated: {len(result["updated_ids"])}')
        LOGGER.info(f'Unchanged: {len(result["unchanged_ids"])}')
        LOGGER.info("✅ Esecuzione completata.")
        LOGGER.info("🔎 Verifica disponibilità vecchie inserzioni...")

        if self._should_stop():
            return

        # verifica disponibilità
        await self.verify_availability_and_mark(
            telegram_bot=telegram_bot,
            admin_info=admin_info,
            max_to_check=100,
            price_lte=int(MAX_PRICE.replace(".", "").replace("€", "").strip()),
            max_mileage_km=int(MAX_MILEAGE_KM.replace(".", "")),
            seller_type="Privato",
        )

    async def _get_starting_page(self) -> bool:
        base_url = "https://www.autoscout24.it/"
        self.driver.get_url(base_url, add_slash=True)
        return await self._async_sleep_or_stop(5)

    async def _reject_cookies(self) -> bool:
        safe_execute(
            None,
            lambda: self.driver.find_element_by_xpath(xpath=f"//button[@class='{self.reject_cookies_class[-1]}']").click()
        )
        if not await self._async_sleep_or_stop(2):
            return False
        safe_execute(
            None,
            lambda: self.driver.find_element_by_xpath(xpath=f"//button[@class='scr-button scr-button--secondary']").click()
        )
        return await self._async_sleep_or_stop(5)

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

    def validate_listing(self, rows: List[ListingSummary]) -> List[ListingSummary]:
        if self.filter:
            valid_rows = filter_listings_for_request(
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
        return valid_rows

    # _________ Listing Details __________
    async def scrape_and_store_details_for_new(self, new_rows: List[ListingSummary]) -> Dict[str, dict]:
        """Open each new listing in a new tab, parse details, upsert, and return an enriched payload per listing.

        Returns:
            Dict[str, dict]: one dict per listing with summary + selected detail fields,
                        ready to be sent to Telegram.
        """
        if len(new_rows) == 0:
            return {}

        results: Dict[str, dict] = {}
        with session_local() as session:
            for index, ls in enumerate(new_rows):
                LOGGER.info(f"Scraping details for listing ID: {ls.listing_id} - {index+1}/{len(new_rows)}")
                url = ls.detail_url or f"https://www.autoscout24.it/annunci/{ls.listing_id}"
                LOGGER.info(f"Scraping detail for {ls.listing_id} - {url}")
                try:
                    # --- open detail ---
                    self._open_detail_in_new_tab(url)
                    self.driver.web_driver_wait(10)
                    if not await self._async_sleep_or_stop(5):  # small pause for async chunks
                        return {}

                    # --- parse details ---
                    data = self._parse_detail_page() or {}
                    data["listing_id"] = ls.listing_id

                    if self._should_stop():
                        return {}

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
                    LOGGER.info(f"[detail] fail {ls.listing_id}: {e}")
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
        # Use wildcard selectors on class names so they survive hash changes
        title = get_txt("[class*='StageTitle_title__']")
        model_version = get_txt("[class*='StageTitle_modelVersion__']")
        price_text = get_txt("[data-testid='price-section'] [class*='PriceInfo_price__']")
        price_label = get_txt("[class*='Price_priceLabelButton__'] p")
        location_text = get_txt("[class*='LocationWithPin_locationItem__']")
        maps_href = get_attr("[class*='LocationWithPin_locationItem__']", "href")
        seller_phone = _extract_phone(soup)

        # Overview items (top summary block)
        ov_map: dict[str, str] = {}
        for it in soup.select("[class*='VehicleOverview_itemContainer__']"):
            k_el = it.select_one("[class*='VehicleOverview_itemTitle__']")
            v_el = it.select_one("[class*='VehicleOverview_itemText__']")
            if k_el and v_el:
                key = k_el.get_text(" ", strip=True).lower()
                val = v_el.get_text(" ", strip=True)
                ov_map[key] = val

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
        carfax = soup.select_one("[class*='CarReports_button__']")
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
    async def fill_search_form_and_submit(self) -> bool:
        try:
            self.driver.click_link_by_class("hf-searchmask-form__detail-search")
            if not await self.fill_requested_filters():
                return False
            btn = self.driver.wait_until_clickable_by_xpath("//button[contains(@class,'DetailSearchPage_button__')]")
            self.driver.scroll_into_view(btn, block="center")
            self.driver.click_element(btn)
            return await self._async_sleep_or_stop(5)
        except Exception as e:
            LOGGER.info(e)
            return False

    async def fill_requested_filters(self):
        """Fill all requested filters in one go."""
        self.set_fuel_types(["Elettrica/Benzina", "Benzina", "Metano", "GPL"])  # Carburante: Elettrica/Benzina, Benzina, Metano, GPL
        self.set_year_from(2015)
        self.select_acquista()
        self.set_price_to(MAX_PRICE)
        self.set_city_zip("20147 Milano")
        self.set_radius(RADIUS)
        self.set_mileage_to("100.000")
        self.select_seller_privato()
        return self._sleep_or_stop(8)

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
                LOGGER.info(f"[fuel] unknown label '{label_text}' (no id mapping)")
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
                    LOGGER.info(f"[fuel] cannot select '{label_text}' ({cid}): {e}")

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

        # 1) Cards: old "list-item" + new "decluttered-list-item"
        cards = soup.select(
            "article[data-testid='list-item'], article[data-testid='decluttered-list-item']"
        )

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
            # Title + subtitle: old classes + new "Decluttered" classes
            # New layout: <span class="ListItemTitle_title__sLi_x"> and <span class="ListItemTitle_subtitle__V_ao6">
            title = (
                    self._text_or_none(art.select_one(".ListItemTitle_title__sLi_x"))
                    or self._text_or_none(art.select_one(".ListItemTitle_heading__G2W_N"))
                    or self._text_or_none(art.select_one(".ListItem_title__ndA4s h2"))
            )
            subtitle = (
                    self._text_or_none(art.select_one(".ListItemTitle_subtitle__V_ao6"))
                    or self._text_or_none(art.select_one(".ListItem_subtitle__VEw08"))
            )

            # Price text (UI): old data-testid + new ".CurrentPrice_price__Ekflz"
            price_text = self._text_or_none(
                art.select_one("[data-testid='regular-price'], .CurrentPrice_price__Ekflz")
            )

            # Spec table items by data-testid
            # First try old data-testid, if missing fall back to new pill structure
            mileage_text = self._text_or_none(
                art.select_one(
                    "[data-testid='VehicleDetails-mileage_odometer'], "
                    "[data-testid='VehicleDetails-mileage_road'], "
                    "[data-testid^='VehicleDetails-mileage_']"
                )
            )

            gearbox_text = self._text_or_none(
                art.select_one("[data-testid='VehicleDetails-gearbox']")
            )
            year_text = self._text_or_none(
                art.select_one("[data-testid='VehicleDetails-calendar']")
            )
            fuel_text = self._text_or_none(
                art.select_one("[data-testid='VehicleDetails-gas_pump']")
            )
            power_text = self._text_or_none(
                art.select_one("[data-testid='VehicleDetails-speedometer']")
            )

            # Fill only missing fields from pills (works even if some are already present)
            pills = art.select("[data-testid^='VehicleDetails-']")
            for pill in pills:
                dtid = pill.get("data-testid") or ""
                pill_text = self._text_or_none(pill)

                if not pill_text:
                    continue

                if (not year_text) and dtid == "VehicleDetails-calendar":
                    year_text = pill_text
                elif (not mileage_text) and dtid.startswith("VehicleDetails-mileage_"):
                    mileage_text = pill_text
                elif (not fuel_text) and dtid == "VehicleDetails-gas_pump":
                    fuel_text = pill_text
                elif (not power_text) and dtid == "VehicleDetails-speedometer":
                    power_text = pill_text

            # Image (first picture on the slider)
            img = art.select_one("picture img")
            image_url = img.get("src") if img else None

            # Seller/Location text: old private seller div + new ListItemSeller_* classes
            location_text = self._text_or_none(
                art.select_one(
                    ".PrivateSellerInfo_private__u71ah, .ListItemSeller_address__Fqhiu"
                )
            )

            # --- link: title anchor ---
            # Old: .ListItem_title__ndA4s a[href]
            # New: .ListItemTitle_wrapper__QhK3w a[href] / a.ListItemTitle_anchor__4TrfR
            link_tag = art.select_one(
                ".ListItem_title__ndA4s a[href], "
                ".ListItemTitle_wrapper__QhK3w a[href], "
                "a.ListItemTitle_anchor__4TrfR[href]"
            )
            if link_tag:
                detail_url = link_tag["href"]
                # ensure absolute URL
                if detail_url.startswith("/"):
                    detail_url = "https://www.autoscout24.it" + detail_url
            else:
                # fallback: build URL from listing id
                detail_url = (
                    f"https://www.autoscout24.it/annunci/{listing_id}" if listing_id else None
                )

            # --- seller type ---
            seller_code = art.get("data-seller-type")  # "p" or "d"
            if seller_code == "p":
                seller_type = "Privato"
            elif seller_code == "d":
                seller_type = "Rivenditore"
            else:
                # fallback from visible text
                txt = self._text_or_none(
                    art.select_one(
                        ".PrivateSellerInfo_private__u71ah, .ListItemSeller_name__3T6DT"
                    )
                )
                seller_type = "Privato" if txt and "Privato" in txt else "Rivenditore"

            mileage_num_int = self._parse_km_int(mileage_text)

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
                "mileage_num": mileage_num_int,
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

    async def scrape_all_pages(self, max_pages: int | None = None) -> List[Dict[str, Any]]:
        """
        Scrape the current page and all subsequent pages until no 'next' is available.
        If max_pages is provided, stop after that many pages (useful for testing).
        """
        all_rows: List[Dict[str, Any]] = []

        # Optionally detect total pages up-front (not mandatory to proceed)
        total_pages = self.get_total_pages()
        LOGGER.info(f"Total pages detected: {total_pages}" if total_pages else "Total pages not detected")

        page_index = 0
        while True:
            if self._should_stop():
                return []  # interrupted

            page_index += 1

            LOGGER.info(f"Scraping page {page_index}...")

            # Lazy-load content: scroll to bottom to ensure all ~20 cards are in DOM
            try:
                self.driver.scroll_down(max_wait=0.5)
            except Exception:
                pass

            # sleep
            if not await self._async_sleep_or_stop(5):
                return []  # interrupted

            # Parse current page
            try:
                rows = self.parse_listings_on_page()
                LOGGER.info(f"Found {len(rows)} rows")
                all_rows.extend(rows)
            except:
                pass

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

    # ---------- verify availability ----------
    def _is_detail_gone(self, soup) -> bool:
        """
        Robustly detect the 'not available' detail page.
        Avoids relying on hashed CSS class suffixes and normalizes text.
        """
        # 1) Main container like <main class="GonePage_main__...">
        main_gone = soup.find(
            'main',
            class_=lambda cs: cs and any(str(c).startswith('GonePage_main__')
                                         for c in (cs if isinstance(cs, list) else [cs]))
        )
        if main_gone:
            return True

        # 2) Headline like <div class="GonePage_title__..."> ... "non è più disponibile"
        title_el = soup.find(
            lambda tag: tag.name in ('div', 'p', 'h1', 'h2', 'h3')
                        and any(str(c).startswith('GonePage_title__') for c in (tag.get('class') or []))
        )
        if title_el:
            import unicodedata, re
            txt = unicodedata.normalize('NFKD', title_el.get_text(" ", strip=True))
            txt = ''.join(ch for ch in txt if not unicodedata.combining(ch)).lower()
            txt = re.sub(r'\s+', ' ', txt)
            if 'non e piu disponibile' in txt:  # accent-insensitive
                return True

        # 3) CTA "Mostra altri ..." inside a GonePage link/button container
        btn = soup.find('a', class_=lambda cs: cs and 'scr-button' in cs and 'scr-button--primary' in cs)
        if btn and btn.get_text(" ", strip=True).startswith('Mostra altri'):
            parent = btn.find_parent(
                lambda tag: any(str(c).startswith('GonePage_linkAndButtonContainer__')
                                for c in (tag.get('class') or []))
            )
            if parent:
                return True

        return False

    async def verify_availability_and_mark(self,
                                     telegram_bot: TelegramBot,
                                     admin_info: list[dict],
                                     max_to_check: int = 30,
                                     price_lte: int = 9000,
                                     max_mileage_km: int = 100000,
                                     seller_type: str = 'Privato'
                                     ):
        """
        Fetch listings that match the criteria, open each detail URL,
        detect if the ad is 'gone', and update availability flags on ListingSummary.
        Filters applied:
          - is_available = TRUE
          - price_eur_num <= price_lte
          - mileage_num < 100_000
          - seller_type == 'Privato'
          - (optional) fuel_text IN fuels
          - (optional) is_active = TRUE
        """

        # __ set up web driver __
        self.init_driver() if not self.driver else None

        now = datetime.now(timezone.utc)

        with session_local() as session:
            q = session.query(ListingSummary).filter(
                ListingSummary.is_available.is_(True),
                ListingSummary.price_eur_num.isnot(None),
                ListingSummary.price_eur_num <= price_lte,
                ListingSummary.mileage_num.isnot(None),
                ListingSummary.mileage_num < max_mileage_km,
                ListingSummary.seller_type == seller_type,
            )

            # priority is given to those that haven't been checked yet
            q = q.order_by(
                ListingSummary.last_availability_check_at.asc().nullsfirst(),
                ListingSummary.first_seen_at.desc(),
            ).limit(max_to_check)

            candidates: list[ListingSummary] = q.all()

            for index, ls in enumerate(candidates):
                LOGGER.info(f"[availability] checking {index + 1}/{len(candidates)}: {ls.listing_id}")
                url = ls.detail_url or f"https://www.autoscout24.it/annunci/{ls.listing_id}"
                try:
                    self._open_detail_in_new_tab(url)
                    self.driver.web_driver_wait(10)
                    if not self._sleep_or_stop(2):
                        LOGGER.info("[availability] interrupted during sleep")
                        return

                    soup = self.driver.get_response()

                    gone = self._is_detail_gone(soup)
                    ls.last_availability_check_at = now
                    if gone and ls.is_available:
                        LOGGER.info(f"[availability] marking gone {ls.listing_id} ({url})")
                        if telegram_bot and admin_info and SEND_WITHDRAWN_ALERTS:
                            admin_chat = [x['chat'] for x in admin_info if x['is_admin']][0]
                            await telegram_bot.send_message(
                                admin_chat,
                                f"⚠️ AutoScout: annuncio non più disponibile!\n{ls.title}\n{url}"
                            )
                        ls.is_available = False
                        ls.unavailable_at = now

                    session.add(ls)
                    session.commit()
                except Exception as e:
                    session.rollback()
                    LOGGER.info(f"[availability] fail {ls.listing_id}: {e}")
                finally:
                    try:
                        self._close_current_tab_and_back()
                    except Exception:
                        pass

        # self.close_driver()

    # ---------- end of day summary ----------
    @staticmethod
    async def _send_eod(telegram_bot, admin_info, text):
        # Send sequentially within one loop - sending to all admin
        for info in admin_info:
            try:
                await telegram_bot.send_message(
                    chat_id=info["chat"],
                    text=text,
                    parse_mode="Markdown",
                )
            except:
                LOGGER.error(f"EOD summary: failed to send to {info['chat']}")

    async def send_end_of_day_summary(self, telegram_bot, admin_info: list[dict], day: datetime | None = None, limit_per_make_model: int = 3):
        """
        - takes the 'new' listings of the day (first_seen_at during the day, eligible)
        - takes the 'withdrawn' listings of the day (unavailable_at during the day
        - sends a single Telegram message with a brief summary
        """
        from src.scraping.AutoScout.db.database import session_local
        from src.scraping.AutoScout.db.models import ListingSummary

        start_utc, end_utc = rome_day_window(day)

        with session_local() as s:
            # NEW: first_seen_at during the day, eligible, and still available
            q_new = (
                s.query(ListingSummary)
                .where(ListingSummary.first_seen_at >= start_utc,
                       ListingSummary.first_seen_at < end_utc,
                       ListingSummary.is_available.is_(True),
                       ListingSummary.price_eur_num.isnot(None),
                       ListingSummary.price_eur_num <= PRICE_MAX,
                       ListingSummary.mileage_num.isnot(None),
                       ListingSummary.mileage_num <= MILEAGE_MAX,
                       ListingSummary.seller_type == REQUIRED_SELLER)
            )
            new_rows = q_new.all()

            # WITHDRAWN: changed to unavailable today (unavailable_at during the day)
            q_gone = (
                s.query(ListingSummary)
                .where(ListingSummary.unavailable_at.isnot(None),
                       ListingSummary.unavailable_at >= start_utc,
                       ListingSummary.unavailable_at < end_utc)
            )
            withdrawn_rows = q_gone.all()

        text = build_daily_summary_text(new_rows, withdrawn_rows)
        await self._send_eod(telegram_bot, admin_info, text)


async def main(is_raspberry: bool = False):
    brwsr = Browser.firefox

    # __ set up telegram bot __
    keys = ["ADMIN_INFO", "TELEGRAM_MARINELLA"]
    admin_info, telegram_bot = set_up_telegram_bot(keys=keys, is_raspberry=is_raspberry)

    counter = 0
    force_run = FORCE_RUN

    app = AutoScout(browser=brwsr, headless=HEADLESS, sslmode='disable', is_raspberry=is_raspberry)
    while True:
        init_time = time()
        hour = datetime.now().hour
        bool_ = 9 <= hour < 24
        if not bool_ and not force_run:
            LOGGER.info("⏸ Pausa notturna: nessuna esecuzione tra le 00:00 e le 09:00")
            if counter == 0:
                await app.send_end_of_day_summary(telegram_bot, admin_info)
            counter += 1
            sleep(10*60)
            continue

        try:
            await app.main(telegram_bot=telegram_bot, admin_info=admin_info)
            await app.verify_availability_and_mark(
                telegram_bot=telegram_bot,
                admin_info=admin_info,
                max_to_check=100,
                price_lte=int(MAX_PRICE.replace(".", "").replace("€", "").strip()),
                max_mileage_km=int(MAX_MILEAGE_KM.replace(".", "")),
                seller_type='Privato'
            )
            counter = 0
        except Exception as e:
            LOGGER.info(f"❌ Errore fatale: {e}")
            if telegram_bot and admin_info:
                admin_chat = [x['chat'] for x in admin_info if x['is_admin']][0]
                await telegram_bot.send_message(admin_chat, f"❌ Errore fatale in AutoScout: {e}")
        finally:
            app.close_driver()

        force_run = False
        LOGGER.info(f"Elapsed time: {seconds_to_time_str(time() - init_time)}")
        LOGGER.info("⏱ In attesa della prossima esecuzione...")
        sleep(30 * 60)  # wait 30 minutes before next run


if __name__ == '__main__':
    from src.raspberryPI5.raspberry_init import IS_RASPBERRY

    asyncio.run(main(is_raspberry=IS_RASPBERRY))