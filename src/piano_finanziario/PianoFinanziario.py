import json
import requests
import io
import datetime

from threading import Thread, Lock
from math import ceil
from enum import Enum
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, date
from time import time, sleep
from dataclasses import dataclass
from copy import deepcopy
from typing import Union

from src.common.tools.library import *
from src.common.web_driver.ChromeDriver import ChromeDriver
from src.common.web_driver.FirefoxDriver import FirefoxDriver


class Browser(str, Enum):
    firefox = 'Firefox'
    chrome = 'Chrome'


class PianoFinanziario:
    reject_cookies_class = ['iubenda-cs-reject-btn iubenda-cs-btn-primary']
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
            self.driver = FirefoxDriver(headless=self.headless)
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

    def main(self):

        import re

        directory = "G:\Slide Pecorari"
        nomi_file = os.listdir(directory)

        pattern_data = r"\d{4}-\d{2}-\d{2}"
        date_list = []

        for nome in nomi_file:
            match = re.search(pattern_data, nome)
            if match:
                data = datetime.strptime(match.group(), "%Y-%m-%d")
                date_list.append(data)

        # find the maximum date
        max_date = max(date_list)

        self.init_driver()
        self.driver.init_driver()

        # __ Get Booking search results page __ #
        self._get_starting_page()

        sleep(5)
        # __ Reject cookies __
        safe_execute(None, self._reject_cookies)
        sleep(5)
        # __ Click on lost password __
        success = safe_execute(False, self._click_on_lost_password())

        # ___ Login ___
        success &= safe_execute(False, self._login)

        sleep(15)
        self.driver.find_element_by_xpath(xpath=f"//a[text()='Contenuti gratuiti']").click()

        self.driver.get_response()
        cards = self.driver.find_all_div_by_class(class_value="course-card my-course-card")

        for card in cards:
            if "I nostri PDF" in card.find("h4").text:
                link_vai = card.find("a", string="Vai")
                if link_vai:
                    href = link_vai.get("href")
                    full_url = self.driver.driver.current_url.split("/")[0] + "//" + self.driver.driver.current_url.split("/")[2] + href
                    print(full_url)
                    self.driver.driver.get(full_url)
                    break

        self.driver.get_response()
        uls = self.driver.find_all_ul_by_class(class_value="course-curriculum uk-accordion")

        ul = uls[0] if len(uls) > 0 else None

        lis = ul.find_all('li') if ul else []

        new_links = []

        for li in lis:
            header_link = li.find("a", class_="uk-accordion-title")
            if not header_link or not header_link.text:
                continue

            # Extract date from the title text (format: DD.MM.YY)
            match = re.search(r"\d{2}\.\d{2}\.\d{2}", header_link.text)
            if not match:
                continue

            # Convert extracted string to datetime object
            day, month, year = match.group().split(".")
            entry_date = datetime.strptime(f"20{year}-{month}-{day}", "%Y-%m-%d")
            formatted_date = entry_date.strftime("%Y-%m-%d")

            # Check if entry is newer than max_date
            if entry_date > max_date:
                content_div = li.find("div", class_="uk-accordion-content")
                if content_div:
                    link_tag = content_div.find("a", href=True)
                    if link_tag:
                        new_links.append({
                            "date": formatted_date,  # <- parametri reali, non hardcoded
                            "href": link_tag["href"]
                        })

        # Assume new_links is already populated and contains relative URLs
        base_url = self.driver.driver.current_url
        protocol = base_url.split("/")[0]
        domain = base_url.split("/")[2]

        for entry in new_links:
            href = entry["href"]
            date_str = entry["date"]

            full_url = f"{protocol}//{domain}{href}"
            print("Opening:", full_url)
            self.driver.driver.get(full_url)
            sleep(5)

            # --- CONFIG ---
            html = self.driver.driver.page_source
            soup = BeautifulSoup(html, "html.parser")
            download_folder = r"G:\Slide Pecorari\new"  # <-- Cambia con la tua destinazione

            # --- EXTRACT PDF LINK ---
            pdf_link_tag = soup.select_one("a[href$='.pdf']")
            pdf_href = pdf_link_tag["href"] if pdf_link_tag else None

            # --- EXTRACT TITLE ---
            title_tag = soup.select_one("div.title__side h1")
            title = title_tag.text.strip() if title_tag else "senza_titolo"

            # --- BUILD FULL PDF URL ---
            pdf_url = f"{protocol}//{domain}{pdf_href}"

            # --- FORMAT FILENAME ---
            safe_title = title.replace(":", "").replace("/", "").replace("?", "").replace('"', '').strip()
            filename = f"{date_str} - {safe_title}.pdf"
            filename = filename.replace("|", "-")
            filepath = os.path.join(download_folder, filename)

            # --- DOWNLOAD PDF ---
            try:
                response = requests.get(pdf_url)
                response.raise_for_status()

                with open(filepath, "wb") as f:
                    f.write(response.content)
                print(f"✅ PDF saved as: {filepath}")

            except Exception as e:
                print(f"❌ Failed to download {pdf_url}: {e}")
            sleep(5)

        # close the driver
        self.close_driver()

    def _get_starting_page(self) -> None:
        base_url = "https://www.pianofinanziario.it/corsi/scopri/i-nostri-pdf/"
        self.driver.get_url(base_url, add_slash=True)

    def _reject_cookies(self):
        self.driver.find_element_by_xpath(xpath=f"//button[@class='{self.reject_cookies_class[-1]}']").click()
        return True

    def _click_on_lost_password(self):
        self.driver.find_element_by_xpath(xpath=f"//a[text()='{self.lost_password_text[-1]}']").click()
        return True

    def _login(self):
        sleep(5)
        self.driver.insert_by_id(id='email', text="valeriostefanelli@hotmail.it")
        self.driver.wait_until_button_clickable_by_id(id='submitto').click()
        sleep(5)
        self.driver.find_element_by_xpath(xpath=f"//a[text()='accesso ospite']").click()
        sleep(5)
        self.driver.insert_by_id(id='cf-name', text="Valerio")
        self.driver.insert_by_id(id='cf-email', text="valeriostefanelli@hotmail.it")
        self.driver.find_element_by_xpath(f"//input[@id='condizioni']").click()
        self.driver.find_element_by_xpath(f"//input[@id='comunicazioni']").click()
        self.driver.find_element_by_xpath(xpath=f"//button[@id='submit-button']").click()
        return True


if __name__ == '__main__':
    brwsr = Browser.firefox
    app = PianoFinanziario(browser=brwsr, headless=False, sslmode='disable')
    app.main()
