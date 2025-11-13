from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver import DesiredCapabilities
from selenium.webdriver.common.keys import Keys
from bs4 import BeautifulSoup
from time import sleep
import pandas as pd
from src.common.tools.library import safe_execute
import math
import random
import re
from selenium.webdriver.common.actions.pointer_input import PointerInput
from selenium.webdriver.common.actions.action_builder import ActionBuilder
from selenium.webdriver.common.keys import Keys

class WebDriver:
    def __init__(self, os_environ=False, max_wait=10):
        self.os_environ = os_environ
        self.max_wait = max_wait
        self.driver = None
        self.wait = None
        self.current_response = None

    # _____ Init/Close driver ______
    def _init_options(self, headless: bool, selenium_profile: bool = False):
        pass

    def init_driver(self):
        pass

    def close_driver(self):
        if self.driver:
            safe_execute(None, self.driver.close)
            self.driver = None if not safe_execute(True, self.driver.quit) else self.driver

    # _____ Info ______
    def get_capabilities(self):
        return self.driver.capabilities

    def get_window_size(self):
        return self.driver.get_window_size()

    # _____ Navigation ______
    def get_response(self):
        self.current_response = BeautifulSoup(self.driver.page_source, 'html.parser')
        return self.current_response

    def get_url(self, url, do_wait=True, add_slash=False):

        if add_slash and url[-1] != '/':
            url += '/'

        self.driver.get(url)
        self.web_driver_wait() if do_wait else None  # wait for content to load
        return self.get_response()

    # _____ Wait _____
    def web_driver_wait(self, max_wait=None):
        max_wait = max_wait if max_wait is not None else self.max_wait
        self.wait = WebDriverWait(self.driver, max_wait)

    def wait_until_clickable_by_id(self, id_):
        return self.wait.until(EC.element_to_be_clickable((By.ID, id_)))

    def wait_until_clickable_by_class(self, class_name):
        return self.wait.until(EC.element_to_be_clickable((By.CLASS_NAME, class_name)))

    def wait_until_clickable_by_text(self, button_text):
        return self.wait.until(EC.element_to_be_clickable((By.XPATH, f"//button[text()='{button_text}']")))

    def wait_until_clickable_by_text_a(self, button_text):
        return self.wait.until(EC.element_to_be_clickable((By.XPATH, f"//a[text()='{button_text}']")))

    def wait_until_button_clickable_by_id(self, id):
        return self.wait.until(EC.element_to_be_clickable((By.XPATH, f"//button[@id='{id}']")))

    def wait_until_a_clickable_by_id(self, id):
        return self.wait.until(EC.element_to_be_clickable((By.XPATH, f"//a[@id='{id}']")))

    def wait_until_select_clickable_by_id(self, id):
        return self.wait.until(EC.element_to_be_clickable((By.XPATH, f"//select[@id='{id}']")))

    def wait_until_span_clickable_by_class(self, class_):
        return self.wait.until(EC.element_to_be_clickable((By.XPATH, f"//span[@class='{class_}']")))

    # def wait_until_button_clickable_by_class(self, class_name):
    #     # return self.wait.until(EC.element_to_be_clickable((By.XPATH, f"//button[@class='{class_name}']")))
    #     return self.wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, f"button.{class_name.replace(' ', '.')}"))).click()

    def wait_until_button_clickable_by_class(self, class_name):
        css_selector = f"button.{class_name.replace(' ', '.')}"
        element = self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, css_selector)))
        return self.wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, css_selector)))

    def click_button_by_class(self, class_name):
        try:
            element = self.wait_until_button_clickable_by_class(class_name)
            self.driver.execute_script("arguments[0].scrollIntoView(true);", element)

            try:
                # __ try to click normally __
                element.click()
            except:
                # __ if normal click fails, try a click with offset __
                actions = ActionChains(self.driver)
                actions.move_to_element_with_offset(element, 5, 5).click().perform()
        except Exception as e:
            print(f"An error occurred: {e}")

    def wait_until_button_clickable_by_data_id(self, data_id):
        return self.wait.until(EC.element_to_be_clickable((By.XPATH, f"//button[@data-id='{data_id}']")))

    def wait_until_button_clickable_by_data_testid(self, data_testid):
        return self.wait.until(EC.element_to_be_clickable((By.XPATH, f"//button[@data-testid='{data_testid}']")))

    def wait_until_clickable_by_partial_text(self, partial_text):
        return self.wait.until(EC.element_to_be_clickable((By.PARTIAL_LINK_TEXT, partial_text)))

    def wait_until_div_clickable_by_data_filters_item(self, data_filters_item):
        return self.wait.until(EC.element_to_be_clickable((By.XPATH, f"//div[@data-filters-item='{data_filters_item}']")))

    def wait_until_all_elements_by_class(self, x_path):
        # self.driver.web_driver_wait(self.driver.max_wait)  # wait for content to load
        self.wait.until(EC.presence_of_element_located((By.XPATH, x_path)))

    # _____ Find _____
    def find_all_by_class(self, tag, class_value=None, response=None):
        response = response if response else self.current_response
        return response.findAll(tag, class_=class_value) if class_value else response.findAll(tag)

    def find_all_by_id(self, tag, id_value=None, response=None):
        response = response if response else self.current_response
        return response.findAll(tag, id=id_value) if id_value else response.findAll(tag)

    def find_children_by_class(self, tag, class_value=None, response=None):
        response = response if response else self.current_response
        return response.findChildren(tag, class_=class_value) if class_value else response.findChildren(tag)

    def find_element_by_xpath(self, xpath):
        return self.driver.find_element(By.XPATH, xpath)

    def find_all_div_by_class(self, class_value=None, response=None):
        return self.find_all_by_class(tag='div', class_value=class_value, response=response)

    def find_all_a_by_class(self, class_value=None, response=None):
        return self.find_all_by_class(tag='a', class_value=class_value, response=response)

    def find_all_ul_by_class(self, class_value=None, response=None):
        return self.find_all_by_class(tag='ul', class_value=class_value, response=response)

    def find_all_li_by_class(self, class_value=None, response=None):
        return self.find_all_by_class(tag='li', class_value=class_value, response=response)

    def find_all_span_by_class(self, class_value=None, response=None):
        return self.find_all_by_class(tag='span', class_value=class_value, response=response)

    def find_children_div_by_class(self, class_value=None, response=None):
        return self.find_children_by_class(tag='div', class_value=class_value, response=response)

    def find_children_a_by_class(self, class_value=None, response=None):
        return self.find_children_by_class(tag='a', class_value=class_value, response=response)

    def find_children_img_by_class(self, class_value=None, response=None):
        return self.find_children_by_class(tag='img', class_value=class_value, response=response)

    def find_children_span_by_class(self, class_value=None, response=None):
        return self.find_children_by_class(tag='span', class_value=class_value, response=response)

    def find_all_div_by_id(self, id_value=None, response=None):
        return self.find_all_by_id(tag='div', id_value=id_value, response=response)

    # _____ inputs _____
    def insert_by_id(self, id, text):
        input_element = self.find_element_by_xpath(f"//input[@id='{id}']")
        input_element.send_keys(Keys.CONTROL + "a")
        input_element.send_keys(Keys.DELETE)
        input_element.send_keys(text)

    # _____ Tables _____
    def get_tables_with_pandas(self, table_id):
        all_tables = pd.read_html(self.driver.page_source, attrs={'id': table_id})
        return all_tables[0]

    # _____ Scroll _____
    def get_scroll_height(self):
        return self.driver.execute_script("return document.body.scrollHeight")

    def scroll_down(self, max_wait=None):
        """A method for scrolling the page."""
        last_height = self.driver.execute_script("return document.body.scrollHeight")  # Get scroll height.
        while True:
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")  # Scroll down to the bottom.
            self.web_driver_wait(max_wait if max_wait is not None else self.max_wait)  # wait for content to load
            new_height = self.driver.execute_script("return document.body.scrollHeight")  # Calculate new scroll height and compare with last scroll height.
            if new_height == last_height:
                break
            last_height = new_height

    def scroll_down_by(self, y: int, max_wait: float = None):
        """A method for scrolling the page by an amount equal to y"""
        try:
            self.driver.execute_script(f"window.scrollTo(0, {y});")  # Scroll down by y
            self.web_driver_wait(max_wait if max_wait is not None else self.max_wait)  # wait for content to load
            return y
        except:
            return None

    # _____ Wait (anchor & css generici) _____
    def wait_until_a_clickable_by_class(self, class_name):
        css_selector = f"a.{class_name.replace(' ', '.')}"
        return self.wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, css_selector)))

    def wait_until_clickable_by_css(self, css_selector):
        return self.wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, css_selector)))

    # _____ Click (anchor & css generici) _____
    def click_link_by_class(self, class_name):
        try:
            element = self.wait_until_a_clickable_by_class(class_name)
            self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
            try:
                element.click()
            except Exception:
                # fallback JS click
                self.driver.execute_script("arguments[0].click();", element)
        except Exception as e:
            print(f"Anchor con classe '{class_name}' non cliccabile: {e}")

        sleep(5)

    def click_by_css(self, css_selector):
        """Utility generica per cliccare qualunque elemento tramite CSS."""
        try:
            element = self.wait_until_clickable_by_css(css_selector)
            self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
            try:
                element.click()
            except Exception:
                self.driver.execute_script("arguments[0].click();", element)
        except Exception as e:
            print(f"Elemento '{css_selector}' non cliccabile: {e}")

    def click_element(self, element):
        """Generic safe click: try native click, then JS click as fallback."""
        try:
            element.click()
        except Exception:
            self.driver.execute_script("arguments[0].click();", element)

    def scroll_into_view(self, element, block="center"):
        """Scroll element into view with configurable block ('start'/'center'/'end')."""
        self.driver.execute_script(
            "arguments[0].scrollIntoView({block: arguments[1], inline: 'center'});",
            element, block
        )

    def wait_until_present_by_id(self, element_id):
        """Wait until an element is present in DOM by id (not necessarily clickable)."""
        return self.wait.until(EC.presence_of_element_located((By.ID, element_id)))

    def wait_until_clickable_by_xpath(self, xpath):
        """Wait until an element is clickable using an XPath locator."""
        return self.wait.until(EC.element_to_be_clickable((By.XPATH, xpath)))

    def press_keys(self, element, *keys):
        """Send one or more keys to an element (generic helper)."""
        for key in keys:
            element.send_keys(key)

    def select_first_suggestion(self, input_id):
        """
        Generic helper for autosuggest fields:
        Focus input by id, press ARROW_DOWN then ENTER to confirm first suggestion.
        """
        el = self.find_element_by_xpath(f"//input[@id='{input_id}']")
        el.click()
        el.send_keys(Keys.ARROW_DOWN)
        el.send_keys(Keys.ENTER)



    # ____________
    # ____________
    # ______ Utilities for coordinates ______
    def _get_element_center_vp(self, css_selector):
        """Return the center of the element in viewport coordinates (x,y)."""
        rect = self.driver.execute_script("""
            const el = document.querySelector(arguments[0]);
            if (!el) return null;
            const r = el.getBoundingClientRect();
            return { x: r.left + r.width/2, y: r.top + r.height/2, w: r.width, h: r.height };
        """, css_selector)
        if not rect:
            raise Exception(f"Element not found for selector: {css_selector}")
        return rect

    def _scroll_element_into_view_center(self, css_selector):
        """Scroll the page to center the element in the viewport."""
        self.driver.execute_script("""
            const el = document.querySelector(arguments[0]);
            if (el) el.scrollIntoView({behavior: 'instant', block: 'center', inline: 'center'});
        """, css_selector)

    def _move_mouse_to_viewport_origin(self):
        """Move the mouse to body (1,1) to have a known starting point."""
        body = self.driver.find_element(By.TAG_NAME, "body")
        ActionChains(self.driver).move_to_element_with_offset(body, 1, 1).perform()
        return 1, 1

    # --- add in WebDriver ---
    def wait_until_present_by_xpath(self, xpath: str):
        """Wait until an element exists in the DOM (presence, not necessarily visible)."""
        return self.wait.until(EC.presence_of_element_located((By.XPATH, xpath)))

    # ______ Human-like trajectory generation ______
    def _bezier_path(self, start, end, steps=30, jitter=1.5):
        """
        Generate a quadratic Bezier path with a random control point.
        Adds slight jitter to simulate hand movement.
        """
        sx, sy = start
        ex, ey = end
        # control point near the midpoint with random offset
        mx, my = (sx + ex) / 2.0, (sy + ey) / 2.0
        spread = max(12, math.hypot(ex - sx, ey - sy) * 0.15)
        cx = mx + random.uniform(-spread, spread)
        cy = my + random.uniform(-spread, spread)

        points = []
        for i in range(steps + 1):
            t = i / steps
            # quadratic Bezier formula
            x = (1 - t)**2 * sx + 2 * (1 - t) * t * cx + t**2 * ex
            y = (1 - t)**2 * sy + 2 * (1 - t) * t * cy + t**2 * ey
            # add slight jitter
            x += random.uniform(-jitter, jitter)
            y += random.uniform(-jitter, jitter)
            points.append((x, y))
        return points

    def _perform_mouse_path(self, points, base_pause=(0.007, 0.02)):
        """
        Perform small offset movements along the generated points,
        with random pauses to simulate hand speed variation.
        """
        curx, cury = self._move_mouse_to_viewport_origin()

        chain = ActionChains(self.driver)
        for x, y in points:
            dx = x - curx
            dy = y - cury
            chain.move_by_offset(dx, dy).pause(random.uniform(*base_pause))
            curx, cury = x, y
        chain.perform()

    def _micro_jitter(self, radius=3, n=6):
        """Perform small random movements around the target point."""
        chain = ActionChains(self.driver)
        for _ in range(n):
            angle = random.uniform(0, 2*math.pi)
            r = random.uniform(0.5, radius)
            chain.move_by_offset(r*math.cos(angle), r*math.sin(angle)).pause(random.uniform(0.03, 0.08))
        chain.perform()

    # ______ Full human-like move and click ______
    def human_move_and_click_by_css(self, css_selector, max_steps=36):
        """
        Simulate a realistic mouse move:
        scroll → curved trajectory → short hover → micro-jitter → click.
        """
        try:
            self._scroll_element_into_view_center(css_selector)
            target = self._get_element_center_vp(css_selector)

            # random starting point on the screen (top-left area)
            start = (random.uniform(20, 150), random.uniform(60, 180))

            # number of steps proportional to the distance
            dist = math.hypot(target["x"] - start[0], target["y"] - start[1])
            steps = min(max_steps, max(18, int(dist / 25)))

            path = self._bezier_path(start, (target["x"], target["y"]), steps=steps, jitter=1.2)
            self._perform_mouse_path(path)

            # short hover before clicking
            ActionChains(self.driver).pause(random.uniform(0.15, 0.35)).perform()
            self._micro_jitter(radius=2, n=4)

            # final click
            ActionChains(self.driver).click().perform()

        except Exception as e:
            print(f"[human_move_and_click_by_css] Error on '{css_selector}': {e}")
            # safe fallback
            try:
                el = self.wait_until_clickable_by_css(css_selector)
                el.click()
            except Exception as e2:
                print(f"Fallback click failed: {e2}")



def main():
    driver = WebDriver()
    driver.init_driver()
    sleep(5)
    driver.close_driver()


if __name__ == '__main__':
    main()
