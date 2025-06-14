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


class WebDriver:
    def __init__(self, os_environ=False, max_wait=10):
        self.os_environ = os_environ
        self.max_wait = max_wait
        self.driver = None
        self.wait = None
        self.current_response = None

    # _____ Init/Close driver ______
    def _init_options(self, headless):
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


def main():
    driver = WebDriver()
    driver.init_driver()
    sleep(5)
    driver.close_driver()


if __name__ == '__main__':
    main()
