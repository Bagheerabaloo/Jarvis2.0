from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.firefox_binary import FirefoxBinary
from time import sleep
import os
from src.common.web_driver.Webdriver import WebDriver


class FirefoxDriver(WebDriver):
    def __init__(self, os_environ: bool = False, max_wait: int = 10, headless: bool = False, selenium_profile: bool = False):
        super().__init__(os_environ=os_environ, max_wait=max_wait)

        self.selenium_profile = selenium_profile  # if True, uses Firefox to avoid CloudFare blocks

        # ___ Set Firefox options___ #
        self.options = Options()
        self._init_options(headless=headless, selenium_profile=selenium_profile)

        self.driver = None
        self.wait = None
        self.current_response = None

    # _____ Init/Close driver ______
    def _init_options(self, headless: bool, selenium_profile: bool = False):
        if headless or self.os_environ:
            self.options.headless = True

        if selenium_profile and not self.os_environ:
            profile_path = r'C:\Users\Vale\AppData\Roaming\Mozilla\Firefox\Profiles\bym01i3w.SeleniumFF'
            self.options.profile = profile_path
            if headless or self.os_environ:
                self.options.headless = True
                self.options.add_argument("-headless")
            # self.options.set_preference("intl.accept_languages", "it-IT, it, en-US, en")

        if not self.os_environ:
            self.options.binary_location = r'C:\Program Files\Mozilla Firefox\firefox.exe'
            self.executable_path = 'C:/Users/Vale/Downloads/geckodriver-v0.32.2-win32/geckodriver.exe'
        else:
            self.binary = FirefoxBinary(os.environ.get('FIREFOX_BIN'))
            self.executable_path = os.environ.get('GECKODRIVER_PATH')
            self.options.add_argument("-remote-debugging-port=9224")
            self.options.add_argument("-disable-gpu")
            self.options.add_argument("-no-sandbox")

    def __init_firefox_driver(self, maximize_window: bool = False):
        if self.os_environ:
            self.driver = webdriver.Firefox(firefox_binary=self.binary, executable_path=self.executable_path, options=self.options)
        else:
            # self.driver = webdriver.Firefox(executable_path=self.executable_path, options=self.options)
            self.driver = webdriver.Firefox(options=self.options)  # Selenium (Chrome) driver with the options defined
            if maximize_window:
                self.driver.maximize_window()
            if self.selenium_profile:
                addon_path = os.path.abspath(r"C:\Users\Vale\PycharmProjects\Jarvis2.0\src\common\web_driver\webdriver_override_ff")
                self.driver.install_addon(addon_path, temporary=True)

            # Checks
            print("languages:", self.driver.execute_script("return navigator.languages"))
            print("perm notifications:", self.driver.execute_script("""
              return navigator.permissions.query({name: 'notifications'})
                .then(r => r.state)
            """))

    def init_driver(self, maximize_window: bool = False):
        if not self.driver:
            self.__init_firefox_driver(maximize_window)


def main():
    driver = FirefoxDriver()
    driver.init_driver()
    base_url = "https://www.booking.com/searchresults.it.html"
    driver.get_url(base_url, add_slash=True)
    sleep(5)
    driver.close_driver()


if __name__ == '__main__':
    main()
