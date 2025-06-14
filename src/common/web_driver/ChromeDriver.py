from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver import DesiredCapabilities
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from time import sleep
import os
from src.common.web_driver.Webdriver import WebDriver
from selenium.webdriver.chrome.service import Service


class ChromeDriver(WebDriver):
    def __init__(self, os_environ: bool = False, max_wait: int = 10, headless: bool = False):
        super().__init__(os_environ=os_environ, max_wait=max_wait)

        # ___ Set Chrome options___ #
        self.options = Options()  # Option object allows to set several options for the driver

        # ___ change with selenium 4.0.0
        # self.capabilities = DesiredCapabilities.CHROME.copy()
        # self.options.set_capability("loggingPrefs", {'performance': 'ALL'})

        self._init_options(headless=headless)

        self.driver = None
        self.wait = None
        self.current_response = None

    # _____ Init/Close driver ______
    def _init_options(self, headless):
        self.options.add_argument("--disable-notifications")  # useful to avoid pop-up during automatic navigation
        self.options.add_argument("--window-size=1920,1080")  # size of the browser window automatically opened

        if headless:
            self.options.add_argument("--headless")

        # self.options.add_argument("--headless")
        # self.options.add_argument('--disable-gpu')
        # self.options.add_argument('--no-sandbox')
        # self.options.add_argument("--disable-extensions")
        # self.options.add_argument("--proxy-server='direct://'")
        # self.options.add_argument("--proxy-bypass-list=*")
        # self.options.add_argument("--start-maximized")
        # self.options.add_argument('--disable-dev-shm-usage')
        # self.options.add_argument('--ignore-certificate-errors')

        # ___ change with selenium 4.0.0
        # self.capabilities['acceptSslCerts'] = True
        # self.capabilities['acceptInsecureCerts'] = True
        # self.options.set_capability('acceptSslCerts', True)
        # self.options.set_capability('acceptInsecureCerts', True)
        # self.options.set_capability("loggingPrefs", {'performance': 'ALL'})

        if not self.os_environ:
            # self.executable_path = 'C:/Users/Vale/Downloads/chromedriver_win32_111/chromedriver.exe'
            self.executable_path = r'C:\Users\Vale\Downloads\chromedriver-win64\chromedriver-win64'
        else:
            chrome_bin = os.environ.get('GOOGLE_CHROME_SHIM', None)
            self.executable_path = 'chromedriver'
            self.options.add_argument("--binary_location={}".format(chrome_bin))
            self.options.add_argument("--disable-dev-shm-usage")
            self.options.add_argument("--no-sandbox")
            # might not be needed
            # self.options.add_argument("--remote-debugging-port=9222")
            # self.options.add_argument('--window-size=1920x1480')

            # self.executable_path = os.environ.get('CHROMEDRIVER_PATH', None)
            # self.options.binary_location = os.environ.get('GOOGLE_CHROME_BIN', None)

    def __init_chrome_driver(self):
        # service = Service(executable_path='./chromedriver.exe')
        # self.driver = webdriver.Chrome(service=service, options=self.options, desired_capabilities=self.capabilities)  # Selenium (Chrome) driver with the options defined

        # self.driver = webdriver.Chrome(options=self.options)  # Selenium (Chrome) driver with the options defined

        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=self.options)
        # self.wait = WebDriverWait(self.driver, self.max_wait)

    def init_driver(self):
        if not self.driver:
            self.__init_chrome_driver()


def main():
    driver = ChromeDriver()
    driver.init_driver()
    base_url = "https://www.booking.com/searchresults.it.html"
    driver.get_url(base_url, add_slash=True)
    sleep(5)
    driver.close_driver()


if __name__ == '__main__':
    main()
