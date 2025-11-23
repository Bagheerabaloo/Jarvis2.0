from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.service import Service as FirefoxService
# from selenium.webdriver.firefox.firefox_binary import FirefoxBinary
from time import sleep
import os
from src.common.web_driver.Webdriver import WebDriver


class FirefoxDriver(WebDriver):
    def __init__(
            self,
            os_environ: bool = False,
            max_wait: int = 10,
            headless: bool = False,
            selenium_profile: bool = False
    ):
        super().__init__(os_environ=os_environ, max_wait=max_wait)

        self.selenium_profile = selenium_profile  # if True, uses Firefox to avoid CloudFare blocks
        self.options = Options()
        self.service = None
        self.headless = headless
        self._init_options(headless=headless, selenium_profile=selenium_profile)

        self.driver = None
        self.wait = None
        self.current_response = None

    # _____ Init/Close driver ______
    def _init_options(self, headless: bool, selenium_profile: bool = False):
        """
        Configura Firefox Options (binary, profilo, headless, flag vari).
        """

        # --- Headless ---
        if headless or self.os_environ:
            self.options.headless = True
            self.options.add_argument("-headless")

        # --- Binary: SOLO via options.binary_location ---
        if self.os_environ:
            # ===============================
            #   RASPBERRY / LINUX
            # ===============================
            # Binario di Firefox dal .env
            firefox_bin = os.environ.get("FIREFOX_BIN")
            if firefox_bin:
                self.options.binary_location = firefox_bin

            # Profilo Selenium copiato dal PC
            if selenium_profile:
                profile_path = os.environ.get("SELENIUM_FIREFOX_PROFILE_RPI")
                if profile_path:
                    self.options.profile = profile_path

            # Flag extra, tipici per headless su Linux
            self.options.add_argument("-remote-debugging-port=9224")
            self.options.add_argument("-disable-gpu")
            self.options.add_argument("-no-sandbox")

            # Service per geckodriver (path dal .env, oppure dal PATH se non settato)
            gecko_path = os.environ.get("GECKODRIVER_PATH", "geckodriver")
            self.service = FirefoxService(executable_path=gecko_path)

        else:
            # ===============================
            #   WINDOWS
            # ===============================
            # Firefox bin (override da env se serve)
            firefox_bin = os.environ.get(
                "FIREFOX_BIN_WIN",
                r"C:\Program Files\Mozilla Firefox\firefox.exe",
            )
            self.options.binary_location = firefox_bin

            # Profilo Selenium  (override da env se serve)
            if selenium_profile:
                profile_path = os.environ.get(
                    "SELENIUM_FIREFOX_PROFILE_WIN",
                    r"C:\Users\Vale\AppData\Roaming\Mozilla\Firefox\Profiles\bym01i3w.SeleniumFF",
                )
                if profile_path:
                    self.options.profile = profile_path

            # Su Windows NON usiamo Service
            self.service = None

    def __init_firefox_driver(self, maximize_window: bool = False):
        """
        Crea l'istanza di webdriver.Firefox usando:
            - executable_path = self.executable_path  (geckodriver)
            - options = self.options
        """
        if self.os_environ:
            # Raspberry / Linux: usa Service + options
            self.driver = webdriver.Firefox(service=self.service, options=self.options)
        else:
            # Windows: niente Service, solo options (geckodriver trovato via PATH)
            self.driver = webdriver.Firefox(options=self.options)
            if maximize_window and not self.headless:
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
