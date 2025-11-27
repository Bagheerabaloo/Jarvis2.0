import yaml
from pathlib import Path
from threading import Thread, Lock, Event

from dotenv import load_dotenv
load_dotenv()

from src.scraping.AutoScout.set_up_logger import LOGGER
from src.common.web_driver.Browser import Browser
from src.scraping.AutoScout.telegram_notifications import *
from src.scraping.AutoScout.AutoScout import AutoScout

def load_config() -> dict:
    """Load YAML configuration from file or exit on error."""
    config_path = Path(__file__).parent.joinpath("config_autoscout.yaml")
    try:
        with config_path.open("r", encoding="utf-8") as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        LOGGER.error(f"[FATAL] Config file not found: {config_path}")
    except yaml.YAMLError as e:
        LOGGER.error(f"[FATAL] Error parsing YAML config {config_path}: {e}")
    except Exception as e:
        LOGGER.error(f"[FATAL] Error loading config {config_path}: {e}")

    # if we get here, something went wrong -> exit
    sys.exit(1)

CONFIG = load_config()

MAX_PRICE = CONFIG["autoscout"]["max_price_str"]
RADIUS = CONFIG["autoscout"]["radius_str"]
MAX_MILEAGE_KM = CONFIG["autoscout"]["max_mileage_str"]
PRICE_MAX = int(CONFIG["autoscout"]["price_max"])
MILEAGE_MAX = int(CONFIG["autoscout"]["mileage_max"])
REQUIRED_SELLER = CONFIG["autoscout"]["required_seller"]
FORCE_RUN = bool(CONFIG["autoscout"]["force_run"])
HEADLESS = bool(CONFIG["browser"]["headless"])
FILTER = bool(CONFIG["autoscout"]["filter_enabled"])
SEND_WITHDRAWN_ALERTS = bool(CONFIG["autoscout"]["send_withdrawn_alerts"])


class AutoScoutApp:
    def __init__(self, is_raspberry: bool = False):
        # flag controllato da run_main
        self.run = True
        self.is_raspberry = is_raspberry

        # core scraper
        self.browser = Browser.firefox
        self.stop_event = Event()
        # event loop dedicato al thread
        self.loop: asyncio.AbstractEventLoop | None = asyncio.new_event_loop()
        self.loop_task: asyncio.Task | None = None

        self.autoscout = AutoScout(
            browser=self.browser,
            headless=HEADLESS,
            sslmode="disable",
            stop_event=self.stop_event,
            is_raspberry=is_raspberry
        )

        # telegram / admin
        keys = CONFIG["telegram"]["keys"]
        admin_info, telegram_bot = set_up_telegram_bot(keys=keys, is_raspberry=is_raspberry)
        self.admin_info = admin_info
        self.telegram = telegram_bot
        self.admin = [x for x in admin_info if x.get("is_admin")][0] if admin_info else None

        # stato interno del loop
        self.counter = 0
        self.force_run = FORCE_RUN

        # worker thread
        self.thread: Thread | None = None

    async def _loop(self):
        while self.run:
            init_time = time()
            hour = datetime.now().hour
            bool_ = 9 <= hour < 24

            # fascia notturna
            if not bool_ and not self.force_run:
                LOGGER.info("⏸ Pausa notturna: nessuna esecuzione tra le 00:00 e le 09:00")
                if self.counter == 0:
                    await self.autoscout.send_end_of_day_summary(self.telegram, self.admin_info)
                self.counter += 1

                # sleep 10 minuti, interrompibile via self.run
                for _ in range(10 * 60):
                    if not self.run:
                        return
                    await asyncio.sleep(1)
                continue

            try:
                await self.autoscout.main(telegram_bot=self.telegram, admin_info=self.admin_info)
                self.counter = 0
            except Exception as e:
                LOGGER.error(f"❌ Errore fatale: {e}")
                if self.telegram and self.admin_info:
                    admin_chat = [x["chat"] for x in self.admin_info if x.get("is_admin")][0]
                    await self.telegram.send_message(admin_chat, f"❌ Errore fatale in AutoScout: {e}")

            finally:
                self.autoscout.close_driver()

            self.force_run = False
            LOGGER.info(f"Elapsed time: {seconds_to_time_str(time() - init_time)}")
            LOGGER.info("⏱ In attesa della prossima esecuzione...")

            # sleep 30 minuti, interrompibile via self.run
            for _ in range(30 * 60):
                if not self.run:
                    return
                await asyncio.sleep(1)

    def _thread_target(self):
        """Thread target: run asyncio loop until _loop finishes."""
        assert self.loop is not None
        asyncio.set_event_loop(self.loop)
        # create the main task
        self.loop_task = self.loop.create_task(self._loop())
        try:
            self.loop.run_until_complete(self.loop_task)
        except asyncio.CancelledError:
            # graceful cancellation
            pass
        finally:
            # shutdown async generators and close loop
            self.loop.run_until_complete(self.loop.shutdown_asyncgens())
            self.loop.close()

    def start(self):
        if self.thread and self.thread.is_alive():
            return
        self.thread = Thread(target=self._thread_target, name="AutoScoutAppThread")
        self.thread.start()

    def close(self):
        # chiamato da run_main quando deve fermare l'app
        self.run = False
        self.stop_event.set()


if __name__ == '__main__':
    from src.raspberryPI5.raspberry_init import IS_RASPBERRY
    from queue import Queue
    from src.common.tools.library import run_main  # o dovunque sia definito

    app = AutoScoutApp(is_raspberry=IS_RASPBERRY)
    app.start()  # fa partire il main in un Thread
    run_main(app, logger=LOGGER)  # gestisce SIGINT e shutdown pulito