import asyncio
import sys
from pathlib import Path

import yaml
from dotenv import load_dotenv

load_dotenv()

from src.scraping.AutoScout.set_up_logger import LOGGER
from src.scraping.AutoScout.AutoScout import AutoScout
from src.scraping.AutoScout.telegram_notifications import set_up_telegram_bot
from src.common.web_driver.Browser import Browser
from src.raspberryPI5.raspberry_init import IS_RASPBERRY


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
    raise SystemExit(1)


CONFIG = load_config()


async def notify_telegram_failure(telegram_bot, admin_info, text: str):
    """
    Invia un messaggio Telegram di errore a tutti i contatti in admin_info.
    admin_info: lista di dict con almeno {"chat": <id>}
    """
    if not telegram_bot or not admin_info:
        return

    for u in admin_info:
        chat_id = u.get("chat")
        if not chat_id:
            continue
        try:
            await telegram_bot.send_message(chat_id=chat_id, text=text)
        except Exception:
            pass


async def run_once(is_raspberry: bool, config:dict) -> int:
    telegram_bot = None
    admin_info = []
    app = None

    try:
        keys = config["telegram"]["keys"]
        if not keys:
            raise RuntimeError("CONFIG.telegram.admin_keys mancante o vuoto")

        admin_info, telegram_bot = set_up_telegram_bot(keys=keys, is_raspberry=is_raspberry)

        app = AutoScout(
            browser=Browser.firefox,
            headless=True,
            sslmode="disable",
            stop_event=None,
            is_raspberry=is_raspberry,
        )

        # Run scraping
        await app.main(telegram_bot=telegram_bot, admin_info=admin_info)

        # Send end-of-day summary
        await app.send_end_of_day_summary(telegram_bot, admin_info)

        LOGGER.info("✅ AutoScoutRunOnce completato con summary.")
        return 0

    except Exception as e:
        LOGGER.exception(f"❌ AutoScoutRunOnce fallito: {e}")

        msg = (
            "❌ AutoScout (run singola) FALLITA\n"
            f"Errore: {type(e).__name__}: {e}\n"
        )
        try:
            await notify_telegram_failure(telegram_bot, admin_info, msg)
        except Exception:
            pass

        return 1

    finally:
        # chiusura driver sempre, ma solo se app è stato creato
        if app is not None:
            try:
                app.close_driver()
            except Exception:
                pass


if __name__ == "__main__":
    sys.exit(asyncio.run(run_once(is_raspberry=IS_RASPBERRY, config=CONFIG)))