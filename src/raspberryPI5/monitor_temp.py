import time
import subprocess
import psycopg2
import os
import asyncio

from pathlib import Path
from dotenv import load_dotenv
from telegram import Bot


# ---- CONFIGURAZIONE ----
MIN_INTERVAL_SECONDS = 15       # ogni quanti secondi salvare la temperatura
ALERT_INTERVAL_SECONDS = 60     # ogni quanto tempo rimandare l'alert se resta sopra soglia

env_path = Path(__file__).resolve().parent.parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

# Database separato solo per le metriche
DB_DSN = "postgresql://admin:admin@localhost:5432/metricsdb"

TELEGRAM_BOT_TOKEN = os.getenv("QUOTES_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("ADMIN_CHAT_ID")
ALERT_THRESHOLD =float(os.getenv("ALERT_TEMP_THRESHOLD"))

if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
    print("⚠️ Attenzione: manca TELEGRAM_BOT_TOKEN o TELEGRAM_CHAT_ID nel .env")

bot = Bot(token=TELEGRAM_BOT_TOKEN)


def read_temp_vcgencmd() -> float:
    """
    Legge la temperatura usando vcgencmd.
    Restituisce la temperatura in °C come float.
    """
    result = subprocess.run(
        ["vcgencmd", "measure_temp"],
        capture_output=True,
        text=True,
        check=True,
    )
    # output tipo: temp=52.3'C
    out = result.stdout.strip()
    value = out.split("=")[1].split("'")[0]
    return float(value)


async def send_telegram_alert_async(temp_c: float):
    """Invia un messaggio su Telegram con python-telegram-bot."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return

    text = f"⚠️ Raspberry caldo: {temp_c:.1f}°C (soglia {ALERT_THRESHOLD:.1f}°C)"
    try:
        await bot.send_message(chat_id=int(TELEGRAM_CHAT_ID), text=text)
    except Exception as e:
        print("Errore nell'invio del messaggio Telegram:", e)


def send_telegram_alert(temp_c: float):
    """Wrapper sync che esegue la coroutine di PTB."""
    try:
        asyncio.run(send_telegram_alert_async(temp_c))
    except RuntimeError:
        # Nel caso ci sia già un event loop (non dovrebbe, qui), fai fallback
        loop = asyncio.get_event_loop()
        loop.run_until_complete(send_telegram_alert_async(temp_c))


def main():
    print(f"📈 Inizio monitoraggio temperatura ogni {MIN_INTERVAL_SECONDS} secondi...")
    conn = psycopg2.connect(DB_DSN)
    conn.autocommit = True
    cur = conn.cursor()

    last_alert_time = None     # timestamp dell'ultimo alert
    above_threshold = False    # eravamo sopra soglia nel giro precedente?

    try:
        while True:
            additional_time = 0
            try:
                temp_c = read_temp_vcgencmd()
                cur.execute(
                    "INSERT INTO rpi_temperature (temp_c) VALUES (%s)",
                    (temp_c,)
                )
                print(f"Salvato: {temp_c:.2f} °C")

                if temp_c < 50:
                    additional_time = 90
                elif temp_c < 60:
                    additional_time = 30
                elif temp_c < 70:
                    additional_time = 15

                now = time.time()

                # Logica alert Telegram
                if temp_c >= ALERT_THRESHOLD:
                    # above threshold
                    if (last_alert_time is None) or (
                        now - last_alert_time >= ALERT_INTERVAL_SECONDS
                    ):
                        send_telegram_alert(temp_c)
                        last_alert_time = now
                    above_threshold = True
                else:
                    # below threshold: reset alert
                    if above_threshold:
                        print("Temperatura tornata sotto soglia, smetto di inviare alert.")
                    above_threshold = False
                    last_alert_time = None

            except Exception as e:
                print("Errore durante la lettura/inserimento:", e)

            time.sleep(MIN_INTERVAL_SECONDS + additional_time)
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()
