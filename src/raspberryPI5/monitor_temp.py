import time
import subprocess
import psycopg2

# ---- CONFIGURAZIONE ----
INTERVAL_SECONDS = 30  # ogni quanti secondi salvare la temperatura

# Database separato solo per le metriche
DB_DSN = "postgresql://admin:admin@localhost:5432/metricsdb"

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

def main():
    print(f"📈 Inizio monitoraggio temperatura ogni {INTERVAL_SECONDS} secondi...")
    conn = psycopg2.connect(DB_DSN)
    conn.autocommit = True
    cur = conn.cursor()

    try:
        while True:
            try:
                temp_c = read_temp_vcgencmd()
                cur.execute(
                    "INSERT INTO rpi_temperature (temp_c) VALUES (%s)",
                    (temp_c,)
                )
                print(f"Salvato: {temp_c:.2f} °C")
            except Exception as e:
                print("Errore durante la lettura/inserimento:", e)

            time.sleep(INTERVAL_SECONDS)
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()
