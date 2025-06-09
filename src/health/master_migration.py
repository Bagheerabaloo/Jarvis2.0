import pandas as pd
from src.health.src.db.database import session_local
from src.health.src.db.models import DailyRoutine
from datetime import datetime, time

# 1. Read the Excel file from the specified sheet
df = pd.read_excel(
    r"C:\Users\Vale\Documents\Google Sheets\master.xlsx",
    sheet_name="Routine Giornaliera"
)

# 2. Normalize boolean values from strings or numbers
def normalize_bool(value):
    if pd.isna(value):
        return None
    if isinstance(value, str):
        return 1 if value.strip().lower() in ["yes", "true", "sì", "si", "y", "1"] else 0
    return int(value)


# 3. Convert time strings to time objects
def parse_time(value):
    try:
        return pd.to_datetime(value).time() if not pd.isna(value) else None
    except:
        return None


def time_decimal_from_21(value):
    if pd.isna(value):
        return None
    try:
        # Case 1: datetime.time object
        if isinstance(value, time):
            total_hours = value.hour + value.minute / 60.0
        # Case 2: string in "hh:mm" format
        elif isinstance(value, str) and ":" in value:
            parts = value.strip().split(":")
            if len(parts) >= 2:
                hour = int(parts[0])
                minute = int(parts[1])
                total_hours = hour + minute / 60.0
            else:
                return None
        # Case 3: float or string like "23.25"
        else:
            val = float(value)
            hour = int(val)
            minutes = int(round((val - hour) * 100))
            total_hours = hour + minutes / 60.0

        shifted = (total_hours - 21) % 24
        return round(shifted, 2)
    except Exception as e:
        print(f"Time parsing error: {value} -> {e}")
        return None

# 4. Start DB session
session = session_local()

# 5. Iterate over the DataFrame rows and create DailyRoutine instances
for i, row in df.iterrows():
    try:
        # Validate date (primary key)
        if pd.isna(row["Data"]):
            print(f"Row {i} skipped: missing date")
            continue

        routine = DailyRoutine(
            date=pd.to_datetime(row["Data"]).date(),
            day_of_week=row["DoW"],
            weight=row["Peso"] if not pd.isna(row["Peso"]) else None,

            water_morning=normalize_bool(row["Acqua Mattina"]),
            workout_morning=normalize_bool(row["Allenamento Mattina"]),
            made_bed=normalize_bool(row["Rifare Letto"]),
            washed_face=normalize_bool(row["Faccia"]),
            breakfast=normalize_bool(row["Colazione"]),
            set_tasks=normalize_bool(row["Task Set"]),
            cream_applied=normalize_bool(row["Crema Emolliente"]),

            fruit=normalize_bool(row["Frutta"]),
            vegetables=normalize_bool(row["Verdura"]),
            dried_fruit=normalize_bool(row["Frutta Secca"]),
            fitness_ring=normalize_bool(row["Anello"]),

            water_day_1=normalize_bool(row["Acqua Giorno 1"]),
            water_day_2=normalize_bool(row["Acqua Giorno 2"]),
            workout_day=normalize_bool(row["Allenamento Giorno"]),
            stretching=normalize_bool(row["Stretching"]),
            cold_shower=normalize_bool(row["Doccia Fredda"]),
            water_evening=normalize_bool(row["Acqua Sera"]),

            three_positive_things=normalize_bool(row["3 Cose Positive"]),
            mood=normalize_bool(row["Umore"]),
            set_tasks_completed=normalize_bool(row["Task Done"]),
            planning=normalize_bool(row["Planning"]),
            wishes_101=normalize_bool(row["101 Desideri"]),
            breakfast_set=normalize_bool(row["Colazione Set"]),
            reading=normalize_bool(row["Leggere"]),
            music=normalize_bool(row["Musica"]),

            bedtime=time_decimal_from_21(row["Ora Letto"]),
            wakeup_time=time_decimal_from_21(row["Ora Sveglia"]),
            sleep_score=int(row["Punteggio Sonno"]) if not pd.isna(row["Punteggio Sonno"]) else None,
            sleep_duration=str(row["Durata Sonno"]) if not pd.isna(row["Durata Sonno"]) else None,
            heart_rate_min=int(row["Battito Minimo"]) if not pd.isna(row["Battito Minimo"]) else None,
            heart_rate_rest=int(row["Battito a Riposo"]) if not pd.isna(row["Battito a Riposo"]) else None,
            weather=row["Meteo"],

            morning_routine_end_time=time_decimal_from_21(row["Ora fine routine mattutina"]),
            workout_end_time=time_decimal_from_21(row["Ora fine allenamento"]),

            smart=normalize_bool(row["Smart"]),
            tasks_set=normalize_bool(row["Set"]),
            tasks_done=normalize_bool(row["Done"]),

            routine_morning=normalize_bool(row["Routine Mattina"]),
            routine_day=normalize_bool(row["Routine Giorno"]),
            routine_evening=normalize_bool(row["Routine Sera"]),
            routine_workout=normalize_bool(row["Routine Allenamento"]),
            routine_water=normalize_bool(row["Routine Acqua"]),

            bdnf=normalize_bool(row["BDNF"])
        )

        session.add(routine)

    except Exception as e:
        print(f"Row {i} skipped due to error: {e}")

# 5. Commit and close
session.commit()
session.close()

print("end")