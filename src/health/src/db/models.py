from _decimal import Decimal
from sqlalchemy import Column, Integer, String, Text, Boolean, Numeric, ForeignKey, Index, UniqueConstraint, Time
from sqlalchemy import Float, BigInteger, Date, DateTime, UUID as SA_UUID
from sqlalchemy.orm import relationship
from src.health.src.db.database import Base


class DailyRoutine(Base):
    __tablename__ = 'daily_routine'

    date = Column(Date, primary_key=True)
    day_of_week = Column(String)

    weight = Column(Integer, nullable=True)
    water_morning = Column(Integer, nullable=True)
    workout_morning = Column(Integer, nullable=True)
    made_bed = Column(Integer, nullable=True)
    washed_face = Column(Integer, nullable=True)
    breakfast = Column(Integer, nullable=True)
    set_tasks = Column(Integer, nullable=True)

    cream_applied = Column(Integer, nullable=True)
    fruit = Column(Integer, nullable=True)
    vegetables = Column(Integer, nullable=True)
    dried_fruit = Column(Integer, nullable=True)
    fitness_ring = Column(Integer, nullable=True)

    water_day_1 = Column(Integer, nullable=True)
    water_day_2 = Column(Integer, nullable=True)
    workout_day = Column(Integer, nullable=True)
    stretching = Column(Integer, nullable=True)
    cold_shower = Column(Integer, nullable=True)

    water_evening = Column(Integer, nullable=True)
    three_positive_things = Column(Integer, nullable=True)
    mood = Column(Integer, nullable=True)
    set_tasks_completed = Column(Integer, nullable=True)
    planning = Column(Integer, nullable=True)
    wishes_101 = Column(Integer, nullable=True)
    breakfast_set = Column(Integer, nullable=True)
    reading = Column(Integer, nullable=True)
    music = Column(Integer, nullable=True)

    bedtime = Column(Float, nullable=True)  # Time in decimal format starting from 21:00
    wakeup_time = Column(Float, nullable=True) # Time in decimal format starting from 21:00
    sleep_score = Column(Integer, nullable=True)
    sleep_duration = Column(String, nullable=True)
    heart_rate_min = Column(Integer, nullable=True)
    heart_rate_rest = Column(Integer, nullable=True)
    weather = Column(String, nullable=True)

    morning_routine_end_time = Column(Float, nullable=True) # Time in decimal format starting from 21:00
    workout_end_time = Column(Float, nullable=True) # Time in decimal format starting from 21:00

    smart = Column(Integer, nullable=True)
    tasks_set = Column(Integer, nullable=True)
    tasks_done = Column(Integer, nullable=True)

    routine_morning = Column(Integer, nullable=True)
    routine_day = Column(Integer, nullable=True)
    routine_evening = Column(Integer, nullable=True)
    routine_workout = Column(Integer, nullable=True)
    routine_water = Column(Integer, nullable=True)

    bdnf = Column(Integer, nullable=True)


    def __repr__(self):
        return f"<DailyRoutine(date={self.date})>"

