import logging
import os
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from src.common.file_manager.FileManager import FileManager

# __ init file manager __
if os.environ.get("RUN_ENV", "").lower() == "raspberry":
    # Raspberry/Linux usa variabili d'ambiente
    postgre_url = os.environ.get("POSTGRE_URL_LOCAL_AUTOSCOUT")
else:
    # PC Windows continua a usare i file config (TXT)
    config_manager = FileManager()
    # postgre_url = config_manager.get_postgre_url("POSTGRE_URL_LOCAL_AUTOSCOUT")
    postgre_url = config_manager.get_postgre_url("POSTGRE_URL_RASPBERRY_AUTOSCOUT")

# Configure SQLAlchemy logging
# logging.basicConfig()
# logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

# Create the connection engine
engine = create_engine(postgre_url)
# engine.echo = True  # Set to True for debugging

# Create a base class for our ORM classes
Base = declarative_base()

# Create a configured session class to interact with the database
session_local = sessionmaker(autocommit=False, autoflush=False, bind=engine)
