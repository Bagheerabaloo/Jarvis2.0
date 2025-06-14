import logging
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from src.common.file_manager.FileManager import FileManager

# __ init file manager __
config_manager = FileManager()
postgre_url = config_manager.get_postgre_url("POSTGRE_URL_LOCAL_SCHEDULE")

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
