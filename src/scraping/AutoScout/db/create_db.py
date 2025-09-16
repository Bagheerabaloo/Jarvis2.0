from sqlalchemy import text
from src.scraping.AutoScout.db.database import engine, Base  # Import the engine and Base class from your database setup
from src.scraping.AutoScout.db.models import *

# Check registered tables
print("Registered tables before creating:", Base.metadata.tables.keys())

# Create all tables defined in the Base's metadata in the database
# Base.metadata.clear()
Base.metadata.create_all(bind=engine)

# Check registered tables after creating
print("Registered tables after creating:", Base.metadata.tables.keys())

# Print confirmation message
print("Database and tables created successfully.")

# List all tables in the database
with engine.connect() as connection:
    result = connection.execute(text("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"))
    for row in result:
        print("     Table:", row[0])