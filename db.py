import os
from dotenv import load_dotenv
import psycopg2

# Load environment variables from .env file
load_dotenv()

DB_NAME = os.getenv("DB_NAME", "default_db")
DB_USER = os.getenv("DB_USER", "default_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "default_password")
DB_HOST = os.getenv("DB_HOST", "localhost")

def get_db_connection():
    """
    Returns a new connection to the PostgreSQL database.
    """
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST
    )
