import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
import os
import logging


logging.basicConfig(
    level=logging.INFO,  # Set the minimum log level
    format='%(asctime)s - %(levelname)s - %(message)s',  # Log message format
    # filename='app.log'  # Log to a file (optional)
)

load_dotenv()

try:
    # Get credentials from environment variables
    user = os.getenv('DB_USER')
    password = os.getenv('DB_PASS')
    host = os.getenv('DB_HOST')
    port = os.getenv('DB_PORT')
    dbname = os.getenv('DB_NAME')

    # Establish a connection to the DB
    conn = psycopg2.connect(dbname='postgres', user=user, password=password, host=host, port=port)
    conn.autocommit = True  # To allow database creation
    cur = conn.cursor()

    # Check if the database already exists
    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (dbname,))
    exists = cur.fetchone()

    if not exists:
        # Create the database if not  exists
        cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dbname)))
        logging.info(f"Database '{dbname}' created successfully.")
    else:
        logging.error(f"Database '{dbname}' already exists.")

    cur.close()
    conn.close()

except psycopg2.Error as e:
    logging.error(f"Error creating database: {e}")
