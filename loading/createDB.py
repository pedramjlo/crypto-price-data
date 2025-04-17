import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

try:
    # Get credentials from environment variables
    user = os.getenv('DB_USER')
    password = os.getenv('DB_PASS')
    host = 'localhost'
    port = 5432
    dbname = 'crypto_data'  # The name of the database you want to create

    # Connect to the default 'postgres' database
    conn = psycopg2.connect(dbname='postgres', user=user, password=password, host=host, port=port)
    conn.autocommit = True  # Enable autocommit to allow database creation
    cur = conn.cursor()

    # Check if the database already exists
    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (dbname,))
    exists = cur.fetchone()

    if not exists:
        # Create the database if it doesn't exist
        cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dbname)))
        print(f"Database '{dbname}' created successfully.")
    else:
        print(f"Database '{dbname}' already exists.")

    cur.close()
    conn.close()

except psycopg2.Error as e:
    print(f"Error creating database: {e}")
