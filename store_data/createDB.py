import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv
import logging


load_dotenv()



logging.basicConfig(
    level=logging.INFO,  # Set the minimum log level
    format='%(asctime)s - %(levelname)s - %(message)s',  # Log message format
    # filename='app.log'  # Log to a file (optional)
)



# Configuration variables
db_config = {
    "dbname": os.getenv('DB_NAME'),
    "user": os.getenv('DB_USER'),
    "password": os.getenv('DB_PASS'),
    "host": os.getenv('DB_HOST'),
    "port": 5432
}

csv_file_path = "../saved_data/saved_data.csv"

# Connect to PostgreSQL
def connect_to_postgres(config):
    try:
        conn = psycopg2.connect(
        dbname=config["dbname"],
        user=config["user"],
        password=config["password"],
        host=config["host"],
        port=config["port"]
        )
        logging.info("Successfully connected to PostgreSQL")
        return conn
    except Exception as e:
        logging.error(f"Failed to connect to the DB, {e}")
        return None

# Create database if not exists
def create_database(db_name, config):
    connection = psycopg2.connect(
        dbname=config["dbname"],  # Default database
        user=config["user"],
        password=config["password"],
        host=config["host"],
        port=config["port"]
    )
    message = ""
    try:
        connection.autocommit = True
        cursor = connection.cursor()
        cursor.execute(f"SELECT 1 FROM pg_database WHERE datname='{db_name}'")
        exists = cursor.fetchone()
        message = "The database already exists"
        if not exists:
            cursor.execute(f"CREATE DATABASE {db_name}")
            message = "The database didn't exists but was created"
        cursor.close()
        connection.close()
        logging.info("Connection closed. Success!")
        return message

    except Exception as e:
        logging.error(f"Failed to create the database, {e}")
        return None

# Create table dynamically based on CSV
def create_table(conn, table_name, csv_data):
    try:
        cursor = conn.cursor()
        columns = ", ".join([f"{col} TEXT" for col in csv_data.columns])
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {columns}
        )
        """
        cursor.execute(create_table_query)
        conn.commit()
        cursor.close()
        logging.info("Successfully created the table.")
    except Exception as e:
        logging.error(f"Failed to create the table based on the CSV content, {e}")

# Insert data into the table
def insert_data(conn, table_name, csv_data):
    try:
        cursor = conn.cursor()
        for _, row in csv_data.iterrows():
            values = ", ".join([f"'{val}'" for val in row])
            insert_query = f"INSERT INTO {table_name} VALUES ({values})"
            cursor.execute(insert_query)
        conn.commit()
        cursor.close()
        logging.info(f"Successfully inserted the CSV content")
    except Exception as e:
        logging.error(f"Failed to insert the CSV content, {e}")

def main():
    # Create and connect to the database
    create_database(db_config["dbname"], db_config)
    conn = connect_to_postgres(db_config)


    csv_data = pd.read_csv(csv_file_path)
    table_name = "crypto_data_table"

    # Create table and insert data
    create_table(conn, table_name, csv_data)
    insert_data(conn, table_name, csv_data)
    conn.close() # connection must be closed after every database modification

if __name__ == "__main__":
    main()
