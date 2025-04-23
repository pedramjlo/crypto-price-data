import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
from urllib.parse import quote
import logging


logging.basicConfig(
    level=logging.INFO,  # Set the minimum log level
    format='%(asctime)s - %(levelname)s - %(message)s',  # Log message format
    # filename='app.log'  # Log to a file (optional)
)


load_dotenv()  





def load_csv_to_postgres():
    user = os.getenv('DB_USER')
    password = quote(os.getenv('DB_PASS'))
    dbname = os.getenv('DB_NAME')
    try:
        csv_file = './saved_data/binance_data.csv'
        df = pd.read_csv(csv_file)
        engine = create_engine(f"postgresql://{user}:{password}@localhost/{dbname}")
        df.to_sql('crypto_data', engine, if_exists='append', index=False)
        logging.info("Successfully loaded the CSV data into the database")
    except Exception as e:
            logging.error(f'Failed to load the data into the database, {e}')



