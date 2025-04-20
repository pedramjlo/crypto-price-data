import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()  # Load variables from .env

user = os.getenv('DB_USER')
password = os.getenv('DB_PASS')
dbname = os.getenv('DB_NAME')

df = pd.read_csv('../saved_data/saved_data.csv')

engine = create_engine(f"postgresql://{user}:{password}@localhost/{dbname}")
df.to_sql('crypto_data', engine, if_exists='append', index=False)



