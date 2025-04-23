import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
from urllib.parse import quote


load_dotenv()  

user = os.getenv('DB_USER')
password = quote(os.getenv('DB_PASS'))
dbname = os.getenv('DB_NAME')

csv_file = './saved_data/binance_data.csv'

df = pd.read_csv(csv_file)

engine = create_engine(f"postgresql://{user}:{password}@localhost/{dbname}")
df.to_sql('crypto_data', engine, if_exists='append', index=False)


