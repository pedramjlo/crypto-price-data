import os
import logging

from extraction.dataExtraction import BinanceDataExtraction
from transformation.dataConvert import DataConvert
from transformation.dataCleaner import DataCleaner

import pandas as pd

logging.basicConfig(
    level=logging.INFO,  # Set the minimum log level
    format='%(asctime)s - %(levelname)s - %(message)s',  # Log message format
    # filename='app.log'  # Log to a file (optional)
)


from config.settings import BINANCE_PRICE_URL, BINANCE_TEST_URL, BINANCE_PRICE_URL # all the url endpoints


if __name__ == "__main__":

    # Extraction
    CSV_PATH = os.path.join("loading/saved_data", "binance_data.csv")
    table_name = "crypto_data"
    cryptos = ["BTCUSDT", "ETHUSDT"]
    interval = "1d"
    start_time = "2020-01-01 00:00:00"
    end_time = "2025-04-01 00:00:00"

    
    binance_to_pg = BinanceDataExtraction(connection_test_endpoint=BINANCE_TEST_URL, binance_prices_endpoint=BINANCE_PRICE_URL)
    connection_response = binance_to_pg.test_connection()
    # exchange_info = binance_to_pg.exchange_information(exchange_info_url=BINANCE_TEST_URL)

    binance_data = binance_to_pg.get_binance_data(binance_price_endpoint=BINANCE_PRICE_URL, cryptos=cryptos, start_time=start_time, end_time=end_time, interval='1d')
    df = pd.DataFrame(binance_data)

    data = binance_to_pg.get_binance_data(
        binance_price_endpoint=BINANCE_PRICE_URL,
        cryptos=cryptos,
        start_time=start_time,
        end_time=end_time,
        interval=interval
    )

    if data:
        df = pd.DataFrame(data)
        
        binance_to_pg.save_data_to_csv(
        csv_file_path=CSV_PATH,
        fieldnames=df.columns.tolist(),
        data=data
        )

    # CLEANER
    cleaner = DataCleaner(df=df)

    cleaner.sum_null_values()
    cleaner.imputate_null_values()
    cleaner.remove_duplicates()

    cleaner.check_string_types()
    cleaner.convert_to_float()
    cleaner.convert_to_datetime()
    cleaner.normalise_headers()

    cleaner.save_changes()



    



    
        






