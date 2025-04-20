from extraction.dataExtraction import DataExtraction
from transformation.dataConvert import DataConvert
from transformation.dataCleaner import DataCleaner


from config.settings import BINANCE_PRICE_URL, BINANCE_INFO_URL, BINANCE_TEST_URL # all the url endpoints


if __name__ == "__main__":

    # Extraction
    extractor = DataExtraction(BINANCE_PRICE_URL, BINANCE_INFO_URL, BINANCE_PRICE_URL)
    
    cryptos = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]
    data = extractor.daily_crypto_price(cryptos=cryptos, interval='1d', start_time='2020-01-01 07:00:00', end_time='2023-01-01 16:00:00')


    global df
    df = None

    # Convert To DataFrame
    if data:
        transformer = DataConvert(daily_price_info=data)
        df = transformer.data_to_dataframe()  # Wrap single JSON in list

        # Step 3: Print or Save the Cleaned Data
        # print(df)

    
    # Cleaning
    if df is not None:
        cleaner = DataCleaner(df=df)
        cleaner.sum_null_values()
        cleaner.imputate_null_values()
        cleaner.remove_duplicates()
        
        cleaner.check_string_types()
        cleaner.convert_to_float()
        cleaner.convert_to_datetime()

        cleaner.normalise_headers()


        cleaner.save_changes()

        

        # print(cleaner.normalise_headers())
        


