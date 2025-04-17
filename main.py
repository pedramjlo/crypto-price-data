from extraction.binanceData import DataExtraction
from transformation.convertToDataFrame import DataConvert


from config.settings import BINANCE_PRICE_URL, BINANCE_INFO_URL, BINANCE_PRICE_URL # all the url endpoints


if __name__ == "__main__":

    # Extraction
    extractor = DataExtraction(BINANCE_PRICE_URL, BINANCE_INFO_URL, BINANCE_PRICE_URL)
    data = extractor.daily_crypto_price(crypto='BNBBTC')

    # Transformation
    if data:
        transformer = DataConvert(daily_price_info=data)
        df = transformer.data_to_dataframe()  # Wrap single JSON in list

        # Step 3: Print or Save the Cleaned Data
        print(df)
