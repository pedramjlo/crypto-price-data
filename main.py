from extraction.binanceData import DataExtraction
from transformation.convertToDataFrame import DataConvert
from config.settings import BINANCE_PRICE_URL, BINANCE_INFO_URL, BINANCE_PRICE_URL
if __name__ == "__main__":

    # Step 1: Extraction
    extractor = DataExtraction(BINANCE_PRICE_URL, BINANCE_INFO_URL, BINANCE_PRICE_URL)
    data = extractor.daily_crypto_price(crypto='BNBBTC')

    # Step 2: Transformation
    if data:
        transformer = DataConvert()
        df = transformer.json_to_dataframe([data])  # Wrap single JSON in list
        cleaned_df = transformer.clean_dataframe(df)

        # Step 3: Print or Save the Cleaned Data
        print(cleaned_df)
