from extraction.binanceData import DataExtraction
from transformation.dataConvert import DataTranformation


from config.settings import BINANCE_PRICE_URL, BINANCE_INFO_URL, BINANCE_PRICE_URL # all the url endpoints


if __name__ == "__main__":

    # Extraction
    extractor = DataExtraction(BINANCE_PRICE_URL, BINANCE_INFO_URL, BINANCE_PRICE_URL)
    
    cryptos = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]
    data = extractor.daily_crypto_price(cryptos=cryptos)

    # Transformation
    if data:
        transformer = DataTranformation(daily_price_info=data)
        df = transformer.data_to_dataframe()  # Wrap single JSON in list

        # Step 3: Print or Save the Cleaned Data
        print(df)
