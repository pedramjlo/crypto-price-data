import requests
import logging
from datetime import datetime
import pytz


logging.basicConfig(
    level=logging.INFO,  # Set the minimum log level
    format='%(asctime)s - %(levelname)s - %(message)s',  # Log message format
    # filename='app.log'  # Log to a file (optional)
)




class DataExtraction:

    def __init__(self, binance_test_url, exchange_info_url, daily_price_url, data_params=None):
        self.binance_test_url = binance_test_url
        self.exchange_info_url = exchange_info_url
        self.daily_price_url = daily_price_url
        self.data_params = data_params


    def get_eastern_time(self):
        """ 
        - checking if currently the time in Eastern time is 4PM (the market's closing time)
        """
        try:
            ny_tz = pytz.timezone('America/New_York')
            current_time_ny = datetime.now(ny_tz)
            logging.info('The Eastern time received')
            return current_time_ny.time()
        except Exception as e:
            logging.error(f'Failed to get the Eastern time, {e}')





    def test_connection(self):
        try:
            response = requests.get(self.binance_test_url, params=self.data_params)
            response.raise_for_status()  
            logging.info('Successfully connected to Binance')
            return response.json()
        except Exception as e:
            logging.error(f'Failed to connect to Binance, {e}')



    def exchange_information(self):
        try:
            response = requests.get(self.exchange_info_url, params=self.data_params)
            response.raise_for_status()  
            logging.info('Retrieved exchange inforamtion successfully')
            return response.json()
        except Exception as e:
            logging.error(f'Failed to retrieve exhange information, {e}')



    def daily_crypto_price(self, crypto):
        request_time = datetime.now()

        params = {
            'symbol': crypto
        }
        try:
            response = requests.get(self.daily_price_url, params=params)
            response.raise_for_status()
            logging.info("Successfully connected to Binance")
            data = response.json()
            return {
                "current_price": data["lastPrice"],
                "price_change": data["priceChangePercent"],
                "high_price": data["highPrice"],
                "low_price": data["lowPrice"],
                "date": f"{request_time}"
            }
        except Exception as e:
            logging.error(f"Failed to retrieve the crypto price, {e}")
        

    ######################################

    def daily_crypto_price(self, crypto):
        request_time = datetime.now()

        params = {
            'symbol': crypto
        }
        
        if request_time == self.get_us_market_close_time:
            try:
                response = requests.get(self.daily_price_url, params=params)
                response.raise_for_status()
                logging.info('Successfully connected to Binance')
                data = response.json()
                return {
                    "current_price": data["lastPrice"],
                    "price_change": data["priceChangePercent"],
                    "high_price": data["highPrice"],
                    "low_price": data["lowPrice"],
                    "date": f"{request_time}"
                }
            except Exception as e:
                logging.error(f'Failed to retrieve BITCOIN price, {e}')
        else:
            logging.error("")
        


if __name__ == "__main__":

    binance_test_url = 'https://api.binance.com/api/v3/ping'

    exchange_info_url = 'https://api.binance.com/api/v3/exchangeInfo'

    daily_price_url = 'https://api.binance.com/api/v3/ticker/24hr'

    extractor = DataExtraction(binance_test_url, exchange_info_url, daily_price_url)



    print(extractor.test_connection())

    # print(extractor.exchange_information())


    # print(extractor.daily_crypto_price(crypto='BNBETH'))

    print(extractor.get_eastern_time())
