import requests
import logging
from datetime import datetime, time
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
        - this function is to check strictly if the requests are specifically are sent at 4PM the ET
        - checking if currently the time in Eastern time is 4PM (the market's closing time)
        """
        try:
            ny_tz = pytz.timezone('America/New_York')
            current_time_ny = datetime.now(ny_tz)
            formatted_datetime = current_time_ny.strftime("%Y-%m-%d %H:%M:%S")
            logging.info('The Eastern time received')
            return formatted_datetime
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

    ######################################

    def daily_crypto_price(self, cryptos):
        """
        Retrieve daily price info for a list of cryptocurrencies
        
        """
        results = []  # Store results for multiple cryptos

        for crypto in cryptos:
            params = {'symbol': crypto}  # Update symbol for each request
            try:
                response = requests.get(self.daily_price_url, params=params)
                response.raise_for_status()
                logging.info(f'Successfully retrieved data for {crypto}')
                data = response.json()
                results.append({
                    "crypto": data["symbol"],
                    "current_price": data["lastPrice"],
                    "price_change": data["priceChangePercent"],
                    "high_price": data["highPrice"],
                    "low_price": data["lowPrice"],
                    "date": f"{self.get_eastern_time()}"
                })
            except Exception as e:
                logging.error(f'Failed to retrieve price for {crypto}, {e}')
                results.append({"crypto": crypto, "error": str(e)})  # Log errors for specific symbols

        return results  # Return list of results
