import requests
import logging
from datetime import datetime, time
import pytz
from retry import retry


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


    def convert_date_to_miliseconds(self, date):
        from datetime import datetime
        try:
            """
            These timestamps are represented in milliseconds since the Unix epoch (January 1, 1970). 
            This is a widely-used format known as Unix time or epoch time.
            """
            date_in_miliseconds = int(datetime.strptime(date, "%Y-%m-%d %H:%M:%S").timestamp() * 1000)
            logging.info("Successfully convert date to UNIX")
            return date_in_miliseconds
        except Exception as e:
            logging.error(f'Failed to convert date to UNIX (miliseconds) format, {e}')



    @retry(tries=5, delay=1, backoff=2, exceptions=(ConnectionResetError, requests.exceptions.RequestException))
    def daily_crypto_price(self, cryptos, interval, start_time, end_time):
        """
        Retrieve daily price info for a list of cryptocurrencies with robust error handling
        """
        results = []

        for crypto in cryptos:
            params = {'symbol': crypto, 
                      'interval': interval, 
                      'startTime': self.convert_date_to_miliseconds(start_time), 
                      'endTime': self.convert_date_to_miliseconds(end_time)
            }
            try:
                response = requests.get(self.daily_price_url, params=params)
                
                # Check if response contains valid JSON data
                try:
                    data = response.json()
                except ValueError:
                    logging.error(f'Invalid JSON response for {crypto}')
                    continue
                    
                # Validate required fields exist in response
                required_fields = ['symbol', 'lastPrice', 'priceChangePercent', 
                                'highPrice', 'lowPrice', 'interval', 'startTime', 'endTime']
                required_indices = [1, 2, 3, 4, 5]
                if not all(isinstance(data[0][i], (int, float, str)) for i in required_indices):
                    logging.error(f'Missing required fields in response for {crypto}')
                    continue
                # Validate numeric fields are actually numbers
                try:
                    price_data = {
                        "crypto": str(data["symbol"]),
                        "current_price": float(data["lastPrice"]),
                        "price_change": float(data["priceChangePercent"]),
                        "high_price": float(data["highPrice"]),
                        "low_price": float(data["lowPrice"]),
                        "date": f"{self.get_eastern_time()}"
                    }
                    results.append(price_data)
                    logging.info(f'Successfully retrieved valid data for {crypto}')
                    
                except (ValueError, TypeError) as e:
                    logging.error(f'Invalid numeric data for {crypto}: {e}')
                    continue
                    
            except requests.exceptions.RequestException as e:
                logging.error(f'Request failed for {crypto}: {e}')
                continue
            except Exception as e:
                logging.error(f'Unexpected error for {crypto}: {e}')
                continue

        return results