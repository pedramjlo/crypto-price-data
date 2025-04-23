import os
import csv
import logging 
import pandas as pd 
import requests

from datetime import datetime

logging.basicConfig(
    level=logging.INFO,  # Set the minimum log level
    format='%(asctime)s - %(levelname)s - %(message)s',  # Log message format
    # filename='app.log'  # Log to a file (optional)
)



class BinanceDataExtraction:
    def __init__(self, connection_test_endpoint, binance_prices_endpoint):
        self.binance_prices_endpoint = binance_prices_endpoint
        self.connection_test_endpoint = connection_test_endpoint



        

    def test_connection(self):
        try:
            response = requests.get(self.connection_test_endpoint)
            response.raise_for_status()  
            logging.info(f'Successfully connected to Binance, {response.status_code}')
            return response.status_code
        except Exception as e:
            logging.error(f'Failed to connect to Binance, {e}')


    def exchange_information(self, exchange_info_url):
        try:
            response = requests.get(exchange_info_url)
            response.raise_for_status()  
            logging.info('Retrieved exchange inforamtion successfully')
            return response.status_code
        except Exception as e:
            logging.error(f'Failed to retrieve exchange information, {e}')


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


    



    def get_binance_data(self, binance_price_endpoint, cryptos, start_time, end_time, interval):
        results = []
        for crypto in cryptos:
            params = {
                "symbol": crypto,
                "interval": interval,
                "startTime": self.convert_date_to_miliseconds(start_time),
                "endTime": self.convert_date_to_miliseconds(end_time),
            }
            
            try:
                response = requests.get(binance_price_endpoint, params=params)
                response.raise_for_status()

                # Validate content type
                if 'application/json' not in response.headers.get('Content-Type', ''):
                    logging.error(f"Unexpected content type for {crypto}")
                    continue

                data = response.json()

                # Ensure valid response
                if not isinstance(data, list) or len(data) == 0:
                    logging.error(f"Invalid response for {crypto}")
                    continue

                # Extract candlestick data
                for candle in data:
                    try:
                        price_data = {
                            "crypto": crypto,
                            "open_price": float(candle[1]),
                            "high_price": float(candle[2]),
                            "low_price": float(candle[3]),
                            "close_price": float(candle[4]),
                            "volume": float(candle[5]),
                            "timestamp": datetime.fromtimestamp(candle[0] / 1000).strftime('%Y-%m-%d %H:%M:%S')
                        }
                        results.append(price_data)
                        logging.info(f"Extracted data: {price_data}")
                    except (ValueError, IndexError) as e:
                        logging.error(f"Error processing candle data for {crypto}: {e}")
                        continue

            except requests.exceptions.RequestException as e:
                logging.error(f"Request failed for {crypto}: {e}")
            except Exception as e:
                logging.error(f"Unexpected error for {crypto}: {e}")

        return results
    

    
    def save_data_to_csv(self, csv_file_path, fieldnames, data):
        try:
            # Ensure directory exists
            os.makedirs(os.path.dirname(csv_file_path), exist_ok=True)
            
            # Write to CSV
            file_exists = os.path.isfile(csv_file_path)
            with open(csv_file_path, 'w', newline='', encoding='utf-8') as csv_file:
                writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
                if not file_exists:
                    writer.writeheader()
                writer.writerows(data)  # Use writerows for bulk write
            logging.info(f"Data saved to {csv_file_path}")
        except Exception as e:
            logging.error(f"CSV save failed: {e}")
            raise