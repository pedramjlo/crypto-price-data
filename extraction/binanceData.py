import requests
import logging



logging.basicConfig(
    level=logging.INFO,  # Set the minimum log level
    format='%(asctime)s - %(levelname)s - %(message)s',  # Log message format
    # filename='app.log'  # Log to a file (optional)
)








class DataExtraction:
    def __init__(self, binance_test_endpoint, exchange_info_url, data_params=None):
        self.binance_test_endpoint = binance_test_endpoint
        self.exchange_info_url = exchange_info_url
        self.data_params = data_params





    def test_connection(self):
        try:
            response = requests.get(self.binance_test_endpoint, params=self.data_params)
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
        


if __name__ == "__main__":

    binance_test_endpoint = 'https://api.binance.com/api/v3/ping'

    exchange_info_url = 'https://api.binance.com/api/v3/exchangeInfo'

    extractor = DataExtraction(binance_test_endpoint, exchange_info_url)



    print(extractor.test_connection())

    print(extractor.exchange_information())

