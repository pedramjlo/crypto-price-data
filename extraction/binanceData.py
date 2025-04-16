import requests
import logging



logging.basicConfig(
    level=logging.INFO,  # Set the minimum log level
    format='%(asctime)s - %(levelname)s - %(message)s',  # Log message format
    # filename='app.log'  # Log to a file (optional)
)








class DataExtraction:
    def __init__(self, binance_api_path, data_params=None):
        self.binance_api_path = binance_api_path
        self.data_params = data_params





    def test_connection(self):
        try:
            response = requests.get(self.binance_api_path, params=self.data_params)
            response.raise_for_status()  
            logging.info('Successfully connected to Binance')
            return response.json()
        except Exception as e:
            logging.error(f'Failed to connect to Binance, {e}')
        


if __name__ == "__main__":

    binance_api_path = 'https://api.binance.com/api/v3/ping'

    extractor = DataExtraction(binance_api_path)

    print(extractor.test_connection())

