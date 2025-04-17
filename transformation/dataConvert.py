import pandas as pd

import logging


logging.basicConfig(
    level=logging.INFO,  # Set the minimum log level
    format='%(asctime)s - %(levelname)s - %(message)s',  # Log message format
    # filename='app.log'  # Log to a file (optional)
)



class DataConvert:
    def __init__(self, daily_price_info):
        self.daily_price_info = daily_price_info


    def data_to_dataframe(self):
        try:
            df = pd.DataFrame(self.daily_price_info)
    
            logging.info("Successfully converted JSON data to Pandas DataFrame.")
            return df
        except Exception as e:
            logging.error(f"Failed to convert JSON data to Pandas DataFrame: {e}")
            return None




