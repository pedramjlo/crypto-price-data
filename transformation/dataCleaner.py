import pandas as pd

import logging

logging.basicConfig(
    level=logging.INFO,  # Set the minimum log level
    format='%(asctime)s - %(levelname)s - %(message)s',  # Log message format
    # filename='app.log'  # Log to a file (optional)
)


class DataCleaner:
    def __init__(self, df):
        self.df = df


    def sum_null_values(self):
        try:
            logging.info("Got the number of null values per rows")
            return self.df.isnull().sum()
        except Exception as e:
            logging.error(f"Failed to get the number of null values: {e}")
            return None


    def imputate_null_values(self):
        try:
            self.df.fillna(method='ffill', inplace=True)  
            logging.info("Successfully imputated rows")
        
        except Exception as e:
            logging.error(f"Failed to imputate rows: {e}")
            return None
        
        
    def remove_duplicates(self):
        try:
            logging.info(f"Number of rows before removing duplicates: {len(self.df)}")
            # Remove duplicate rows based on all columns
            self.df = self.df.drop_duplicates(keep='first')
            logging.info(f"Number of rows after removing duplicates: {len(self.df)}")
        except Exception as e:
            logging.error(f"Failed to remove some duplicate values: {e}")

        return self.df

    def normalise_headers(self):
        try:
            self.df.columns = ['_'.join(word.title() for word in column.split("_")) for column in self.df.columns]
            self.df = self.df.reset_index(drop=True)
            logging.info("Successfully normalized column headers.")
        except Exception as e:
            logging.error(f"Failed to normalize column headers: {e}")
        return self.df