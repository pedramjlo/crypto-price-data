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
    

    def check_string_types(self):
        try:
            cols_to_convert = ['crypto']
            self.df[cols_to_convert] = self.df[cols_to_convert].astype(str)
            logging.info("Successfully converted specified columns to string type")
        except Exception as e:
            logging.error(f"Failed to convert some columns into strings: {e}")
        return self.df
    

    def convert_to_integer(self):
        try:
            numeric_cols = ['current_price', 'price_change', 'high_price', 'low_price']
            self.df[numeric_cols] = self.df[numeric_cols].apply(pd.to_numeric, errors='coerce').astype('Float64')
            logging.info("Successfully converted specified columns to float type")
        except Exception as e:
            logging.error(f"Failed to convert some columns into float: {e}")
        return self.df
    

    def convert_to_datetime(self):
        try:
            date_cols = ['date']
            for col in date_cols:
                self.df[col] = pd.to_datetime(self.df[col], errors='coerce')  # Converts invalid values to NaT
            logging.info("Successfully converted specified columns to datetime type.")
        except Exception as e:
            logging.error(f"Failed to convert some columns into datetime: {e}")
        return self.df


    



    def normalise_headers(self):
        try:
            self.df.columns = ['_'.join(word.title() for word in column.split("_")) for column in self.df.columns]
            self.df = self.df.reset_index(drop=True)
            logging.info("Successfully normalized column headers.")
        except Exception as e:
            logging.error(f"Failed to normalize column headers: {e}")
        
        return self.df
    

    

