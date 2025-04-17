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
    

    def convert_to_float(self):
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
    

    def save_changes(self, cleaned_data_path='./saved_data/saved_data.csv'):
        import os
        import pandas as pd
        
        try:
            # Ensure directory exists
            directory = os.path.dirname(cleaned_data_path)
            if not os.path.exists(directory):
                os.makedirs(directory)
                logging.info(f"Created directory: {directory}")

            # Check if file exists and has content
            file_exists = os.path.exists(cleaned_data_path) and os.path.getsize(cleaned_data_path) > 0
            
            # Save with conditional header
            self.df.to_csv(
                cleaned_data_path,
                mode='a',          # Append mode
                index=False,       # No index column
                header=not file_exists,  # Write header only if file doesn't exist or is empty
                encoding='utf-8'    # Explicit encoding
            )
            logging.info(f"Data saved successfully. Header written: {not file_exists}")
            
        except Exception as e:
            logging.error(f"Failed to save data: {e}")
            raise  # Re-raise exception after logging
        return self.df