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
        """Count the number of null values in each column."""
        try:
            logging.info("Calculating the number of null values per column.")
            return self.df.isnull().sum()
        except Exception as e:
            logging.error(f"Failed to calculate null values: {e}")
            return None

    def imputate_null_values(self):
        """Imputate null values using forward fill."""
        try:
            self.df.fillna(method='ffill', inplace=True)
            logging.info("Successfully imputated null values.")
        except Exception as e:
            logging.error(f"Failed to imputate null values: {e}")

    def remove_duplicates(self):
        """Remove duplicate rows."""
        try:
            initial_row_count = len(self.df)
            self.df = self.df.drop_duplicates(keep='first')
            logging.info(f"Removed duplicates. Rows reduced from {initial_row_count} to {len(self.df)}.")
        except Exception as e:
            logging.error(f"Failed to remove duplicates: {e}")

    def check_string_types(self):
        """Convert specific columns to string type."""
        try:
            cols_to_convert = ['crypto']
            for col in cols_to_convert:
                if col in self.df.columns:
                    self.df[col] = self.df[col].astype(str)
            logging.info("Successfully converted columns to string type.")
        except Exception as e:
            logging.error(f"Failed to convert columns to string: {e}")

    def convert_to_float(self):
        """Convert numeric columns to float type."""
        try:
            numeric_cols = ['current_price', 'price_change', 'high_price', 'low_price']
            existing_cols = [col for col in numeric_cols if col in self.df.columns]
            self.df[existing_cols] = self.df[existing_cols].apply(pd.to_numeric, errors='coerce').astype('Float64')
            logging.info("Successfully converted numeric columns to float type.")
        except Exception as e:
            logging.error(f"Failed to convert columns to float type: {e}")

    def convert_to_datetime(self):
        """Convert date columns to datetime."""
        try:
            date_cols = ['date']
            for col in date_cols:
                if col in self.df.columns:
                    self.df[col] = pd.to_datetime(self.df[col], errors='coerce')
            logging.info("Successfully converted columns to datetime type.")
        except Exception as e:
            logging.error(f"Failed to convert columns to datetime: {e}")

    def normalise_headers(self):
        """Normalize column headers by standardizing their format."""
        try:
            self.df.columns = ['_'.join(word.title() for word in column.split("_")) for column in self.df.columns]
            self.df = self.df.reset_index(drop=True)
            logging.info("Successfully normalized column headers.")
        except Exception as e:
            logging.error(f"Failed to normalize headers: {e}")

    def save_changes(self, cleaned_data_path='./saved_data/saved_data.csv'):
        """Save the cleaned data to a CSV file."""
        import os
        try:
            directory = os.path.dirname(cleaned_data_path)
            if not os.path.exists(directory):
                os.makedirs(directory)
                logging.info(f"Created directory: {directory}")
            
            file_exists = os.path.exists(cleaned_data_path) and os.path.getsize(cleaned_data_path) > 0
            self.df.to_csv(cleaned_data_path, mode='a', index=False, header=not file_exists, encoding='utf-8')
            logging.info(f"Data saved successfully to {cleaned_data_path}. Header written: {not file_exists}")
        except Exception as e:
            logging.error(f"Failed to save data: {e}")