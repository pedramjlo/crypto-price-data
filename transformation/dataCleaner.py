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
            numeric_cols = ['current_price', 'price_change', 'high_price', 'low_price', 'volume']
            existing_cols = [col for col in numeric_cols if col in self.df.columns]
            self.df[existing_cols] = self.df[existing_cols].apply(pd.to_numeric, errors='coerce').astype('Float64')
            logging.info("Successfully converted numeric columns to float type.")
        except Exception as e:
            logging.error(f"Failed to convert columns to float type: {e}")

    def convert_to_datetime(self):
        """Convert date columns to datetime."""
        try:
            date_cols = ['timestamp']
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
            logging.info(f"Successfully normalized column headers. New headers: {[h for h in self.df.columns[:3]]} and etc.")
        except Exception as e:
            logging.error(f"Failed to normalize headers: {e}")


    def save_changes(self, cleaned_data_path='./saved_data/saved_data.csv'):
        """
        Save the cleaned data to the same CSV file. Creates the file if it doesn't exist.
        """
        try:
            import os
            
            # Ensure the directory exists
            os.makedirs(os.path.dirname(cleaned_data_path), exist_ok=True)

            # Save DataFrame to CSV
            self.df.to_csv(cleaned_data_path, index=False, encoding='utf-8')
            logging.info(f"Successfully saved cleaned data to {cleaned_data_path}.")
        except Exception as e:
            logging.error(f"Failed to save the cleaned data: {e}")
