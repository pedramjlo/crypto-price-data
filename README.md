# The Introduction
API app to extract crypto data from Binance.

# The Pipeline

1- extraction.BinanceDataExtraction.get_binance_data() extracts crypto data from a Binance API endpoint along with some paramters and saves it into a saved_data.csv file;

  - crypto
  - starte date (unix epoch milliseconds)
  - interval
  - end date (unix epoch milliseconds)
  - keywords

<br>

2- transformation.DataCleaner class contains method to clean the extracted Binance data and saves the cleaned data in the binance_data.csv.

  - Sum of null value rows
  - Imputation of null values
  - Removal of duplicate rows
  - Check for string, float, and date datatypes
  - Save the cleaned dataset in a new file

<br>

3- loading.createDB file exclusively created a PostgreSQL databse if it doesn't exist.

<br>

4- loading.loadToDB.load_csv_to_postgres() loads the cleaned data from binance_data.csv to the PostgreSQL table
