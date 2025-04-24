# The Introduction
API app to extract crypto data from Binance.

# The Pipeline

1- extraction.BinanceDataExtraction.get_binance_data() extracts crypto data from a Binance API endpoint along with some paramters and saves it into a saved_data.csv file;
<ui>
  <li>crypto</li>
  <li>starte date (unix epoch milliseconds)</li>
  <li>interval</li>
  <li>end date (unix epoch milliseconds)</li>
  <li>keywords</li>
</ui>

<br>

2- transformation.DataCleaner class contains method to clean the extracted Binance data and saves the cleaned data in the binance_data.csv.
<ul>
  <li>Sum of null value rows</li>
  <li>Imputation of null values</li>
  <li>Removal of duplicate rows</li>
  <li>Check for string, float, and date datatypes</li>
  <li>Save the cleaned dataset in a new file</li>
</u>

<br>

3- loading.createDB file exclusively created a PostgreSQL databse if it doesn't exist.

<br>

4- loading.loadToDB.load_csv_to_postgres() loads the cleaned data from binance_data.csv to the PostgreSQL table
