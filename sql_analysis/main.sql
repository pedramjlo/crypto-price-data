SELECT * FROM crypto_data_table;


SELECT "Crypto", MAX("Price_Change") as "Highest_Price_Change"
FROM crypto_data
GROUP BY "Crypto";

