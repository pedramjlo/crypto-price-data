SELECT * FROM crypto_data;


SELECT "Crypto", MAX("Price_Change") as "Highest_Price_Change"
FROM crypto_data
GROUP BY "Crypto";

