

SELECT * FROM crypto_data;


SELECT 
    "Crypto",
    "Price_Change"
FROM crypto_data
GROUP BY "Crypto", "Price_Change"
ORDER BY "Price_Change" DESC;



SELECT * 
FROM crypto_data 
WHERE "Current_Price" IS NULL OR "High_Price" IS NULL;






