
SELECT * FROM crypto_data;



SELECT MIN("timestamp") as "early_date", MAX("timestamp") as "late_date"
FROM crypto_data;


SELECT EXTRACT(YEAR FROM "timestamp") AS year
FROM crypto_data;


WITH TIMESTAMP_TO_DATE AS (
    SELECT 
        "crypto",
        DATE("timestamp") as "date"
    FROM crypto_data
)
SELECT 
    DISTINCT "date"
FROM TIMESTAMP_TO_DATE
ORDER BY "date" ASC;



WITH HIGHEST_PRICE_CHANGE AS (
    SELECT 
        "crypto",
        ("close_price" - "open_price") as "price_change",
        DATE(timestamp) as date
    FROM crypto_data
    GROUP BY "crypto"
)
SELECT
    crypto,
    MAX(price_change) AS highest_change,
    date
FROM HIGHEST_PRICE_CHANGE
GROUP BY crypto, date
ORDER BY highest_change DESC
LIMIT 1;


news_api=9a4b1be4d6c44333a3a193bdd6c7520c