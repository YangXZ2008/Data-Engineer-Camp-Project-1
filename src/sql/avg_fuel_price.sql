CREATE TABLE fuel_avg_prices_per_date (
    date DATE,
    fuel_type VARCHAR(255),
    average_price DECIMAL(10, 2)
);
INSERT INTO fuel_avg_prices_per_date (date, fuel_type, average_price)
SELECT
    TO_DATE(SUBSTRING(last_updated, 1, 10), 'dd/mm/yyyy') AS date,
    fuel_type,
    AVG(CAST(price AS DECIMAL(10, 2))) AS average_price
FROM
    fuel_prices
GROUP BY
    date,
    fuel_type;