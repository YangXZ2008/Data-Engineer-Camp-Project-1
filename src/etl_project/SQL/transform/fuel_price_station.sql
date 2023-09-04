SELECT nfp.station_code,
nfp.price,
nfp.fuel_type,
ns.name,
ns.address,
nfp.last_updated,
CAST(ns.lat AS varchar) || ' ' || CAST(ns.long AS varchar) AS location,
AVG(nfp.price) OVER(PARTITION BY nfp.fuel_type) AS avg_fuel_price_by_category,
AVG(nfp.price) OVER(PARTITION BY nfp.last_updated) AS avg_fuel_price_by_datetime,
RANK() OVER(PARTITION BY nfp.fuel_type ORDER BY nfp.price desc) AS rank_fuel_by_catagory,
RANK() OVER(PARTITION BY nfp.station_code ORDER BY nfp.price desc) AS rank_fuel_by_station
FROM nsw_fuel_price nfp
INNER JOIN nsw_stations ns ON ns.station_code = nfp.station_code 
ORDER BY nfp.station_code;