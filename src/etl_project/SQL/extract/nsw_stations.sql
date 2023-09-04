{% set config = {
    "extract_type": "full",
    "source_table_name": "nsw_stations"
} %}

select 
    name, 
    station_code,
    address,
    lat,
    long
from 
    {{ config["source_table_name"] }}