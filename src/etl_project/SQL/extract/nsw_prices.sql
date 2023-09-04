{% set config = {
    "extract_type": "incremental", 
    "incremental_column": "last_update",
    "source_table_name": "nsw_fuel_price",
} %}

select 
    id,
    station_code, 
    fuel_type, 
    price,
    last_update 
from 
    {{ config["source_table_name"] }}

{% if is_incremental %}
    where {{ config["incremental_column"] }} > '{{ incremental_value }}'
{% endif %}