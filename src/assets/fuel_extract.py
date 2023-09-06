import pandas as pd


from connectors.fuel_api import FuelAPIClient
from connectors.postgres_client import PostgreSqlClient


def extract(fuel_client: FuelAPIClient):
    data = fuel_client.get_fuel_data()
    df_stations = pd.json_normalize(data["stations"])
    df_fuel_prices = pd.json_normalize(data["prices"])

    return df_stations, df_fuel_prices


def transform(df, table="station"):
    if table == "station":
        df_renamed = df[["code", "brand", "name", "address", "state", "location.latitude", "location.longitude"]].rename(columns={
            "code": "station_code",
            "location.latitude": "lat",
            "location.longitude": "lon"
        })

    else:  # fuel
        df_renamed = df[["stationcode", "state", "fueltype", "price", "lastupdated"]].rename(
            columns={
                "stationcode": "station_code",
                "fueltype": "fuel_type",
                "lastupdated": "last_updated"
            }
        )

    return df_renamed


def load(df_exchange: pd.DataFrame, postgresql_client: PostgreSqlClient, table, metadata):
    postgresql_client.write_to_table(data=df_exchange.to_dict(
        orient="records"), table=table, metadata=metadata)
