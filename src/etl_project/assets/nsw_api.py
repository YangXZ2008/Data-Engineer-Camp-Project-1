import pandas as pd
from datetime import datetime, timezone, timedelta
from pathlib import Path
from sqlalchemy import Table, MetaData
from etl_project.connectors.postgresql import PostgreSqlClient
from etl_project.connectors.nsw_api import NswApiClient
import uuid

def extract(api_client: NswApiClient):
    prices_data = []
    stations_data = []

    if api_client.get("prices") is not None and api_client.get("stations") is not None: 
        prices_data.extend(api_client.get("prices"))
        stations_data.extend(api_client.get("stations"))

    prices_df = pd.json_normalize(data=prices_data)
    stations_df = pd.json_normalize(data=stations_data)
    return prices_df, stations_df

def transform_2_df(prices_df: pd.DataFrame, stations_df: pd.DataFrame) -> pd.DataFrame:

    #prices
    prices_df.rename(columns={
    "stationcode":"station_code",
    "fueltype": "fuel_type",
    "lastupdated": "last_updated"
    }, inplace=True)
    prices_df['station_code'] = prices_df['station_code'].astype(int)
    prices_df["id"] = [uuid.uuid4().hex[:16] for _ in range(len(prices_df))] #add unique id for each row

    #stations
    stations_df.drop(columns=['brandid','stationid','brand'], inplace=True)
    stations_df.rename(columns={
        "location.latitude":"lat",
        "location.longitude":"long",
        'code':'station_code'
    }, inplace=True)
    stations_df['station_code'] = stations_df['station_code'].astype('int')

    return prices_df, stations_df

def load(df: pd.DataFrame, pg_client:PostgreSqlClient, table, metadata, loadtype:str):
    pg_client.write_to_table(data=df.to_dict(orient="records"), table=table, metadata=metadata, loadtype=loadtype)  
      