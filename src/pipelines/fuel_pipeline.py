from dotenv import load_dotenv
import os
from sqlalchemy import Table, Column, Integer, String, MetaData, Float, Date
from connectors.fuel_api import FuelAPIClient
from connectors.postgres_client import PostgreSqlClient
from assets.fuel_extract import extract, load, transform
from assets.pipeline_logging import PipelineLogging
from datetime import datetime
from jinja2 import Environment, FileSystemLoader


def initialize_logger():
    """Initialize the logger."""
    return PipelineLogging('fuel_pipeline', '../logs')


def load_environment_variables():
    """Load environment variables from .env file."""
    load_dotenv()
    return (
        os.getenv('APIKEY'),
        os.getenv('APISECRET'),
        os.getenv('AUTHORIZATIONHEADER'),
        os.environ.get("DB_USERNAME"),
        os.environ.get("DB_PASSWORD"),
        os.environ.get("DB_SERVER_NAME"),
        os.environ.get("DB_DATABASE_NAME"),
        os.environ.get("PORT")
    )


def initialize_api(api_key, api_secret, auth_header):
    """Initialize the FuelAPI client."""
    return FuelAPIClient(api_key, api_secret, auth_header)


def extract_and_transform_data(api_client):
    """Extract and transform data from the FuelAPI."""
    data_station, data_fuel = extract(api_client)
    df_station = transform(data_station, table="station")
    df_fuel = transform(data_fuel, table="fuel")
    return df_station, df_fuel


def initialize_postgresql_client(server_name, database_name, username, password, port):
    """Initialize the PostgreSQL client."""
    return PostgreSqlClient(
        server_name=server_name,
        database_name=database_name,
        username=username,
        password=password,
        port=port
    )


def create_station_metadata_and_table(table_name):
    """Create metadata and table for a given table name."""
    metadata = MetaData()
    table = Table(
        table_name, metadata,
        Column("station_code", Integer, primary_key=True),
        Column("lat", Float, primary_key=True),
        Column("lon", Float, primary_key=True),
        Column("name", String),
        Column("brand", String),
        Column("address", String),
        Column("state", String)
    )
    return metadata, table


def create_fuel_metadata_and_table(table_name):
    """Create metadata and table for a given table name."""
    metadata = MetaData()
    table = Table(
        table_name, metadata,
        Column("station_code", Integer, primary_key=True),
        Column("last_updated", String, primary_key=True),
        Column("fuel_type", String, primary_key=True),
        Column("price", Float),
        Column("state", String)
    )
    return metadata, table


def calculate_average_fuel_prices(df_fuel):

    df_fuel['date'] = df_fuel['last_updated'].apply(
        lambda x: datetime.strptime(x[:10], '%d/%m/%Y'))
    avg_prices = df_fuel.groupby(['date', 'fuel_type'])[
        'price'].mean().reset_index()
    return avg_prices


def create_avg_prices_metadata_and_table(table_name):
    """Create metadata and table for average fuel prices."""
    metadata = MetaData()
    table = Table(
        table_name, metadata,
        Column("date", Date, primary_key=True),
        Column("fuel_type", String, primary_key=True),
        Column("average_price", Float),
    )
    return metadata, table


def main():
    api_key, api_secret, auth_header, db_username, db_password, server_name, database_name, port = load_environment_variables()
    logger = initialize_logger()
    logger.logger.info("Starting the fuel pipeline...")

    try:
        api_client = initialize_api(api_key, api_secret, auth_header)
        df_station, df_fuel = extract_and_transform_data(api_client)

        postgresql_client = initialize_postgresql_client(
            server_name, database_name, db_username, db_password, port)

        metadata_station, table_station = create_station_metadata_and_table(
            "station")
        load(df_exchange=df_station, postgresql_client=postgresql_client,
             table=table_station, metadata=metadata_station)

        metadata_fuel, table_fuel = create_fuel_metadata_and_table(
            "fuel_prices")
        load(df_exchange=df_fuel, postgresql_client=postgresql_client,
             table=table_fuel, metadata=metadata_fuel)

        avg_prices = calculate_average_fuel_prices(df_fuel)
        metadata_avg_prices, table_avg_prices = create_avg_prices_metadata_and_table(
            "average_fuel_prices")
        load(df_exchange=avg_prices, postgresql_client=postgresql_client,
             table=table_avg_prices, metadata=metadata_avg_prices)
        env = Environment(loader=FileSystemLoader('../'))
        template = env.get_template('avg_fuel_price.sql')
        rendered_sql = template.render()
        postgresql_client.engine.execute(rendered_sql)
        logger.logger.info("Fuel pipeline completed successfully.")

    except Exception as e:
        logger.logger.error(f"An error occurred: {str(e)}")
        logger.logger.info("Fuel pipeline failed.")


if __name__ == '__main__':
    main()
