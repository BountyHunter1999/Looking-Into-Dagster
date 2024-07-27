import requests
from . import constants
from ..partitions import monthly_partition
from dagster import asset, AssetExecutionContext
from dagster_duckdb import DuckDBResource
import duckdb
import os


@asset(partitions_def=monthly_partition)
def taxi_trips_file(context: AssetExecutionContext) -> None:
    """
    The raw parquest files for the taxi trips dataset. Sourced from the NYC Open Data portal.
    """
    # context argument provides us with metadata about the current materialization
    # how dagster is running and materializing our asset (which partition is materializing, which job triggered it
    # or what metadata was attached to its previous materilization)
    # partition_key property to dynamically fetch a specific partition's month of data
    partition_date_str = context.partition_key
    # we get key in YYYY-MM-DD format
    month_to_fetch = partition_date_str[:-3]
    raw_trips = requests.get(
        f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month_to_fetch}.parquet"
    )

    with open(
        constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch), "wb"
    ) as output_file:
        output_file.write(raw_trips.content)


@asset
def taxi_zones_file() -> None:
    """
    The raw CSV file for the taxi zones dataset. Source from the NYC Open Data portal
    """
    data = requests.get(
        "https://data.cityofnewyork.us/api/views/755u-8jsi/rows.csv?accessType=DOWNLOAD"
    )
    with open(constants.TAXI_ZONES_FILE_PATH, "wb") as f:
        f.write(data.content)


@asset(deps=["taxi_trips_file"], partitions_def=monthly_partition)
def taxi_trips(context: AssetExecutionContext, database: DuckDBResource) -> None:
    """
    The raw taxi trips dataset, loaded into DuckDB database
    """
    # sql_query = """
    #     CREATE OR REPLACE TABLE trips AS (
    #         SELECT
    #             VendorID AS vendor_id,
    #             PULocationID AS pickup_zone_id,
    #             DOLocationID AS dropoff_zone_id,
    #             RatecodeID AS rate_code_id,
    #             payment_type AS payment_type,
    #             tpep_dropoff_datetime AS dropoff_datetime,
    #             tpep_pickup_datetime AS pickup_datetime,
    #             trip_distance AS trip_distance,
    #             passenger_count AS passenger_count,
    #             total_amount AS total_amount
    #         FROM 'data/raw/taxi_trips_2023-03.parquet'
    #     )
    # """
    partition_date_str = context.partition_key
    month_to_fetch = partition_date_str[:-3]
    sql_query = f"""
        CREATE TABLE IF NOT EXISTS trips (
            vendor_id integer, pickup_zone_id integer, dropoff_zone_id integer,
            rate_code_id double, payment_type integer, dropoff_datetime timestamp,
            pickup_datetime timestamp, trip_distance double, passenger_count double,
            total_amount double, partition_date varchar
        );
        
        DELETE FROM trips WHERE partition_date = '{month_to_fetch}';
        
        INSERT INTO trips
        SELECT
            VendorID, PULocationID, DOLocationID, RatecodeID, payment_type, tpep_dropoff_datetime,
            tpep_pickup_datetime, trip_distance, passenger_count, total_amount, '{month_to_fetch}' AS partition_date
        FROM '{constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch)}';
    """

    # conn = duckdb.connect(os.getenv("DUCKDB_DATABASE"))
    with database.get_connection() as conn:
        conn.execute(sql_query)


@asset(deps=["taxi_zones_file"])
def taxi_zones(database: DuckDBResource) -> None:
    # CTAS
    sql_query = f"""
        CREATE OR REPLACE TABLE zones AS (
            SELECT
                LocationID AS zone_id,
                zone,
                borough,
                the_geom AS geometry
            FROM '{constants.TAXI_ZONES_FILE_PATH}'
        )
    """
    with database.get_connection() as conn:
        conn.execute(sql_query)
