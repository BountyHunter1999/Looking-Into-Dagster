from dagster import asset, AssetExecutionContext
from dagster_duckdb import DuckDBResource

from datetime import datetime, timedelta
import plotly.express as px
import plotly.io as pio
import geopandas as gpd
import pandas as pd

import duckdb
import os

from . import constants
from ..partitions import weekly_partition


@asset(deps=["taxi_trips", "taxi_zones"])
def manhattan_stats(database: DuckDBResource) -> None:
    query = """
        SELECT
            zones.zone,
            zones.borough,
            zones.geometry,
            count(1) as num_trips,
        FROM trips
        LEFT JOIN zones ON trips.pickup_zone_id = zones.zone_id
        WHERE borough = 'Manhattan' AND geometry IS NOT NULL
        GROUP BY zone, borough, geometry
        """
    with database.get_connection() as conn:
        trips_by_zone = conn.execute(query).fetch_df()

    trips_by_zone["geometry"] = gpd.GeoSeries.from_wkt(trips_by_zone["geometry"])
    trips_by_zone = gpd.GeoDataFrame(trips_by_zone)

    with open(constants.MANHATTAN_STATS_FILE_PATH, "w") as output_file:
        output_file.write(trips_by_zone.to_json())


@asset(
    deps=["manhattan_stats"],
)
def manhattan_map() -> None:
    trips_by_zone = gpd.read_file(constants.MANHATTAN_STATS_FILE_PATH)

    print(trips_by_zone.head())
    print(trips_by_zone.geometry.__geo_interface__)

    fig = px.choropleth_mapbox(
        trips_by_zone,
        geojson=trips_by_zone.geometry.__geo_interface__,
        locations=trips_by_zone.index,
        color="num_trips",
        color_continuous_scale="Plasma",
        mapbox_style="carto-positron",
        center={"lat": 40.758, "lon": -73.985},
        zoom=11,
        opacity=0.7,
        labels={"num_trips": "Number of Trips"},
    )

    pio.write_image(fig, constants.MANHATTAN_MAP_FILE_PATH)


@asset(deps=["taxi_trips"], partitions_def=weekly_partition)
def trips_by_week(context: AssetExecutionContext, database: DuckDBResource) -> None:

    period_to_fetch = context.partition_key

    # start_date = datetime.strptime("2023-03-01", constants.DATE_FORMAT)
    # end_date = datetime.strptime("2023-04-01", constants.DATE_FORMAT)

    # result = pd.DataFrame()

    # while start_date < end_date:
    #     start_date_str = start_date.strftime(constants.DATE_FORMAT)
    #     query = f"""
    #         SELECT
    #             vendor_id, total_amount, trip_distance, passenger_count
    #         FROM trips
    #         WHERE DATE_TRUNC('week', pickup_datetime) = DATE_TRUNC('week', '{start_date_str}'::date)
    #     """
    #     with database.get_connection() as conn:
    #         data_for_week = conn.execute(query).fetch_df()

    #     print(data_for_week.head())
    #     aggregate = (
    #         data_for_week.agg(
    #             {
    #                 "vendor_id": "count",
    #                 "total_amount": "sum",
    #                 "trip_distance": "sum",
    #                 "passenger_count": "sum",
    #             }
    #         )
    #         .rename({"vendor_id": "num_trips"})
    #         .to_frame()
    #         .T
    #     )

    #     aggregate["period"] = start_date

    #     result = pd.concat([result, aggregate])
    #     start_date += timedelta(days=7)

    # get all the trips for the week
    query = f"""
        SELECT vendor_id, total_amount, trip_distance, passenger_count
        FROM trips
        WHERE pickup_datetime >= '{period_to_fetch}'
            and pickup_datetime < '{period_to_fetch}'::date + interval '1 week';
    """

    with database.get_connection() as conn:
        data_for_month = conn.execute(query).fetch_df()

    aggregate = (
        data_for_month.agg(
            {
                "vendor_id": "count",
                "total_amount": "sum",
                "trip_distance": "sum",
                "passenger_count": "sum",
            }
        )
        .rename({"vendor_id": "num_trips"})
        .to_frame()
        .T
    )

    # clean up the formatting of the dataframe
    aggregate["period"] = period_to_fetch
    aggregate["num_trips"] = aggregate["num_trips"].astype(int)
    aggregate["passenger_count"] = aggregate["passenger_count"].astype(int)
    aggregate["total_amount"] = aggregate["total_amount"].round(2).astype(float)
    aggregate["trip_distance"] = aggregate["trip_distance"].round(2).astype(float)
    aggregate = aggregate[
        ["period", "num_trips", "total_amount", "trip_distance", "passenger_count"]
    ]
    # aggregate = aggregate.sort_values(by="period")

    try:
        # if the file already exists, append to it, but replace the existing month's data
        existing = pd.read_csv(constants.TRIPS_BY_WEEK_FILE_PATH)
        existing = existing[existing["period"] != period_to_fetch]
        existing = pd.concat([existing, aggregate]).sort_values(by="period")
        existing.to_csv(constants.TRIPS_BY_WEEK_FILE_PATH, index=False)
    # print(aggregate.head())
    except FileNotFoundError:
        aggregate.to_csv(constants.TRIPS_BY_WEEK_FILE_PATH, index=False)
