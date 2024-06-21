from dagster import asset

from datetime import datetime, timedelta
import plotly.express as px
import plotly.io as pio
import geopandas as gpd
import pandas as pd

import duckdb
import os

from . import constants


@asset(deps=["taxi_trips", "taxi_zones"])
def manhattan_stats() -> None:
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

    conn = duckdb.connect(os.getenv("DUCKDB_DATABASE"))
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


@asset(
    deps=["taxi_trips"],
)
def trips_by_week() -> None:
    conn = duckdb.connect(os.getenv("DUCKDB_DATABASE"))
    start_date = datetime.strptime("2023-03-01", constants.DATE_FORMAT)
    end_date = datetime.strptime("2023-04-01", constants.DATE_FORMAT)

    result = pd.DataFrame()

    while start_date < end_date:
        start_date_str = start_date.strftime(constants.DATE_FORMAT)
        query = f"""
            SELECT 
                vendor_id, total_amount, trip_distance, passenger_count
            FROM trips
            WHERE DATE_TRUNC('week', pickup_datetime) = DATE_TRUNC('week', '{start_date_str}'::date)
        """

        data_for_week = conn.execute(query).fetch_df().head()

        print(data_for_week)
        aggregate = (
            data_for_week.agg(
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

        aggregate["period"] = start_date

        result = pd.concat([result, aggregate])
        start_date += timedelta(days=7)

    # clean up the formatting of the dataframe
    result["passenger_count"] = result["passenger_count"].astype(int)
    result["num_trips"] = result["num_trips"].astype(int)
    result["total_amount"] = result["total_amount"].round(2).astype(float)
    result["trip_distance"] = result["trip_distance"].round(2).astype(float)
    result = result[
        ["period", "num_trips", "total_amount", "trip_distance", "passenger_count"]
    ]
    result = result.sort_values(by="period")

    print(result.head())

    result.to_csv(constants.TRIPS_BY_WEEK_FILE_PATH, index=False)
