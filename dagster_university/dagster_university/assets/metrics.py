from dagster import asset

import plotly.express as px
import plotly.io as pio
import geopandas as gpd

import duckdb
import os

from . import constants


@asset(deps=["taxi_trips", "taxi_zones"])
def manhattan_stats() -> None:
    pass
