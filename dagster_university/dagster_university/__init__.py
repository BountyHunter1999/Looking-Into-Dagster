# fmt: off
from dagster import Definitions, load_assets_from_modules

from .assets import metrics, trips
from .resources import database_resource
from .jobs import trip_update_job, weekly_update_job

trip_assets = load_assets_from_modules([trips])
metric_assets = load_assets_from_modules([metrics])
all_jobs = [trip_update_job, weekly_update_job]

defs = Definitions(
    assets=[*trip_assets, *metric_assets],
    resources={
        "database": database_resource,
    },
    jobs=all_jobs
)
