from dagster import AssetSelection, define_asset_job
from ..partitions import monthly_partition, weekly_partition

# we don't want it to run it with the rest of our pipeline and it should be run more frequently
trips_by_week = AssetSelection.assets(["trips_by_week"])

# trip_update_job = define_asset_job(
#     name="trip_update_job",
#     selection=AssetSelection.all() - trips_by_week,
# )
# only get the latest month's data and not refresh the entirety of the asset
trip_update_job = define_asset_job(
    name="trip_update_job",
    selection=AssetSelection.all() - trips_by_week,
    partitions_def=monthly_partition,
)


weekly_update_job = define_asset_job(
    name="weekly_update_job", selection=trips_by_week, partitions_def=weekly_partition
)
