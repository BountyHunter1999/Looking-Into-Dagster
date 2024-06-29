from dagster import AssetSelection, define_asset_job

# we don't want it to run it with the rest of our pipeline and it should be run more frequently
trips_by_week = AssetSelection.assets("trips_by_week")


trip_update_job = define_asset_job(
    name="trip_update_job", selection=AssetSelection.all() - trips_by_week
)
