from dagster_duckdb import DuckDBResource
from dagster import EnvVar

database_resource = DuckDBResource(
    # database="data/staging/data.duckdb"
    # envvar fetches the environmental var every time a run starts
    ## so we can change it without restarting dagster server
    # os.getenv fetches the env var when the code location is loaded
    database=EnvVar("DUCKDB_DATABASE")
)
