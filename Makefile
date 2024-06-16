create_project:
	dagster project from-example --example project_dagster_university_start --name dagster_university
	cd dagster_university && \
	cp .env.example .env && \
	pip install -e ".[dev]"

# pip install -e ".[dev] doing this dagster can pick up changes in our asset function's code without needing to reload
# the code location again

run:
	cd dagster_university && dagster dev -p 4001