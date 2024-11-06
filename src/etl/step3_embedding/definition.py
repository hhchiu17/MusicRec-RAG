# dagster related modules
from dagster import Definitions
from dagster import define_asset_job
from dagster import AssetSelection

# import assets
from app import main


defs = Definitions(
    assets=[main], 
    # sensors=[run_function_b_sensor],
    jobs=[
        define_asset_job(
            name="step3_embedding_job",
            selection=AssetSelection.groups("step3_embedding"),
        )
    ])