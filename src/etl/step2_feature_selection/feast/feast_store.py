from feast import FeatureStore
import datetime
import os
from .feature_repo.example_repo import feature_definition

from dagster import asset 
from dagster import AssetExecutionContext

from ..data_merge_code.data_merged import data_merged



def apply_feast_changes(repo_path: str):
    if repo_path is None:
        repo_path = path

    # Initialize the FeatureStore object
    store = FeatureStore(repo_path=repo_path)

    # Apply changes to the feature store
    store.apply(feature_definition)


@asset(
        group_name="step2_feature_selection",
        deps=[data_merged],
)
def materialize_incrementally(context: AssetExecutionContext):

    script_dir = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(script_dir, "feature_repo") 

    end_date = datetime.datetime.now()

    # Initialize the FeatureStore object
    store = FeatureStore(repo_path=path)

    # Materialize incrementally up to the specified end date
    store.materialize_incremental(end_date)

    context.log.info("materialize_incremental successfully")






