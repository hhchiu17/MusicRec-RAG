# This is an example feature definition file

from datetime import timedelta

import pandas as pd

from feast import (
    Entity,
    FeatureService,
    FeatureView,
    Field,
    FileSource,
    PushSource,
    RequestSource,
    ValueType
)
from feast.feature_logging import LoggingConfig
from feast.infra.offline_stores.file_source import FileLoggingDestination
from feast.on_demand_feature_view import on_demand_feature_view
from feast.types import Float32, Float64, Int64, String

from feast.infra.offline_stores.contrib.postgres_offline_store.postgres_source import (
    PostgreSQLSource,
)


song_id = Entity(name="id", value_type=ValueType.STRING, description="song id")


rag_source = PostgreSQLSource(
    name="rag_db",
    query='SELECT * FROM public.merge_demo',
    timestamp_field="fetchDate"
)


rag_fv = FeatureView(
    name="rag_feature",
    entities=[song_id],
    ttl=timedelta(days=30),

    schema=[
        Field(name="song", dtype=String),
        Field(name="artist", dtype=String),
        Field(name="category", dtype=String),
        Field(name="playlist", dtype=String),
        Field(name="playlistDescription", dtype=String),
        Field(name="language", dtype=String),
        Field(name="publishDate", dtype=String),
        Field(name="platform", dtype=String),
        Field(name="songDuration", dtype=Float32),
        Field(name="url", dtype=String),
        Field(name="lyrics", dtype=String)
    ],
    online=True,
    source=rag_source,

)


feature_definition = [song_id, rag_fv]
