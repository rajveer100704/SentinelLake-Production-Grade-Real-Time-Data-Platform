from datetime import timedelta
from feast import (
    Entity,
    FeatureView,
    Field,
    FileSource,
    PushSource,
)
from feast.types import Int64, String, Float32

# Define an entity for the user
user = Entity(name="user_id", join_keys=["user_id"], description="User ID")

# Define the source of the data (Silver layer in Delta)
user_stats_source = FileSource(
    path="/tmp/delta/silver",
    event_timestamp_column="event_time",
    created_timestamp_column="ingestion_timestamp",
)

# Define the Feature View
user_activity_v1 = FeatureView(
    name="user_activity_stats",
    entities=[user],
    ttl=timedelta(days=1),
    schema=[
        Field(name="event_type", dtype=String),
        Field(name="timestamp", dtype=Int64),
    ],
    online=True,
    source=user_stats_source,
    tags={"team": "analytics"},
)
