from pathlib import Path

from feast import Entity, FeatureView, FileSource, ValueType

HERE = Path(__file__).parent
DATA_PATH = HERE / "data" / "data.parquet"

file_source = FileSource(
    path=str(DATA_PATH),
    event_timestamp_column="timestamp",
)

entity = Entity(name="id", value_type=ValueType.STRING, description="entity id")

feature_view = FeatureView(
    name="demo_fv",
    entities=[entity],
    ttl=None,
    # omit explicit schema so Feast will infer it from the FileSource
    source=file_source,
)
