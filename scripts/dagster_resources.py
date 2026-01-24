from __future__ import annotations

import os

import mlflow as _mlflow
import redis
from feast import FeatureStore

from dagster import resource


@resource(config_schema={"repo_path": str})
def feast_resource(context):
    repo_path = context.resource_config.get("repo_path") or os.getenv("FEAST_REPO_PATH", "feast_repo")
    return FeatureStore(repo_path=repo_path)


@resource(config_schema={"host": str, "port": int})
def redis_resource(context):
    host = context.resource_config.get("host") or os.getenv("REDIS_HOST", "localhost")
    port = context.resource_config.get("port") or int(os.getenv("REDIS_PORT", "6379"))
    return redis.Redis(host=host, port=port, db=0)


@resource
def mlflow_resource(_context):
    # configure MLflow via env var if present
    tracking_uri = os.getenv("MLFLOW_TRACKING_URI")
    if tracking_uri:
        _mlflow.set_tracking_uri(tracking_uri)
    return _mlflow
