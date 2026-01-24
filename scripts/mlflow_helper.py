"""Lightweight optional MLflow helper.

If `mlflow` is installed, this wraps basic logging. Otherwise provides no-op
implementations so scripts can run without having MLflow in the environment.
"""
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict

try:
    import mlflow
    _MLFLOW = True
except Exception:
    mlflow = None  # type: ignore
    _MLFLOW = False
import os

# Allow disabling MLflow at runtime for environments where migrations/registry
# are not available (useful for batch local runs). If set to '1', treat as
# not available even if the `mlflow` package is installed.
if os.getenv("OCTA_DISABLE_MLFLOW", "0") == "1":
    mlflow = None
    _MLFLOW = False

try:
    import os
    import pickle
    import tempfile
except Exception:
    pickle = None  # type: ignore
    tempfile = None  # type: ignore
    os = None  # type: ignore


def available() -> bool:
    return _MLFLOW


@contextmanager
def start_run(run_name: str):
    if _MLFLOW:
        with mlflow.start_run(run_name=run_name) as r:
            yield r
    else:
        class _Dummy:
            def __enter__(self):
                return None

            def __exit__(self, exc_type, exc, tb):
                return False

        yield _Dummy()


def log_params(params: Dict[str, Any]) -> None:
    if not _MLFLOW:
        return
    for k, v in params.items():
        try:
            mlflow.log_param(k, v)
        except Exception:
            pass


def log_metrics(metrics: Dict[str, float]) -> None:
    if not _MLFLOW:
        return
    for k, v in metrics.items():
        try:
            mlflow.log_metric(k, float(v))
        except Exception:
            pass


def log_artifacts(path: Path) -> None:
    if not _MLFLOW:
        return
    try:
        mlflow.log_artifacts(str(path))
    except Exception:
        pass


def register_model(model_uri: str, name: str, run_id: str | None = None) -> None:
    """Register a model in MLflow Model Registry.

    If `run_id` is provided and `model_uri` is a relative path, it will be
    converted to a `runs:/<run_id>/<model_uri>` URI. This is best-effort and
    will catch exceptions if MLflow cannot register the artifact format.
    """
    if not _MLFLOW:
        return
    try:
        uri = model_uri
        if run_id and not model_uri.startswith("runs:/") and not model_uri.startswith("/"):
            uri = f"runs:/{run_id}/{model_uri}"
        mlflow.register_model(uri, name)
    except Exception:
        # registration is best-effort for now
        return


def log_pyfunc_model_and_register(
    model_obj: object,
    artifact_path: str,
    registered_name: str | None = None,
    conda_env: Dict[str, Any] | None = None,
    run_id: str | None = None,
    promote_on_valid: bool = False,
    validation_passed: bool = True,
) -> None:
    """Log a Python model as an mlflow.pyfunc model and optionally register it.

    This will pickle `model_obj` and register a lightweight `PythonModel`
    wrapper that loads the pickle artifact at inference time.
    """
    if not _MLFLOW:
        return
    try:
        import mlflow.pyfunc as _pyfunc
        from mlflow.tracking import MlflowClient
    except Exception:
        return

    # ensure we have a run id
    active_run = None
    try:
        active_run = mlflow.active_run()
    except Exception:
        active_run = None
    if run_id is None and active_run is not None:
        run_id = active_run.info.run_id

    # create a temp pickle of the model
    tmpdir = tempfile.mkdtemp(prefix="mlflow_pyfunc_")
    ppath = os.path.join(tmpdir, "model.pkl")
    try:
        with open(ppath, "wb") as fh:
            pickle.dump(model_obj, fh)

        class _PickleWrapper(_pyfunc.PythonModel):
            def load_context(self, context):
                import pickle as _p
                p = context.artifacts.get("model_pkl")
                with open(p, "rb") as _fh:
                    self._model = _p.load(_fh)

            def predict(self, context, model_input):
                # expect the underlying model to have a `predict` method
                if hasattr(self._model, "predict"):
                    return self._model.predict(model_input)
                # fallback: try call on array-like
                return self._model(model_input)

        artifacts = {"model_pkl": ppath}

        # log model
        _pyfunc.log_model(
            artifact_path=artifact_path,
            python_model=_PickleWrapper(),
            artifacts=artifacts,
            conda_env=conda_env,
        )

        # register model if requested
        if registered_name:
            # model uri pointing into the run
            # if we have a run_id, use runs:/ URI
            model_uri = None
            if run_id:
                model_uri = f"runs:/{run_id}/{artifact_path}"
            else:
                # try the logged model local path
                model_uri = f"{artifact_path}"
            try:
                mv = mlflow.register_model(model_uri, registered_name)
                if promote_on_valid and validation_passed:
                    client = MlflowClient()
                    # transition to Production and archive others
                    client.transition_model_version_stage(
                        name=registered_name,
                        version=mv.version,
                        stage="Production",
                        archive_existing_versions=True,
                    )
            except Exception:
                # swallow registration errors; best-effort
                pass
    finally:
        try:
            # keep the temp dir for inspection if needed; remove to avoid clutter
            os.remove(ppath)
            os.rmdir(tmpdir)
        except Exception:
            pass
