from __future__ import annotations

from typing import Any

import mlflow
import mlflow.pyfunc
import pandas as pd


class SimplePyFuncModel(mlflow.pyfunc.PythonModel):
    def __init__(self, model_state: dict):
        self.model_state = model_state

    def predict(self, context, model_input: Any):
        # model_state contains coef and intercept
        coef = self.model_state.get("coef", [])
        intercept = float(self.model_state.get("intercept", 0.0))
        df = pd.DataFrame(model_input)
        if df.shape[1] == 1:
            arr = df.iloc[:, 0].astype(float).tolist()
            return [coef[0] * float(x) + intercept for x in arr]
        out = []
        for _, row in df.iterrows():
            s = 0.0
            for c, v in zip(coef, row.tolist(), strict=False):
                s += float(c) * float(v)
            s += intercept
            out.append(s)
        return out


def log_pyfunc_model(model_state: dict, artifact_path: str = "model_pyfunc", registered_name: str | None = None, run_id: str | None = None):
    pyfunc_model = SimplePyFuncModel(model_state)
    mlflow.pyfunc.log_model(artifact_path=artifact_path, python_model=pyfunc_model)
    if registered_name and run_id:
        # Register model from the artifacts
        mlflow.register_model(f"runs:/{run_id}/{artifact_path}", registered_name)
