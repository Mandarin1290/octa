"""OptionsModelAdapter — thin runtime wrapper for trained options LSTM models.

Provides a uniform predict() interface over H5/Keras model files.
TensorFlow is imported lazily (inside methods) so the module can be
imported in environments where TF is unavailable.
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)


class OptionsModelAdapter:
    """Thin adapter for loading and running a trained options LSTM.

    Usage::

        adapter = OptionsModelAdapter("/path/to/model.h5")
        adapter.load()
        predictions = adapter.predict(X)  # X: ndarray (batch, seq_len, features)

    The adapter fails closed: if the model cannot be loaded, ``predict()``
    returns a neutral 0.5 array and logs a WARNING — it does NOT raise.
    """

    def __init__(self, model_path: str, seq_len: int = 32) -> None:
        self.model_path = str(model_path)
        self.seq_len = seq_len
        self._model: Any = None
        self._load_error: Optional[str] = None

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def is_loaded(self) -> bool:
        return self._model is not None

    @property
    def load_error(self) -> Optional[str]:
        return self._load_error

    # ------------------------------------------------------------------
    # SHA-256 sidecar
    # ------------------------------------------------------------------

    def sha256(self) -> Optional[str]:
        """Read SHA-256 from the <model>.sha256 sidecar file if present."""
        sidecar = Path(self.model_path).with_suffix(".sha256")
        try:
            return sidecar.read_text(encoding="utf-8").strip()
        except Exception:
            return None

    # ------------------------------------------------------------------
    # Load
    # ------------------------------------------------------------------

    def load(self) -> None:
        """Load the Keras H5 model. Raises on failure.

        Call this explicitly to pre-warm the model before inference.
        predict() will call it lazily if not already loaded.
        """
        try:
            import tensorflow as tf  # deferred import
            self._model = tf.keras.models.load_model(self.model_path)
            self._load_error = None
            logger.info(
                "options_model_loaded",
                extra={"model_path": self.model_path},
            )
        except ImportError as exc:
            self._load_error = "tensorflow_not_installed"
            logger.warning(
                "options_model_load_failed",
                extra={"model_path": self.model_path, "reason": self._load_error},
            )
            raise RuntimeError(self._load_error) from exc
        except Exception as exc:
            self._load_error = str(exc)
            logger.warning(
                "options_model_load_failed",
                extra={"model_path": self.model_path, "reason": self._load_error},
            )
            raise

    # ------------------------------------------------------------------
    # Predict
    # ------------------------------------------------------------------

    def predict(self, X: Any) -> Any:
        """Return sigmoid predictions for X (shape: batch × seq_len × features).

        Returns a 1-D numpy array of floats in [0, 1].
        On any error (TF unavailable, model not found, shape mismatch)
        returns a neutral 0.5 array and logs a WARNING — does NOT raise.
        """
        import numpy as np

        if not self.is_loaded:
            try:
                self.load()
            except Exception:
                logger.warning(
                    "options_model_predict_fallback",
                    extra={"model_path": self.model_path, "reason": self._load_error},
                )
                return np.full(len(X), 0.5, dtype=np.float32)

        try:
            raw = self._model.predict(X, verbose=0)
            return raw.flatten().astype(np.float32)
        except Exception as exc:
            logger.warning(
                "options_model_predict_error",
                extra={"model_path": self.model_path, "error": str(exc)},
            )
            import numpy as np
            return np.full(len(X), 0.5, dtype=np.float32)
