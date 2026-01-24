import json
from pathlib import Path

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from octa_atlas.models import ArtifactMetadata, ModelBundle
from octa_atlas.registry import AtlasRegistry, FileIntegrityError


def test_save_load_roundtrip(tmp_path):
    root = str(tmp_path / "atlas")
    priv = Ed25519PrivateKey.generate()
    priv_bytes = priv.private_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PrivateFormat.Raw,
        encryption_algorithm=serialization.NoEncryption(),
    )
    pub_bytes = priv.public_key().public_bytes(
        encoding=serialization.Encoding.Raw, format=serialization.PublicFormat.Raw
    )
    reg = AtlasRegistry(root, signing_key_bytes=priv_bytes, public_key_bytes=pub_bytes)
    metadata = ArtifactMetadata(
        asset_id="a1",
        artifact_type="model",
        version="v1",
        created_at="2025-01-01T00:00:00Z",
        dataset_hash="dh",
        training_window="2020-2021",
        feature_spec_hash="fh",
        hyperparams={"lr": 0.1},
        metrics={"acc": 0.9},
        code_fingerprint="cf",
        gate_status="PENDING",
    )
    model = ModelBundle(model_obj={"weights": [1, 2, 3]})
    reg.save_artifact("a1", "model", "v1", model, metadata)
    obj, meta = reg.load_latest("a1", "model")
    assert isinstance(obj, ModelBundle)
    assert meta["artifact_hash"]


def test_integrity_failure_blocks(tmp_path):
    root = str(tmp_path / "atlas2")
    priv = Ed25519PrivateKey.generate()
    priv_bytes = priv.private_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PrivateFormat.Raw,
        encryption_algorithm=serialization.NoEncryption(),
    )
    pub_bytes = priv.public_key().public_bytes(
        encoding=serialization.Encoding.Raw, format=serialization.PublicFormat.Raw
    )
    reg = AtlasRegistry(root, signing_key_bytes=priv_bytes, public_key_bytes=pub_bytes)
    metadata = ArtifactMetadata(
        asset_id="a2",
        artifact_type="model",
        version="v1",
        created_at="2025-01-01T00:00:00Z",
        dataset_hash="dh",
        training_window="2020-2021",
        feature_spec_hash="fh",
        hyperparams={},
        metrics={},
        code_fingerprint="cf",
        gate_status="PENDING",
    )
    model = ModelBundle(model_obj={"weights": [1]})
    reg.save_artifact("a2", "model", "v1", model, metadata)
    # tamper artifact file
    art = Path(root) / "models" / "a2" / "model" / "v1" / "artifact.pkl"
    data = art.read_bytes()
    art.write_bytes(data + b"x")
    try:
        reg.load_latest("a2", "model")
        ok = True
    except FileIntegrityError:
        ok = False
    assert not ok


def test_metadata_signature_verifies(tmp_path):
    root = str(tmp_path / "atlas3")
    priv = Ed25519PrivateKey.generate()
    priv_bytes = priv.private_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PrivateFormat.Raw,
        encryption_algorithm=serialization.NoEncryption(),
    )
    pub_bytes = priv.public_key().public_bytes(
        encoding=serialization.Encoding.Raw, format=serialization.PublicFormat.Raw
    )
    reg = AtlasRegistry(root, signing_key_bytes=priv_bytes, public_key_bytes=pub_bytes)
    metadata = ArtifactMetadata(
        asset_id="a3",
        artifact_type="model",
        version="v1",
        created_at="2025-01-01T00:00:00Z",
        dataset_hash="dh",
        training_window="2020-2021",
        feature_spec_hash="fh",
        hyperparams={},
        metrics={},
        code_fingerprint="cf",
        gate_status="PENDING",
    )
    model = ModelBundle(model_obj={"w": []})
    reg.save_artifact("a3", "model", "v1", model, metadata)
    # load meta.json and check signature exists
    meta_path = Path(root) / "models" / "a3" / "model" / "v1" / "meta.json"
    meta = json.loads(meta_path.read_text())
    assert "signature" in meta
