from __future__ import annotations

import json
import pickle
from pathlib import Path
from typing import Any, List, Optional

from octa_fabric.fingerprint import sha256_hexdigest

from .integrity import file_sha256, sign_metadata, verify_metadata_signature
from .models import ArtifactMetadata


class RegistryError(Exception):
    pass


class ArtifactNotFound(RegistryError):
    pass


class FileIntegrityError(RegistryError):
    pass


class AtlasRegistry:
    def __init__(
        self,
        root: str,
        signing_key_bytes: Optional[bytes] = None,
        public_key_bytes: Optional[bytes] = None,
    ) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.signing_key_bytes = signing_key_bytes
        self.public_key_bytes = public_key_bytes

    def _artifact_dir(self, asset_id: str, artifact_type: str, version: str) -> Path:
        return self.root / "models" / asset_id / artifact_type / version

    def save_artifact(
        self,
        asset_id: str,
        artifact_type: str,
        version: str,
        artifact_obj: Any,
        metadata: ArtifactMetadata,
    ) -> None:
        d = self._artifact_dir(asset_id, artifact_type, version)
        d.mkdir(parents=True, exist_ok=True)
        art_path = d / "artifact.pkl"
        meta_path = d / "meta.json"
        integrity_path = d / "integrity.sha256"

        # write artifact
        with open(art_path, "wb") as fh:
            pickle.dump(artifact_obj, fh)

        # compute integrity
        art_hash = file_sha256(str(art_path))
        with open(integrity_path, "w", encoding="utf-8") as fh:
            fh.write(art_hash)

        meta_dict = metadata.to_dict()
        meta_dict["artifact_hash"] = art_hash
        meta_dict["code_fingerprint"] = meta_dict.get(
            "code_fingerprint"
        ) or sha256_hexdigest(meta_dict)

        # sign metadata if key present
        signature = None
        if self.signing_key_bytes:
            signature = sign_metadata(self.signing_key_bytes, meta_dict)
            meta_dict["signature"] = signature

        with open(meta_path, "w", encoding="utf-8") as fh:
            json.dump(meta_dict, fh, sort_keys=True, indent=2, ensure_ascii=False)

    def list_versions(self, asset_id: str, artifact_type: str) -> List[str]:
        base = self.root / "models" / asset_id / artifact_type
        if not base.exists():
            return []
        return [p.name for p in base.iterdir() if p.is_dir()]

    def load_latest(self, asset_id: str, artifact_type: str):
        versions = self.list_versions(asset_id, artifact_type)
        if not versions:
            raise ArtifactNotFound("no versions")
        # assume lexical order; caller should manage semantic versions
        latest = sorted(versions)[-1]
        return self._load(asset_id, artifact_type, latest)

    def _load(self, asset_id: str, artifact_type: str, version: str):
        d = self._artifact_dir(asset_id, artifact_type, version)
        art_path = d / "artifact.pkl"
        meta_path = d / "meta.json"
        integrity_path = d / "integrity.sha256"
        if (
            not art_path.exists()
            or not meta_path.exists()
            or not integrity_path.exists()
        ):
            raise ArtifactNotFound("artifact files missing")

        with open(integrity_path, "r", encoding="utf-8") as fh:
            expected_hash = fh.read().strip()
        actual_hash = file_sha256(str(art_path))
        if actual_hash != expected_hash:
            raise FileIntegrityError("artifact hash mismatch")

        with open(meta_path, "r", encoding="utf-8") as fh:
            meta = json.load(fh)

        # verify signature if public key provided
        sig = meta.get("signature")
        if self.public_key_bytes and sig:
            ok = verify_metadata_signature(
                self.public_key_bytes,
                {k: v for k, v in meta.items() if k != "signature"},
                sig,
            )
            if not ok:
                raise FileIntegrityError("metadata signature verification failed")

        with open(art_path, "rb") as fh:
            obj = pickle.load(fh)
        return obj, meta
