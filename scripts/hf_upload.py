from __future__ import annotations

import argparse
import os
from pathlib import Path

from huggingface_hub import HfApi, Repository


def upload_model_to_hf(model_file: str, repo_id: str, commit_message: str = "upload model") -> int:
    token = os.getenv("HF_TOKEN")
    if not token:
        print("HF_TOKEN not set; cannot upload")
        return 2

    model_path = Path(model_file)
    if not model_path.exists():
        print("Model file not found:", model_file)
        return 3

    api = HfApi()
    try:
        # create repo if not exists
        api.create_repo(repo_id=repo_id, token=token, exist_ok=True)
    except Exception as e:
        print("Could not ensure repo exists:", e)

    # clone repo to temp dir and copy file
    tempdir = Path("/tmp/hf_repo_") / repo_id.replace("/", "_")
    tempdir.mkdir(parents=True, exist_ok=True)
    repo = Repository(local_dir=str(tempdir), clone_from=repo_id, use_auth_token=token)
    dest = tempdir / model_path.name
    with model_path.open("rb") as src, dest.open("wb") as dst:
        dst.write(src.read())
    try:
        repo.push_to_hub(commit_message=commit_message)
    except Exception as e:
        print("Failed to push to hub:", e)
        return 4
    print("Uploaded", model_path.name, "to", repo_id)
    return 0


def _parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--model-file", required=True)
    p.add_argument("--repo-id", required=True)
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    raise SystemExit(upload_model_to_hf(args.model_file, args.repo_id))
