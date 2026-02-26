import os
import subprocess
from pathlib import Path

import pytest
import redis

# This integration test runs a minimal Feast repo flow using fakeredis-server
# It requires `feast` to be available in the test environment (added to CI job)

FEAST_REPO = Path("feast_repo")


def run(cmd, cwd=FEAST_REPO, env=None, timeout=120):
    res = subprocess.run(cmd, cwd=cwd, shell=True, env=env, capture_output=True, text=True, timeout=timeout)
    print(res.stdout)
    print(res.stderr)
    res.check_returncode()
    return res


@pytest.mark.integration
@pytest.mark.slow
def test_feast_apply_and_materialize():
    # Start a fakeredis server (python package fakeredis-server provides redis-server binary in PATH on CI)
    # Fallback: use a local ephemeral real redis if available
    r = redis.Redis(host="127.0.0.1", port=6379)
    try:
        r.ping()
    except Exception:
        # try to start a subprocess redis-server (fakeredis-server recommended in CI)
        pytest.skip("A redis server must be available on localhost:6379 for this integration test")

    # Ensure feast repo exists
    assert FEAST_REPO.exists(), "feast_repo folder with feature definitions is required for integration test"

    env = os.environ.copy()
    env["FEAST_REPO_DIR"] = str(FEAST_REPO)
    env["REDIS_HOST"] = "127.0.0.1"
    env["REDIS_PORT"] = "6379"

    # Ensure feast_repo has a data.parquet with required join key `id` and `timestamp` columns.
    # Generate deterministic sample data inline — no external file dependency.
    prep_cmd = (
        "python3 - <<'PY'\n"
        "import pandas as pd, pathlib\n"
        "dst='feast_repo/data/data.parquet'\n"
        "pathlib.Path(dst).parent.mkdir(parents=True, exist_ok=True)\n"
        "n=20\n"
        "df=pd.DataFrame({\n"
        "    'id': [str(i) for i in range(n)],\n"
        "    'timestamp': pd.date_range('2025-01-01', periods=n, freq='D', tz='UTC'),\n"
        "    'feat1': [float(i) * 0.1 for i in range(n)],\n"
        "    'feat2': [float(i) * 0.2 for i in range(n)],\n"
        "})\n"
        "df.to_parquet(dst, index=False)\n"
        "print('wrote', dst)\n"
        "PY"
    )
    run(prep_cmd, cwd=Path('.'), env=env)

    # run feast apply (Feast reads project from feature_store.yaml)
    run("feast apply", env=env)

    # ingest or materialize sample data (if repo provides a sample ingestion script)
    # materialize incremental for a small timeframe
    # CLI expects a single END_TS in some Feast versions; use end timestamp
    run("feast materialize-incremental 2025-12-31T23:59:59", env=env)

    # check redis keys for feature rows
    r = redis.Redis(host="127.0.0.1", port=6379)
    keys = r.keys()
    assert len(keys) > 0, "Expected keys in redis after materialize"
