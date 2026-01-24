Dagster scaffolding for orchestrating the trainer.

Files:
- `dagster/repository.py` registers jobs
- `dagster/jobs.py` defines the training job
- `dagster/ops/train.py` simple op that invokes the trainer script

Run locally with Dagster, pointing at the repository file (this repo also contains a `dagster/` folder, so file-based loading is the most reliable):

`./.venv/bin/dagster dev -f dagster/repository.py`
