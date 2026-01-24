from octa.core import orchestration


def test_orchestrator_import_smoke() -> None:
    assert hasattr(orchestration, "run_cascade")
    job_factory = orchestration.get_cascade_job()
    assert job_factory is not None

