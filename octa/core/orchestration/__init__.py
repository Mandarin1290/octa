from .runner import run_cascade


def get_cascade_job():
    from .flow import cascade_job

    return cascade_job


__all__ = ["get_cascade_job", "run_cascade"]
