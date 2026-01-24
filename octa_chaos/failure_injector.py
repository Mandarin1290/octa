import random
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass
class InjectionRecord:
    ts: float
    target: str
    failure_type: str
    details: Dict[str, Any]


class FailureInjector:
    """Controlled chaos injector for non-live subsystems.

    - Uses a deterministic RNG when `seed` provided.
    - Refuses to act on targets that declare `live=True`.
    - Supports failure types: 'restart', 'delay', 'partial_unavailable'.
    - Provides `recover_all()` to reverse injections.
    """

    def __init__(self, seed: Optional[int] = None):
        self.rng = random.Random(seed)
        self.records: List[InjectionRecord] = []
        self._lock = threading.Lock()

    def _log(self, target_name: str, failure_type: str, details: Dict[str, Any]):
        rec = InjectionRecord(
            ts=time.time(),
            target=target_name,
            failure_type=failure_type,
            details=details,
        )
        with self._lock:
            self.records.append(rec)

    def inject_restart(
        self, target: Any, target_name: str, allow_live: bool = False
    ) -> bool:
        # target may optionally have `live` attribute
        if getattr(target, "live", False) and not allow_live:
            self._log(target_name, "restart_refused_live", {})
            return False
        # simulate restart: call `restart()` if exists, mark attribute
        if hasattr(target, "restart") and callable(target.restart):
            target.restart()
        target._restarted = True
        self._log(target_name, "restart", {})
        return True

    def inject_delay(
        self,
        target: Any,
        target_name: str,
        delay_seconds: float,
        allow_live: bool = False,
    ) -> bool:
        if getattr(target, "live", False) and not allow_live:
            self._log(target_name, "delay_refused_live", {"delay": delay_seconds})
            return False
        # mark target as delayed; real systems would hook into I/O; here we set flag and optionally sleep in test hooks
        target._delayed_for = delay_seconds
        self._log(target_name, "delay", {"delay": delay_seconds})
        return True

    def inject_partial_unavailable(
        self,
        target: Any,
        target_name: str,
        keys: Optional[List[str]] = None,
        allow_live: bool = False,
    ) -> bool:
        if getattr(target, "live", False) and not allow_live:
            self._log(target_name, "partial_refused_live", {"keys": keys})
            return False
        # mark unavailable keys on target by setting attribute `_unavailable_keys`
        unavailable = set(keys or [])
        target._unavailable_keys = unavailable
        self._log(target_name, "partial_unavailable", {"keys": list(unavailable)})
        return True

    def random_inject(
        self, targets: Dict[str, Any], failure_types: List[str], max_events: int = 1
    ) -> List[InjectionRecord]:
        events = []
        names = sorted(targets.keys())
        for _ in range(max_events):
            if not names:
                break
            t = self.rng.choice(names)
            f = self.rng.choice(failure_types)
            target = targets[t]
            ok = False
            if f == "restart":
                ok = self.inject_restart(target, t)
            elif f == "delay":
                delay = self.rng.uniform(0.1, 1.0)
                ok = self.inject_delay(target, t, delay)
            elif f == "partial_unavailable":
                # pick some keys if iterable
                keys = (
                    list(getattr(target, "keys", lambda: [])())
                    if hasattr(target, "keys")
                    else []
                )
                pick = [keys[i] for i in range(min(len(keys), 1))] if keys else []
                ok = self.inject_partial_unavailable(target, t, pick)
            if ok:
                with self._lock:
                    events.append(self.records[-1])
        return events

    def recover_all(self, targets: Dict[str, Any]) -> None:
        # undo flags
        for _name, target in targets.items():
            if hasattr(target, "_restarted"):
                try:
                    if hasattr(target, "restart_complete") and callable(
                        target.restart_complete
                    ):
                        target.restart_complete()
                except Exception:
                    pass
                delattr(target, "_restarted")
            if hasattr(target, "_delayed_for"):
                delattr(target, "_delayed_for")
            if hasattr(target, "_unavailable_keys"):
                delattr(target, "_unavailable_keys")
        self._log("injector", "recover_all", {})


__all__ = ["FailureInjector", "InjectionRecord"]
