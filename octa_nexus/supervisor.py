from __future__ import annotations

import os
import time
from multiprocessing import Process
from typing import Callable, Dict

from .bus import NexusBus
from .messages import Healthbeat, _now_iso


class ComponentSpec:
    def __init__(
        self,
        name: str,
        target: Callable[..., None],
        args: tuple = (),
        kwargs: dict | None = None,
    ):
        self.name = name
        self.target = target
        self.args = args
        self.kwargs = kwargs or {}


def _child_entry(
    bus_path: str, name: str, target: Callable[..., None], args: tuple, kwargs: dict
) -> None:
    """Entrypoint executed inside child process to run target and emit final heartbeat."""
    bus = NexusBus(bus_path)
    try:
        target(*args, **(kwargs or {}))
    finally:
        try:
            bus.publish(
                Healthbeat(
                    id=f"hb-{name}-{int(time.time())}",
                    type="Healthbeat",
                    ts=_now_iso(),
                    component=name,
                    epoch=_now_iso(),
                )
            )
        except Exception:
            pass


class Supervisor:
    def __init__(self, bus_path: str):
        os.makedirs(bus_path, exist_ok=True)
        self.bus_path = bus_path
        self.bus = NexusBus(bus_path)
        self.specs: Dict[str, ComponentSpec] = {}
        self.procs: Dict[str, Process] = {}
        self._stop = False

    def register(self, spec: ComponentSpec) -> None:
        self.specs[spec.name] = spec

    def start(self) -> None:
        for name, spec in self.specs.items():
            self._start_one(name, spec)
        # supervision loop
        try:
            while not self._stop:
                time.sleep(1.0)
                for name, proc in list(self.procs.items()):
                    if not proc.is_alive():
                        # restart
                        self._start_one(name, self.specs[name])
        except KeyboardInterrupt:
            self.stop()

    def _start_one(self, name: str, spec: ComponentSpec) -> None:
        p = Process(
            target=_child_entry,
            args=(self.bus_path, spec.name, spec.target, spec.args, spec.kwargs),
            daemon=False,
        )
        p.start()
        self.procs[name] = p

    def stop(self) -> None:
        self._stop = True
        for p in self.procs.values():
            try:
                p.terminate()
            except Exception:
                pass
