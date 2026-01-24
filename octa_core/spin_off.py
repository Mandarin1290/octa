from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Set


@dataclass
class ModuleDescriptor:
    name: str
    provides: List[str]
    depends_on: List[str]
    critical: bool = False  # whether module must remain in core


class SpinOffError(Exception):
    pass


class SpinOffManager:
    """Manage proposals for spinning off modules while preserving core integrity.

    Rules enforced:
    - Spin-off cannot remove modules marked `critical`.
    - Remaining core modules must have all dependencies satisfied (either in core or explicitly declared external).
    - Modules must declare explicit dependencies; unknown dependencies cause rejection.
    """

    def __init__(self) -> None:
        self._modules: Dict[str, ModuleDescriptor] = {}
        self._external_allowlist: Set[str] = set()

    def register_module(self, mod: ModuleDescriptor) -> None:
        if mod.name in self._modules:
            raise SpinOffError(f"Module already registered: {mod.name}")
        self._modules[mod.name] = mod

    def allow_external(self, name: str) -> None:
        self._external_allowlist.add(name)

    def list_modules(self) -> List[str]:
        return list(self._modules.keys())

    def validate_proposal(self, spin_off: List[str]) -> Dict[str, Any]:
        # check modules exist
        missing = [m for m in spin_off if m not in self._modules]
        if missing:
            raise SpinOffError(f"Unknown modules in proposal: {missing}")

        # check critical modules not spun off
        critical_spun = [m for m in spin_off if self._modules[m].critical]
        if critical_spun:
            raise SpinOffError(
                f"Cannot spin off critical core modules: {critical_spun}"
            )

        remaining = {n for n in self._modules if n not in spin_off}

        # ensure remaining modules have their dependencies satisfied
        unresolved = []
        for name in remaining:
            mod = self._modules[name]
            for dep in mod.depends_on:
                # dependency can be another module name or external name
                if dep in remaining:
                    continue
                if dep in self._external_allowlist:
                    continue
                # if dependency is being spun off, core weakened
                if dep in spin_off:
                    unresolved.append((name, dep, "dep_spun_off"))
                else:
                    unresolved.append((name, dep, "unknown"))

        if unresolved:
            raise SpinOffError(
                f"Remaining core has unresolved dependencies: {unresolved}"
            )

        # ensure spun-off modules declare explicit dependencies (no implicit unknowns)
        implicit = []
        for name in spin_off:
            mod = self._modules[name]
            for dep in mod.depends_on:
                if dep not in self._modules and dep not in self._external_allowlist:
                    implicit.append((name, dep))
        if implicit:
            raise SpinOffError(
                f"Spin-off modules have implicit/unknown dependencies: {implicit}"
            )

        # build manifest
        manifest: Dict[str, Any] = {
            "spin_off": list(spin_off),
            "remaining_core": sorted(list(remaining)),
            "external_allowlist": sorted(list(self._external_allowlist)),
        }
        return manifest

    def propose_spin_off(self, spin_off: List[str]) -> Dict[str, Any]:
        """Return a validated proposal manifest for the requested spin-off.

        Raises `SpinOffError` if proposal violates rules.
        """
        manifest = self.validate_proposal(spin_off)
        # Add per-module artifact summary (serializable)
        artifacts: Dict[str, Any] = {
            "spin_off_modules": [],
            "remaining_core_modules": [],
        }
        for n in manifest["spin_off"]:
            artifacts["spin_off_modules"].append(asdict(self._modules[n]))
        for n in manifest["remaining_core"]:
            artifacts["remaining_core_modules"].append(asdict(self._modules[n]))
        manifest["artifacts"] = artifacts
        return manifest
