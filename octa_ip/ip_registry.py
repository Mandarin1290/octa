from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def canonical_hash(obj) -> str:
    # Deterministic JSON hash using sorted keys
    dumped = json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(dumped.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class VersionRef:
    asset_id: str
    version: str

    def __str__(self) -> str:
        return f"{self.asset_id}@{self.version}"


@dataclass
class Version:
    version: str
    parent: Optional[str]
    created_at: str
    metadata: Dict
    dependencies: List[str] = field(default_factory=list)
    lifecycle: str = "active"  # active | deprecated
    evidence_hash: str = ""

    def to_canonical(self):
        return {
            "version": self.version,
            "parent": self.parent,
            "created_at": self.created_at,
            "metadata": self.metadata,
            "dependencies": sorted(self.dependencies),
            "lifecycle": self.lifecycle,
        }


@dataclass
class IPAsset:
    asset_id: str
    name: str
    description: Optional[str] = None
    versions: Dict[str, Version] = field(default_factory=dict)


class IPRegistryError(Exception):
    pass


class IPRegistry:
    """Registry for IP assets and immutable version lineage.

    - Assets identified by `asset_id` (string).
    - Each version is immutable once created.
    - Dependencies recorded as strings `asset_id@version`.
    - Provides dependency graph utilities and lifecycle state management.
    """

    def __init__(self) -> None:
        self._assets: Dict[str, IPAsset] = {}

    def add_asset(
        self,
        name: str,
        asset_id: Optional[str] = None,
        description: Optional[str] = None,
    ) -> str:
        aid = asset_id or str(uuid.uuid4())
        if aid in self._assets:
            raise IPRegistryError(f"Asset id already exists: {aid}")
        self._assets[aid] = IPAsset(asset_id=aid, name=name, description=description)
        return aid

    def get_asset(self, asset_id: str) -> IPAsset:
        try:
            return self._assets[asset_id]
        except KeyError:
            raise IPRegistryError(f"Unknown asset: {asset_id}") from None

    def add_version(
        self,
        asset_id: str,
        version: str,
        parent: Optional[str] = None,
        metadata: Optional[Dict] = None,
        dependencies: Optional[List[str]] = None,
    ) -> str:
        metadata = metadata or {}
        dependencies = dependencies or []
        asset = self.get_asset(asset_id)
        if version in asset.versions:
            # Enforce immutability: cannot modify existing version
            raise IPRegistryError(
                f"Version already exists and is immutable: {asset_id}@{version}"
            )

        # Validate parent exists if provided
        if parent is not None and parent not in asset.versions:
            raise IPRegistryError(f"Parent version not found for {asset_id}: {parent}")

        # Validate dependency format and existence
        for dep in dependencies:
            if "@" not in dep:
                raise IPRegistryError(
                    f"Dependency must be in form asset_id@version: {dep}"
                )
            dep_aid, dep_ver = dep.split("@", 1)
            if dep_aid not in self._assets:
                raise IPRegistryError(f"Dependency asset not found: {dep_aid}")
            if dep_ver not in self._assets[dep_aid].versions:
                raise IPRegistryError(f"Dependency version not found: {dep}")

        created_at = _now_iso()
        v = Version(
            version=version,
            parent=parent,
            created_at=created_at,
            metadata=metadata,
            dependencies=list(dependencies),
        )
        # Compute evidence hash
        v.evidence_hash = canonical_hash({"asset_id": asset_id, **v.to_canonical()})
        asset.versions[version] = v

        # Validate no cycles introduced
        if self._has_cycle():
            # rollback
            del asset.versions[version]
            raise IPRegistryError(
                f"Adding version introduces dependency cycle: {asset_id}@{version}"
            )

        return v.evidence_hash

    def deprecate_version(self, asset_id: str, version: str) -> None:
        asset = self.get_asset(asset_id)
        if version not in asset.versions:
            raise IPRegistryError(f"Unknown version: {asset_id}@{version}")
        asset.versions[version].lifecycle = "deprecated"

    def list_assets(self) -> List[str]:
        return list(self._assets.keys())

    def get_version(self, asset_id: str, version: str) -> Version:
        asset = self.get_asset(asset_id)
        try:
            return asset.versions[version]
        except KeyError:
            raise IPRegistryError(f"Unknown version: {asset_id}@{version}") from None

    def dependency_graph(self) -> Dict[str, List[str]]:
        # Nodes are strings asset_id@version
        graph: Dict[str, List[str]] = {}
        for aid, asset in self._assets.items():
            for ver, v in asset.versions.items():
                node = f"{aid}@{ver}"
                graph.setdefault(node, [])
                for dep in v.dependencies:
                    graph[node].append(dep)
                # parent creates an implicit dependency
                if v.parent is not None:
                    graph[node].append(f"{aid}@{v.parent}")
        return graph

    def _has_cycle(self) -> bool:
        graph = self.dependency_graph()
        visited: Set[str] = set()
        stack: Set[str] = set()

        def visit(node: str) -> bool:
            if node in stack:
                return True
            if node in visited:
                return False
            visited.add(node)
            stack.add(node)
            for nb in graph.get(node, []):
                if visit(nb):
                    return True
            stack.remove(node)
            return False

        for n in graph:
            if visit(n):
                return True
        return False

    def verify_lineage(self, asset_id: str, version: str) -> bool:
        # Verify that the chain of parents exists and hashes are stable
        v = self.get_version(asset_id, version)
        # verify evidence hash matches canonical
        expected = canonical_hash({"asset_id": asset_id, **v.to_canonical()})
        if expected != v.evidence_hash:
            return False
        if v.parent is None:
            return True
        return self.verify_lineage(asset_id, v.parent)

    def topological_sort(self) -> List[str]:
        # Kahn's algorithm but interpret edges as dependency -> dependent
        # Build reverse graph: for each node -> deps, create dep -> node
        g = self.dependency_graph()
        rev: Dict[str, List[str]] = {}
        nodes: Set[str] = set()
        for n, deps in g.items():
            nodes.add(n)
            for d in deps:
                nodes.add(d)
                rev.setdefault(d, []).append(n)
        indegree: Dict[str, int] = {n: 0 for n in nodes}
        for _src, targets in rev.items():
            for t in targets:
                indegree[t] += 1

        queue = [n for n, deg in indegree.items() if deg == 0]
        order: List[str] = []
        while queue:
            n = queue.pop(0)
            order.append(n)
            for m in rev.get(n, []):
                indegree[m] -= 1
                if indegree[m] == 0:
                    queue.append(m)
        if len(order) != len(indegree):
            raise IPRegistryError(
                "Dependency graph has cycles; topological sort failed"
            )
        return order
