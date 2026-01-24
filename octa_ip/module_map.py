import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Set, Tuple


@dataclass
class ModuleInfo:
    name: str
    owner: str
    classification: str  # 'core' | 'licensable' | 'internal'
    files: Set[str] = field(default_factory=set)


class ModuleMap:
    def __init__(self):
        self.modules: Dict[str, ModuleInfo] = {}
        # dependencies: module -> set(modules it depends on)
        self.deps: Dict[str, Set[str]] = {}

    def add_module(self, name: str, owner: str, classification: str = "internal"):
        if name in self.modules:
            raise KeyError(f"module {name} already registered")
        self.modules[name] = ModuleInfo(
            name=name, owner=owner, classification=classification
        )
        self.deps.setdefault(name, set())

    def add_file_to_module(self, module: str, filepath: str):
        if module not in self.modules:
            raise KeyError(module)
        self.modules[module].files.add(filepath)

    def add_dependency(self, src: str, dst: str):
        if src not in self.deps:
            self.deps[src] = set()
        self.deps[src].add(dst)

    def detect_violations(
        self, allowed_cross: Set[Tuple[str, str]] | None = None
    ) -> List[Tuple[str, str, str]]:
        """Detect violations. Returns list of tuples (src, dst, reason).

        Rule: Any dependency between modules with different owners is a potential violation
        unless explicitly allowed by `allowed_cross`. Additionally, no module should depend on
        an `internal` classification module owned by someone else.
        """
        allowed_cross = allowed_cross or set()
        violations = []
        for src, dsts in self.deps.items():
            for dst in dsts:
                if src == dst:
                    continue
                if (src, dst) in allowed_cross:
                    continue
                if src not in self.modules or dst not in self.modules:
                    # unknown module reference
                    violations.append((src, dst, "unknown-module"))
                    continue
                ms = self.modules[src]
                md = self.modules[dst]
                if ms.owner != md.owner:
                    # cross-owner dependency
                    if md.classification == "internal":
                        violations.append((src, dst, "cross-owner-internal"))
                    else:
                        violations.append((src, dst, "cross-owner"))
        return violations

    def build_from_files(self, root: str, package_prefix: str = "octa_"):
        """Scan .py files under root to build modules and dependencies using import heuristics.

        Modules are top-level package names starting with `package_prefix` (e.g., octa_alpha).
        """
        rootp = Path(root)
        py_files = list(rootp.rglob("*.py"))
        import_re = re.compile(r"^(?:from|import)\s+([a-zA-Z0-9_\.]+)")
        for fp in py_files:
            try:
                text = fp.read_text()
            except Exception:
                continue
            # find imports
            for line in text.splitlines():
                m = import_re.match(line.strip())
                if not m:
                    continue
                pkg = m.group(1).split(".")[0]
                if pkg.startswith(package_prefix):
                    modname = pkg
                    # ensure module exists (owner unknown)
                    if modname not in self.modules:
                        self.add_module(
                            modname, owner="UNKNOWN", classification="internal"
                        )
                    # record file under module
                    self.add_file_to_module(modname, str(fp))
            # also attribute the file to its containing top-level module if path contains octa_x
            parts = fp.parts
            for part in parts:
                if part.startswith(package_prefix):
                    modname = part
                    if modname not in self.modules:
                        self.add_module(
                            modname, owner="UNKNOWN", classification="internal"
                        )
                    self.add_file_to_module(modname, str(fp))
                    break
        # derive dependencies by scanning imports again and attributing based on file owner
        for mod in self.modules.values():
            for f in list(mod.files):
                try:
                    text = Path(f).read_text()
                except Exception:
                    continue
                for line in text.splitlines():
                    m = import_re.match(line.strip())
                    if not m:
                        continue
                    pkg = m.group(1).split(".")[0]
                    if pkg.startswith(package_prefix):
                        self.add_dependency(mod.name, pkg)

    def dependency_graph(self) -> Dict[str, Set[str]]:
        return dict(self.deps)


__all__ = ["ModuleMap", "ModuleInfo"]
