import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Set

from .module_map import ModuleMap


@dataclass
class ExternalizationReport:
    module: str
    purity_score: float
    leakage_flags: List[str]
    coupling_score: float
    ready: bool
    reasons: List[str]


class ExternalizationScanner:
    """Conservative scanner to assess externalization readiness.

    Only fully isolated modules are considered eligible. The scanner uses
    simple, auditable heuristics: dependency purity, file-based leakage
    indicators, and execution coupling signs.
    """

    SUSPICIOUS_PATTERNS = [
        r"os\.environ",
        r"boto3",
        r"requests",
        r"subprocess",
        r"socket",
        r"open\(",
        r"sqlite3",
        r"psycopg2",
        r"pymongo",
        r"__main__",
    ]

    def __init__(self):
        self.patterns = [re.compile(p) for p in self.SUSPICIOUS_PATTERNS]

    def analyze_module(self, mm: ModuleMap, module_name: str) -> ExternalizationReport:
        if module_name not in mm.modules:
            raise KeyError(module_name)

        total_modules = max(1, len(mm.modules))
        deps: Set[str] = set(mm.deps.get(module_name, set()))
        reverse_deps = {m for m, ds in mm.deps.items() if module_name in ds}

        # Dependency purity score: 1.0 when no deps, else scaled
        dep_score = 1.0 - (len(deps) / max(1, total_modules - 1))

        # Execution coupling: penalize reverse deps and presence of deps
        coupling_penalty = (len(deps) + len(reverse_deps)) / max(1, total_modules)
        coupling_score = max(0.0, 1.0 - coupling_penalty)

        # Data leakage: scan files for suspicious patterns
        minfo = mm.modules[module_name]
        leakage_flags: List[str] = []
        checked_files = 0
        for f in list(minfo.files):
            try:
                text = Path(f).read_text()
            except Exception:
                # file unreadable — conservative flag
                leakage_flags.append("unreadable_file")
                continue
            checked_files += 1
            for pat in self.patterns:
                if pat.search(text):
                    leakage_flags.append(f"pattern:{pat.pattern}")

        # Normalize leakage flags
        leakage_flags = sorted(set(leakage_flags))

        # Purity score combines dep_score and leakage absence; heavily penalize leakage
        leakage_penalty = 0.0 if not leakage_flags else 1.0
        purity_score = max(0.0, min(1.0, dep_score * (1.0 - leakage_penalty)))

        # Readiness: conservative rules
        reasons: List[str] = []
        ready = True
        if deps:
            ready = False
            reasons.append("has_dependencies")
        if reverse_deps:
            ready = False
            reasons.append("is_depended_on")
        if leakage_flags:
            ready = False
            reasons.append("leakage_indicators")
        if purity_score < 1.0:
            ready = False

        return ExternalizationReport(
            module=module_name,
            purity_score=purity_score,
            leakage_flags=leakage_flags,
            coupling_score=coupling_score,
            ready=ready,
            reasons=reasons,
        )

    def scan_all(self, mm: ModuleMap) -> Dict[str, ExternalizationReport]:
        reports: Dict[str, ExternalizationReport] = {}
        for m in mm.modules:
            reports[m] = self.analyze_module(mm, m)
        return reports


__all__ = ["ExternalizationScanner", "ExternalizationReport"]
