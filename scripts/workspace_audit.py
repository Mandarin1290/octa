from __future__ import annotations

import argparse
import ast
import hashlib
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Set, Tuple

ROOT = Path(".")
REPORTS_DIR = Path("artifacts") / "reports"
ARCHIVE_ROOT = Path("tools") / "archive"


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d")


def _iter_py_files() -> List[Path]:
    return [p for p in ROOT.rglob("*.py") if ".venv" not in p.parts and "__pycache__" not in p.parts]


def _hash_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        while True:
            chunk = fh.read(8192)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def _extract_imports(path: Path) -> Set[str]:
    imports: Set[str] = set()
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"))
    except Exception:
        return imports
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                imports.add(alias.name)
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                imports.add(node.module)
    return imports


def _module_name(path: Path) -> str:
    rel = path.with_suffix("")
    parts = rel.parts
    return ".".join(parts)


def _build_import_index(files: List[Path]) -> Set[str]:
    imported: Set[str] = set()
    for path in files:
        for mod in _extract_imports(path):
            imported.add(mod)
    return imported


def _find_orphans(files: List[Path], imported: Set[str]) -> List[Path]:
    orphans: List[Path] = []
    for path in files:
        if path.name == "__init__.py":
            continue
        mod = _module_name(path)
        if mod not in imported:
            orphans.append(path)
    return orphans


def _find_duplicates(files: List[Path]) -> Dict[str, List[Path]]:
    by_stem: Dict[str, List[Path]] = defaultdict(list)
    for path in files:
        by_stem[path.stem].append(path)
    return {stem: paths for stem, paths in by_stem.items() if len(paths) > 1}


def _find_near_duplicates(files: List[Path]) -> Dict[str, List[Path]]:
    hashes: Dict[str, List[Path]] = defaultdict(list)
    for path in files:
        try:
            hashes[_hash_file(path)].append(path)
        except Exception:
            continue
    return {h: paths for h, paths in hashes.items() if len(paths) > 1}


def _write_report(
    *,
    report_path: Path,
    duplicates: Dict[str, List[Path]],
    hash_dupes: Dict[str, List[Path]],
    orphans: List[Path],
) -> None:
    lines: List[str] = []
    lines.append(f"workspace_audit: {datetime.now(timezone.utc).isoformat()}")
    lines.append("")

    lines.append("## Safe candidates to archive")
    if not orphans:
        lines.append("- None detected")
    else:
        for path in sorted(orphans):
            lines.append(f"- {path}")
    lines.append("")

    lines.append("## Potential merge targets")
    if not duplicates:
        lines.append("- None detected")
    else:
        for stem, paths in sorted(duplicates.items()):
            lines.append(f"- {stem}:")
            for path in paths:
                lines.append(f"  - {path}")
    lines.append("")

    lines.append("## Near-duplicate files (hash match)")
    if not hash_dupes:
        lines.append("- None detected")
    else:
        for _, paths in sorted(hash_dupes.items()):
            lines.append("- Group:")
            for path in paths:
                lines.append(f"  - {path}")
    lines.append("")

    lines.append("## Risky items – do not touch")
    lines.append("- Core pipeline modules under octa/core/orchestration/")
    lines.append("- Gate implementations under octa/core/gates/")
    lines.append("- Data providers/loaders under octa/core/data/")

    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _apply_archive(paths: Iterable[Path], tag: str) -> None:
    archive_root = ARCHIVE_ROOT / tag
    archive_root.mkdir(parents=True, exist_ok=True)

    for path in paths:
        rel = path.relative_to(ROOT)
        dest = archive_root / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(path.read_bytes())
        shim = _shim_text(rel, tag)
        path.write_text(shim, encoding="utf-8")


def _shim_text(rel: Path, tag: str) -> str:
    module_path = rel.with_suffix("").as_posix().replace("/", ".")
    return (
        "# Auto-generated shim. Original archived for audit safety.\n"
        f"from tools.archive.{tag}.{module_path} import *  # type: ignore\n"
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="Move candidates to archive and leave shims")
    args = parser.parse_args()

    files = _iter_py_files()
    imported = _build_import_index(files)
    orphans = _find_orphans(files, imported)
    duplicates = _find_duplicates(files)
    hash_dupes = _find_near_duplicates(files)

    report_path = REPORTS_DIR / f"workspace_audit_{_now_tag()}.md"
    _write_report(
        report_path=report_path,
        duplicates=duplicates,
        hash_dupes=hash_dupes,
        orphans=orphans,
    )

    if args.apply:
        _apply_archive(orphans, _now_tag())

    print(f"report: {report_path}")


if __name__ == "__main__":
    main()
