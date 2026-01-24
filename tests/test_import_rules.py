import ast
import pathlib
from typing import List

ROOT = pathlib.Path(__file__).resolve().parents[1]


def _collect_imports(path: pathlib.Path) -> List[str]:
    src = path.read_text()
    tree = ast.parse(src)
    found: List[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for n in node.names:
                found.append(n.name)
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                found.append(node.module)
    return found


def test_vertex_does_not_import_stream_or_atlas() -> None:
    vertex_dir = ROOT / "octa_vertex"
    bad: List[str] = []
    for p in vertex_dir.rglob("*.py"):
        for mod in _collect_imports(p):
            if mod.startswith("octa_stream") or mod.startswith("octa_atlas"):
                bad.append(f"{p}: {mod}")
    assert not bad, (
        "octa_vertex must not import octa_stream or octa_atlas: " + ", ".join(bad)
    )
