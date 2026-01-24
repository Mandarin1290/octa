from __future__ import annotations

import ast
from pathlib import Path


FORBIDDEN_PREFIXES = (
    "okta_altdat",
    "octa_training",
    "octa_stream",
    "octa_altdata",
    "octa_core",
)


def _collect_imports(node: ast.AST, in_type_checking: bool, found: list[tuple[int, str]]) -> None:
    if isinstance(node, ast.If) and isinstance(node.test, ast.Name) and node.test.id == "TYPE_CHECKING":
        for child in node.body:
            _collect_imports(child, True, found)
        for child in node.orelse:
            _collect_imports(child, in_type_checking, found)
        return

    if isinstance(node, ast.Import) and not in_type_checking:
        for alias in node.names:
            found.append((node.lineno, alias.name))
    elif isinstance(node, ast.ImportFrom) and not in_type_checking:
        if node.module:
            found.append((node.lineno, node.module))

    for child in ast.iter_child_nodes(node):
        _collect_imports(child, in_type_checking, found)


def test_core_has_no_legacy_imports() -> None:
    offenders: list[str] = []
    for path in Path("octa/core").rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        tree = ast.parse(text, filename=str(path))
        imports: list[tuple[int, str]] = []
        _collect_imports(tree, False, imports)
        for lineno, mod in imports:
            if mod.startswith(FORBIDDEN_PREFIXES):
                offenders.append(f"{path}:{lineno}:{mod}")

    assert not offenders, "Legacy imports found in octa/core:\\n" + "\\n".join(offenders)
