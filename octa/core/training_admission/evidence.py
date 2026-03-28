from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from octa.core.data.recycling.common import sha256_file, stable_hash, utc_now_compact, write_json, write_text


@dataclass(frozen=True)
class RunContext:
    run_id: str
    run_dir: Path
    output_dir: Path
    dry_run: bool


class EvidencePack:
    def __init__(self, evidence_root: Path, output_root: Path, *, run_id: str | None, dry_run: bool) -> None:
        resolved_run_id = run_id or f"training_admission_{utc_now_compact()}"
        self.ctx = RunContext(
            run_id=resolved_run_id,
            run_dir=evidence_root / resolved_run_id,
            output_dir=output_root / resolved_run_id,
            dry_run=dry_run,
        )
        if not dry_run:
            self.ctx.run_dir.mkdir(parents=True, exist_ok=False)
            self.ctx.output_dir.mkdir(parents=True, exist_ok=True)
        self._hash_rows: list[str] = []

    def write_json(self, name: str, payload: Any, *, output_copy: bool = False) -> Path:
        path = self.ctx.run_dir / name
        if self.ctx.dry_run:
            return path
        write_json(path, payload)
        self._hash_rows.append(f"{sha256_file(path)}  {path.name}")
        if output_copy:
            write_json(self.ctx.output_dir / name, payload)
        return path

    def write_text(self, name: str, text: str, *, output_copy: bool = False) -> Path:
        path = self.ctx.run_dir / name
        if self.ctx.dry_run:
            return path
        write_text(path, text)
        self._hash_rows.append(f"{sha256_file(path)}  {path.name}")
        if output_copy:
            write_text(self.ctx.output_dir / name, text)
        return path

    def bootstrap(self, *, config_snapshot: dict[str, Any], input_manifest: dict[str, Any], reference_time: str) -> None:
        self.write_json(
            "admission_run_manifest.json",
            {
                "run_id": self.ctx.run_id,
                "dry_run": self.ctx.dry_run,
                "started_at": reference_time,
                "config_hash": stable_hash(config_snapshot),
                "input_manifest_hash": stable_hash(input_manifest),
                "layer": "training_admission",
                "offline_scope_only": True,
                "no_auto_promotion": True,
            },
        )
        self.write_json("admission_config_snapshot.json", config_snapshot)
        self.write_json("admission_input_manifest.json", input_manifest)

    def finalize(self, summary_markdown: str) -> None:
        self.write_text("admission_summary.md", summary_markdown)
        if self.ctx.dry_run:
            return
        hashes_path = self.ctx.run_dir / "hashes.sha256"
        hashes_path.write_text("\n".join(sorted(self._hash_rows)) + "\n", encoding="utf-8")
