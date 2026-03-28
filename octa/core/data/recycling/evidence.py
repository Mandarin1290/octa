from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .common import environment_snapshot, git_snapshot, sha256_file, stable_hash, utc_now_compact, write_json, write_text


@dataclass(frozen=True)
class RunContext:
    run_id: str
    run_dir: Path
    output_dir: Path
    dry_run: bool


class EvidencePack:
    def __init__(self, evidence_root: Path, output_root: Path, *, run_id: str | None, dry_run: bool) -> None:
        resolved_run_id = run_id or f"parquet_recycling_{utc_now_compact()}"
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

    def write_json(self, name: str, payload: Any) -> Path:
        path = self.ctx.run_dir / name
        if self.ctx.dry_run:
            return path
        write_json(path, payload)
        self._hash_rows.append(f"{sha256_file(path)}  {path.name}")
        return path

    def write_text(self, name: str, text: str) -> Path:
        path = self.ctx.run_dir / name
        if self.ctx.dry_run:
            return path
        write_text(path, text)
        self._hash_rows.append(f"{sha256_file(path)}  {path.name}")
        return path

    def bootstrap(self, *, config_snapshot: dict[str, Any], input_manifest: dict[str, Any]) -> None:
        self.write_json(
            "run_manifest.json",
            {
                "run_id": self.ctx.run_id,
                "dry_run": self.ctx.dry_run,
                "started_at": utc_now_compact(),
                "config_hash": stable_hash(config_snapshot),
                "input_manifest_hash": stable_hash(input_manifest),
            },
        )
        self.write_json("config_snapshot.json", config_snapshot)
        self.write_json("environment_snapshot.json", environment_snapshot())
        self.write_text("git_snapshot.txt", "\n".join(f"{k}: {v}" for k, v in git_snapshot().items()))
        self.write_json("input_manifest.json", input_manifest)

    def finalize(self, summary_markdown: str) -> None:
        self.write_text("summary.md", summary_markdown)
        if self.ctx.dry_run:
            # No artifacts written; hashes.sha256 intentionally omitted in dry-run.
            return
        hashes_path = self.ctx.run_dir / "hashes.sha256"
        hashes_path.write_text("\n".join(sorted(self._hash_rows)) + "\n", encoding="utf-8")
