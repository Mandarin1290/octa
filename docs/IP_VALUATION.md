**IP Valuation Framework (Non-Financial)**

Zweck:
- Quantifiziert OCTA's System‑Wert als IP ohne Umsatz‑ oder Multiplikatorannahmen.
- Liefert konservative, evidenzbasierte Scores und ein unveränderliches Report‑Hash zur Auditierbarkeit.

Wesentliche Metriken:
- `codebase_complexity` — Heuristische Einschätzung basierend auf Anzahl Python‑Dateien, LOC und Monolith‑Penalitäten.
- `autonomy_depth` — Grad der Automatisierung (z. B. autonome Modellrefreshes, systematisches Sunsetting, Audit‑Automatismen).
- `governance_coverage` — Abdeckungsgrad von Governance‑Reviews, Audit‑Trails und Richtlinien.
- `survivability_evidence` — Nachweisbarkeit und Persistenz von Beweisdaten (Audit‑Logs, Registry‑Evidence).

Benutzung:

```
from octa_reports.ip_valuation import ValuationFramework

vf = ValuationFramework(repo_path='.', subsystems={
    'registry': registry_instance,
    'audit': audit_instance,
    'governance': governance_instance,
    'model_refresh': model_manager,
})

report = vf.generate_report()
print(report)
```

Interpretation:
- Werte sind bewusst konservativ und vergleichbar über Zeit und Releases.
- `composite` ist eine geometrisch aggregierte Score (0–100) — niedrige Werte weisen auf fehlende evidence oder Governance.

Auditability:
- Das Modul erzeugt einen deterministischen `report_hash` (SHA‑256 über canonical JSON). Für externe Auditierung empfiehlt sich Signieren und Persistieren des Reports.

Keine finanziellen Annahmen:
- Dieses Framework enthält explizit keine Umsatzannahmen, Markt‑Multiples oder Schätzwerte für monetären Wert.
# IP Valuation Metadata

This document describes the evidence-based metrics produced by the `ValuationEngine`.

Metrics
- `usage_frequency`: normalized usage counts (module-specific calls or hits) divided by total usage across considered modules.
- `dependency_centrality`: normalized indegree (how many internal modules depend on this module) divided by (N-1).
- `risk_criticality`: conservative composite of `dependency_centrality`, owner diversity (how many distinct owners depend on the module), and `usage_frequency`.
- `revenue_relevance`: empirical revenue tied to the module (only reported when `ModuleInfo.classification` indicates `licensable` and a `revenue_map` is provided).

Design Principles
- No marketing assumptions — all metrics are derived from observable repo structure and empirical usage/revenue inputs.
- Conservative defaults: divisions guard against zero denominators; results are reproducible given the same inputs.

Usage
- Instantiate `ValuationEngine` and call `compute_metadata(module_map, usage_counts, revenue_map)`.

Limitations
- Owner diversity is approximated using `ModuleMap` owner fields.
- For deeper network centrality metrics (e.g., eigenvector centrality) integrate a graph library; this engine intentionally stays simple and auditable.
