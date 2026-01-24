# Externalization Readiness Scanner

This document describes the heuristics used by `ExternalizationScanner`.

Principles
- Conservative: only fully isolated modules are eligible for externalization/licensing.
- Auditable: heuristics are simple and based on repository metadata and lightweight static checks.

Heuristics
- Dependency purity: modules with zero internal dependencies score highest. The scanner computes a
  `purity_score` in [0,1] where 1 means no internal deps and no leakage.
- Data leakage checks: files are scanned for suspicious patterns (e.g., `os.environ`, `requests`,
  `boto3`, `open(`, database connectors). Any match flags the module as potentially leaking secrets
  or depending on environment-specific resources.
- Execution coupling: modules that are depended upon by other modules or that depend on many modules
  receive a lower coupling score.

Readiness rules (conservative)
- A module is `ready` only if:
  - It has no internal dependencies.
  - No other internal modules depend on it.
  - No leakage indicators are found when scanning files.

Usage
- Instantiate `octa_ip.externalization_scan.ExternalizationScanner`, build a `ModuleMap`, and call
  `analyze_module(mm, module_name)` or `scan_all(mm)` to obtain `ExternalizationReport` objects.

Limitations
- This is a heuristic scanner. For final legal/licensing decisions, manual review and SPDX/license
  scanning is required.
