# Spinoff / Licensing Simulation Mode

The `SpinoffSimulator` provides a virtual extraction simulation for modules. It does NOT
perform any actual detachment or code movement — it only computes the set of modules that
would be required, missing dependencies, and a conservative list of adaptations needed.

Outputs
- `included`: lexicographically sorted list of modules included in the virtual bundle.
- `missing`: list of dependencies referenced but not present in the `ModuleMap`.
- `adaptations`: human-readable items describing required changes (e.g., missing dependencies,
  cross-owner coupling to resolve, vendorization suggestions).

Usage
```
from octa_ip.spinoff_simulator import SpinoffSimulator
sim = SpinoffSimulator()
report = sim.simulate(module_map, "octa_alpha")
```

Notes
- The simulator is deliberately conservative: any missing data or cross-owner use is surfaced
  as an adaptation. Manual review is required before any real extraction.
