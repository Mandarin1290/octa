# Rates & Bonds Model

Overview
--------
This module models interest rate and bond exposure (duration, DV01, curve risk). It provides conservative proxies and stress transforms to approximate curve moves.

Files
-----
- `octa_assets/rates/duration.py` — `BondSpec` with duration/modified_duration and convexity.
- `octa_assets/rates/dv01.py` — `bond_dv01()` and aggregation with cap for missing duration proxies.
- `octa_assets/rates/curve.py` — `CurveBuckets` with bucketization and stress transforms (parallel, steepen).

Guidance
--------
- If a bond lacks a duration proxy, use a conservative cap rather than failing silently.
- Curve stresses include parallel shifts and steepening/flattening; apply stress to DV01 exposures conservatively.
