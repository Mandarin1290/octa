# Volatility Model

Overview
--------
Volatility instruments are modeled as risk factors not price assets. Term structure (contango/backwardation) and convexity exposure determine risk characteristics. VIX futures and Vol ETFs carry convexity and decay risks that must be explicitly tracked.

Files
-----
- `octa_assets/vol/term_structure.py` — utilities to detect contango/backwardation.
- `octa_assets/vol/convexity.py` — `ConvexityTracker` and `VolPosition` for convexity exposure aggregation and short-vol cap enforcement.

Gamma / Convexity
-----------------
- Long vol positions have positive convexity; short vol positions have negative convexity.
- Aggregate convexity is a proxy to understand portfolio sensitivity to volatility moves.

Risk Gates
----------
- Short-vol cap enforcement triggers sentinel gates when exceeded.
- Term structure alerts inform execution logic (avoid aggressive shorting in deep contango for ETFs with high decay).
