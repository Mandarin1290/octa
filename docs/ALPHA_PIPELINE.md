# Alpha Generation Pipeline

This module defines the canonical alpha-generation pipeline. It enforces a
strict stage order and prevents bypass by requiring an internal run context
token for stage execution. Alpha sources must propose hypotheses and be run
through `AlphaPipeline.run()`.

Pipeline stages (in order)
1) Hypothesis definition
2) Feature eligibility check
3) Data sufficiency gate
4) Signal construction
5) Risk pre-screen
6) Paper deployment

Reproducibility
---------------
All deterministic choices (sorting, Decimal quantization, UUIDs used only for
identifiers) ensure reproducibility when the same inputs are provided.
