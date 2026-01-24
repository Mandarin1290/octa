"""okta_altdat: additive alternative-data platform.

This package is designed to be safe-by-default:
- Disabled unless explicitly enabled via config/env.
- Never blocks core training when deps or sources are unavailable.
- Enforces backward as-of joins to prevent lookahead bias.
"""
