# Fund Entity (Logical Model)

Overview
--------
`FundEntity` is a logical, auditable representation of a fund. It is not legal advice and models the fund for internal systems.

Core fields (immutable)
------------------------
- `fund_id`, `name`
- `base_currency` — ISO currency code
- `inception_date` — ISO datetime string
- `accounting_calendar` — e.g. `monthly`, `quarterly`
- `share_classes` — mapping of share class ids to attributes (currency, shares_outstanding, fees)

Links
-----
- `attach_aum_state(aum_state)` — link to the canonical `AUMState` for live AUM queries.
- `attach_nav_engine(nav_engine)` — link to a NAV engine implementing `compute_nav(share_classes)`.

Audit & Reproducibility
-----------------------
- All attachments and NAV computations may call the provided `audit_fn` to persist events to the audit chain.
- Core fields are frozen to ensure immutable provenance.
