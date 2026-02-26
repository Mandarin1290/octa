from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, List, Optional

try:
    from pydantic.v1 import BaseModel, Field, validator
except Exception:  # pragma: no cover
    from pydantic.v1 import BaseModel, Field, validator


# ---------------------------------------------------------------------------
# New submodels (Phase C additions)
# ---------------------------------------------------------------------------


class CalendarSpec(BaseModel):
    """Trading calendar parameters — drives annualisation and walk-forward window sizing."""

    trading_days_per_year: int = 252  # 252=equities, 261=FX, 365=crypto
    session_start_utc: Optional[str] = None  # e.g. "14:30"
    session_end_utc: Optional[str] = None    # e.g. "21:00"
    is_24_7: bool = False   # True for crypto
    is_24_5: bool = False   # True for FX spot


class AltDataSpec(BaseModel):
    """AltData source requirements for this asset profile."""

    required_sources: List[str] = Field(default_factory=list)
    optional_sources: List[str] = Field(default_factory=list)
    # gate_layer → list of builder module short names (overrides default dispatch)
    dispatch_overrides: Dict[str, List[str]] = Field(default_factory=dict)


class CryptoExt(BaseModel):
    """Crypto-specific extension parameters."""

    onchain_required_columns: List[str] = Field(default_factory=lambda: ["funding_rate"])
    allow_missing_onchain: bool = True
    funding_rate_threshold: float = 0.001


class FxExt(BaseModel):
    """FX-specific extension parameters."""

    base_currency: Optional[str] = None
    quote_currency: Optional[str] = None
    eco_calendar_required: bool = False
    cot_required: bool = False


class FutureExt(BaseModel):
    """Futures-specific extension parameters."""

    root_symbol: Optional[str] = None
    roll_window_days: int = 5
    oi_multiplier_trigger: float = 1.2
    fold_boundary_roll_buffer_bars: int = 5
    basis_required: bool = True
    back_adjust_method: str = "ratio"  # "additive" | "ratio" | "panama"
    required_parquet_columns: List[str] = Field(
        default_factory=lambda: ["open", "high", "low", "close", "volume", "roll_flag"]
    )


class OptionExt(BaseModel):
    """Options-specific extension parameters."""

    underlying_symbol: Optional[str] = None
    required_parquet_columns: List[str] = Field(
        default_factory=lambda: [
            "close", "bid", "ask", "strike", "expiry", "option_type",
            "delta", "iv", "tte_days", "moneyness", "underlying_close",
        ]
    )
    lstm_seq_len: int = 32
    lstm_features: List[str] = Field(
        default_factory=lambda: [
            "delta", "gamma", "vega", "theta", "moneyness",
            "tte_days", "mid", "underlying_close", "iv",
        ]
    )
    model_format: str = "h5"  # "h5" | "cbm"
    require_underlying_cascade_pass: bool = True
    gate_layer: str = "structure_30m"
    performance_threshold_auc: float = 0.55


class CostModel(BaseModel):
    # Optional: allows per-asset cost overrides (not yet wired into broker/signal).
    cost_bps: Optional[float] = None
    spread_bps: Optional[float] = None
    stress_cost_multiplier: Optional[float] = None


class DataTruth(BaseModel):
    # Optional: for data-integrity checks / assumptions.
    require_monotonic_timestamps: Optional[bool] = None
    allow_negative_prices: Optional[bool] = None


class Liquidity(BaseModel):
    enabled: Optional[bool] = None
    min_adv_usd: Optional[float] = None
    min_adv_shares: Optional[float] = None
    min_history_days: Optional[int] = None


class CorporateActions(BaseModel):
    # Optional: for equities adjustments assumptions.
    split_adjusted: Optional[bool] = None
    dividend_adjusted: Optional[bool] = None


class AssetProfile(BaseModel):
    """Asset-specific policy bundle.

    Notes:
    - This is intentionally lightweight: we focus on gates first.
    - Additional sub-config sections exist to support future policy expansion.
    """

    name: str
    kind: str = "legacy"  # stock/index/fx/crypto/future/option/legacy

    # Gate overrides in the *same shape* as cfg.gates fragments.
    # Supported keys:
    # - global: dict of GateSpec fields
    # - global_by_timeframe: {"1D"|"1H"|"30m"|"5m"|"1m": {GateSpec fields}}
    gates: Dict[str, Any] = Field(default_factory=dict)

    cost_model: CostModel = Field(default_factory=CostModel)
    data_truth: DataTruth = Field(default_factory=DataTruth)
    liquidity: Liquidity = Field(default_factory=Liquidity)
    corporate_actions: CorporateActions = Field(default_factory=CorporateActions)

    # Phase C additions — optional; defaults preserve backward compatibility
    calendar: CalendarSpec = Field(default_factory=CalendarSpec)
    altdata: AltDataSpec = Field(default_factory=AltDataSpec)
    # Asset-class-specific extension: populated by resolve_asset_profile()
    asset_class_ext: Optional[Dict[str, Any]] = None

    @validator("kind", pre=True)
    def _norm_kind(cls, v: Any) -> str:
        if v is None:
            return "legacy"
        s = str(v).strip().lower()
        aliases = {
            "equity": "stock",
            "equities": "stock",
            "stocks": "stock",
            "indices": "index",
            "forex": "fx",
            "futures": "future",
            "options": "option",
        }
        return aliases.get(s, s)


class AssetDefaults(BaseModel):
    default_profile: str = "legacy"
    by_dataset: Dict[str, str] = Field(default_factory=dict)
    by_asset_class: Dict[str, str] = Field(default_factory=dict)


def _stable_json_dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def profile_hash(profile_name: str, applied_thresholds: Dict[str, Any]) -> str:
    payload = {"profile": str(profile_name), "applied_thresholds": applied_thresholds}
    raw = _stable_json_dumps(payload).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


# Canonical profile name for stocks (centralized)
STOCKS_PROFILE_NAME = "stock"


class AssetProfileMismatchError(RuntimeError):
    """Raised when a dataset requires a specific asset profile but a different
    profile was resolved.

    Attributes:
        profile_name: resolved profile name (str)
        profile_hash: computed profile hash (str) or None
        gate_version: optional gate_version observed/applied
    """

    def __init__(self, msg: str, profile_name: Optional[str] = None, profile_hash: Optional[str] = None, gate_version: Optional[str] = None):
        super().__init__(msg)
        self.profile_name = profile_name
        self.profile_hash = profile_hash
        self.gate_version = gate_version


def ensure_canonical_profile_for_dataset(dataset: Optional[str], resolved: AssetProfile, applied_thresholds: Optional[Dict[str, Any]] = None, gate_version: Optional[str] = None) -> None:
    """Enforce dataset-specific canonical profile rules.

    Currently enforces that `dataset=='stocks'` must resolve to the
    canonical `STOCKS_PROFILE_NAME`. Raises AssetProfileMismatchError on violation.
    """
    try:
        ds = str(dataset or "").strip().lower()
        if ds != "stocks":
            return
    except Exception:
        return

    prof_name = str(getattr(resolved, 'name', None) or '')
    # normalize
    prof_name_norm = prof_name.strip().lower()
    if prof_name_norm != STOCKS_PROFILE_NAME:
        ph = None
        try:
            ph = profile_hash(prof_name, dict(applied_thresholds or {}))
        except Exception:
            ph = None
        raise AssetProfileMismatchError(
            f"STOCKS_PROFILE_MISMATCH: expected '{STOCKS_PROFILE_NAME}', got '{prof_name}'",
            profile_name=prof_name,
            profile_hash=ph,
            gate_version=gate_version,
        )


def resolve_asset_profile(
    *,
    symbol: str,
    dataset: Optional[str],
    asset_class: Optional[str],
    parquet_path: Optional[str],
    cfg: Any,
) -> AssetProfile:
    """Resolve the asset profile to use for this symbol.

    Backwards-compatible behavior:
    - If cfg.asset_profiles is missing/empty, returns a synthesized 'legacy' profile.

    Resolution order:
    1) cfg.asset_defaults.by_dataset[dataset]
    2) cfg.asset_defaults.by_asset_class[asset_class]
    3) cfg.asset_defaults.default_profile
    4) Heuristic fallback by dataset/asset_class
    """

    ds = str(dataset or "").strip().lower() or None
    ac = str(asset_class or "").strip().lower() or None

    # Read raw config fragments safely.
    raw_profiles = getattr(cfg, "asset_profiles", None)
    raw_defaults = getattr(cfg, "asset_defaults", None)

    defaults = None
    try:
        if isinstance(raw_defaults, AssetDefaults):
            defaults = raw_defaults
        elif isinstance(raw_defaults, dict):
            defaults = AssetDefaults.parse_obj(raw_defaults)
    except Exception:
        defaults = None

    profiles: Dict[str, AssetProfile] = {}
    try:
        if isinstance(raw_profiles, dict):
            for k, v in raw_profiles.items():
                if v is None:
                    continue
                if isinstance(v, AssetProfile):
                    profiles[str(k)] = v
                elif isinstance(v, dict):
                    vv = dict(v)
                    vv.setdefault("name", str(k))
                    profiles[str(k)] = AssetProfile.parse_obj(vv)
    except Exception:
        profiles = {}

    def _pick_name() -> str:
        if defaults is not None:
            if ds and ds in (defaults.by_dataset or {}):
                return str(defaults.by_dataset[ds])
            if ac and ac in (defaults.by_asset_class or {}):
                return str(defaults.by_asset_class[ac])
            if defaults.default_profile:
                return str(defaults.default_profile)

        # Heuristic fallback
        if ds in {"fx", "forex"} or ac in {"fx", "forex"}:
            return "fx"
        if ds in {"indices", "index"} or ac in {"index", "indices"}:
            return "index"
        if ds in {"stocks", "stock", "equities", "equity"} or ac in {"stock", "equity"}:
            return "stock"
        if ac in {"crypto"}:
            return "crypto"
        if ac in {"future", "futures"}:
            return "future"
        if ac in {"option", "options"}:
            return "option"
        return "legacy"

    name = _pick_name()

    # If profiles are not configured, synthesize a legacy profile.
    if not profiles:
        kind = name
        if kind == "legacy":
            # best-effort kind inference for audit clarity
            if ds:
                kind = ds
            elif ac:
                kind = ac
        # For stocks, never synthesize a legacy profile — prefer canonical stock profile.
        if ds == "stocks":
            return _apply_calendar_defaults(
                AssetProfile(name=STOCKS_PROFILE_NAME, kind=STOCKS_PROFILE_NAME, gates={})
            )
        return _apply_calendar_defaults(AssetProfile(name="legacy", kind=str(kind), gates={}))

    prof = profiles.get(name)
    if prof is not None:
        return _apply_calendar_defaults(prof)

    # If a named profile is missing, fail-closed by falling back to legacy.
    # (We do not raise here because we want diagnose flows to continue.)
    kind = name
    if kind == "legacy":
        if ds:
            kind = ds
        elif ac:
            kind = ac
    # If dataset is stocks, prefer the canonical stock profile instead of legacy.
    if ds == "stocks":
        return _apply_calendar_defaults(
            AssetProfile(name=STOCKS_PROFILE_NAME, kind=STOCKS_PROFILE_NAME, gates={})
        )
    return _apply_calendar_defaults(AssetProfile(name="legacy", kind=str(kind), gates={}))


def _apply_calendar_defaults(profile: AssetProfile) -> AssetProfile:
    """Set CalendarSpec defaults based on asset kind when not already configured."""
    kind = (profile.kind or "legacy").lower()
    cal = profile.calendar
    # Only override if the calendar is still at its default (252, non-24x)
    if cal.trading_days_per_year == 252 and not cal.is_24_7 and not cal.is_24_5:
        if kind in {"crypto"}:
            try:
                profile = profile.copy(update={"calendar": CalendarSpec(trading_days_per_year=365, is_24_7=True)})
            except Exception:
                pass
        elif kind in {"fx", "forex"}:
            try:
                profile = profile.copy(update={"calendar": CalendarSpec(trading_days_per_year=261, is_24_5=True)})
            except Exception:
                pass
    return profile
