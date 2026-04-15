from math import ceil
from statistics import NormalDist, mean, pstdev
from typing import Any, Dict, List, Optional


def estimate_sample_size(
    effect_size: float, sigma: float, alpha: float = 0.05, power: float = 0.8
) -> int:
    """Estimate required sample size (one-sample) for detecting `effect_size`.

    Uses normal approximation: n = ((Z_{1-alpha/2} + Z_{power}) * sigma / effect_size)^2
    Returns integer sample size (rounded up).
    """
    if effect_size <= 0 or sigma <= 0:
        raise ValueError("effect_size and sigma must be positive")
    z_alpha = NormalDist().inv_cdf(1 - alpha / 2)
    z_beta = NormalDist().inv_cdf(power)
    n = ((z_alpha + z_beta) * sigma / effect_size) ** 2
    return int(ceil(n))


def confidence_interval(
    mean_val: float, std: float, n: int, alpha: float = 0.05
) -> Dict[str, float]:
    """Return two-sided confidence interval for the mean (normal approximation).

    Returns dict with (lower, upper, margin).
    """
    if n <= 0:
        raise ValueError("n must be positive")
    z = NormalDist().inv_cdf(1 - alpha / 2)
    se = float(std) / (n**0.5)
    margin = z * se
    return {"lower": mean_val - margin, "upper": mean_val + margin, "margin": margin}


def validate_regime_coverage(
    regimes: List[str], required: Optional[List[str]] = None, min_fraction: float = 0.1
) -> Dict[str, Any]:
    """Validate that data covers required regimes and meets minimal regime fraction.

    `regimes` is a list indicating regime label per sample. `required` is a list
    of regimes that must appear. `min_fraction` is minimal fraction of samples
    that must belong to each required regime.
    """
    total = len(regimes)
    counts: Dict[str, int] = {}
    for r in regimes:
        counts[r] = counts.get(r, 0) + 1
    report = {r: counts.get(r, 0) / total if total > 0 else 0 for r in counts}
    missing = []
    insufficient = []
    if required:
        for r in required:
            if counts.get(r, 0) == 0:
                missing.append(r)
            else:
                frac = counts[r] / total
                if frac < min_fraction:
                    insufficient.append((r, frac))
    return {
        "total": total,
        "counts": counts,
        "report": report,
        "missing": missing,
        "insufficient": insufficient,
    }


def is_data_sufficient(
    values: List[float],
    regimes: Optional[List[str]] = None,
    effect_size: Optional[float] = None,
    sigma: Optional[float] = None,
    alpha: float = 0.05,
    power: float = 0.8,
    required_regimes: Optional[List[str]] = None,
    min_fraction: float = 0.1,
) -> Dict[str, Any]:
    """Run composite checks: sample size, regime coverage, and confidence intervals.

    Returns dict with keys: sufficient (bool), reasons (list), details.
    """
    reasons: List[str] = []
    details: Dict[str, Any] = {}
    n = len(values)
    if n == 0:
        return {"sufficient": False, "reasons": ["no_data"], "details": {}}

    # estimate sigma if not provided
    est_sigma = sigma
    if est_sigma is None:
        if n < 2:
            reasons.append("insufficient_variance_samples")
            details["n"] = n
            return {"sufficient": False, "reasons": reasons, "details": details}
        est_sigma = pstdev(values)
    details["estimated_sigma"] = est_sigma

    if effect_size is not None:
        req_n = estimate_sample_size(
            effect_size=float(effect_size),
            sigma=float(est_sigma),
            alpha=alpha,
            power=power,
        )
        details["required_n"] = req_n
        details["observed_n"] = n
        if n < req_n:
            reasons.append("underpowered")

    # regime coverage
    if regimes is not None and required_regimes is not None:
        cov = validate_regime_coverage(
            regimes, required=required_regimes, min_fraction=min_fraction
        )
        details["regime_coverage"] = cov
        if cov["missing"]:
            reasons.append("insufficient_regime_data")
        if cov["insufficient"]:
            reasons.append("insufficient_regime_coverage")

    # confidence intervals around mean
    m = mean(values)
    ci = confidence_interval(m, float(est_sigma), n, alpha=alpha)
    details["mean"] = m
    details["ci"] = ci

    sufficient = len(reasons) == 0
    return {"sufficient": sufficient, "reasons": reasons, "details": details}
