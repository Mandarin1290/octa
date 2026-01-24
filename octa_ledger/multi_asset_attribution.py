from typing import Any, Dict


def attribute_pnl(
    pnl_by_strategy_asset: Dict[str, Dict[str, Dict[str, Any]]],
    fx_rates: Dict[str, float],
    hedges: Dict[str, Dict[str, Dict[str, Any]]] | None = None,
) -> Dict[str, Any]:
    """Compute multi-asset performance attribution.

    Inputs:
      pnl_by_strategy_asset: {strategy: {asset_class: {'pnl_local': float, 'currency': str}}}
      fx_rates: {currency: rate_to_base}
      hedges: same structure as pnl_by_strategy_asset for hedge PnL (optional)

    Outputs:
      { 'asset_class_pnl': {asset: pnl_base},
        'strategy_asset_matrix': {strategy: {asset: pnl_base}},
        'fx_translation_effect': float,
        'hedge_contribution': float,
        'total_pnl': float,
        'reconciles': bool }

    Reconciliation rule: total_pnl == sum(strategy_asset_matrix values) + hedge_contribution
    and fx_translation_effect == total_pnl - sum(local_pnl)
    """
    hedges = hedges or {}

    # translate strategy-asset PnL to base currency
    strategy_asset_matrix: Dict[str, Dict[str, float]] = {}
    asset_class_pnl: Dict[str, float] = {}
    sum_local = 0.0
    sum_base = 0.0

    for strat, assets in pnl_by_strategy_asset.items():
        strategy_asset_matrix[strat] = {}
        for asset, data in assets.items():
            pnl_local = float(data.get("pnl_local", 0.0))
            currency = data.get("currency", "BASE")
            rate = float(fx_rates.get(currency, 1.0))
            pnl_base = pnl_local * rate
            strategy_asset_matrix[strat][asset] = pnl_base
            asset_class_pnl[asset] = asset_class_pnl.get(asset, 0.0) + pnl_base
            sum_local += pnl_local
            sum_base += pnl_base

    # hedges
    hedge_contribution = 0.0
    for _strat, assets in hedges.items():
        for _asset, data in assets.items():
            pnl_local = float(data.get("pnl_local", 0.0))
            currency = data.get("currency", "BASE")
            rate = float(fx_rates.get(currency, 1.0))
            pnl_base = pnl_local * rate
            hedge_contribution += pnl_base

    total_pnl = sum_base + hedge_contribution

    fx_translation_effect = sum_base - sum_local

    # reconciliation
    matrix_sum = sum(sum(v for v in d.values()) for d in strategy_asset_matrix.values())
    reconciles = abs(total_pnl - (matrix_sum + hedge_contribution)) < 1e-9

    return {
        "asset_class_pnl": asset_class_pnl,
        "strategy_asset_matrix": strategy_asset_matrix,
        "fx_translation_effect": fx_translation_effect,
        "hedge_contribution": hedge_contribution,
        "total_pnl": total_pnl,
        "reconciles": reconciles,
    }
