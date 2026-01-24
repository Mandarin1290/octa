from octa_capital.liquidity_buckets import AssetLiquidity, Bucket, LiquidityBuckets


def test_illiquid_asset_flag_forces_illiquid_bucket():
    lb = LiquidityBuckets()
    asset = AssetLiquidity(asset_id="X", liquidity_days=1.0, illiquid_flag=True)
    b = lb.classify_asset(asset)
    assert b == Bucket.ILLIQUID


def test_stress_worsens_liquidity():
    lb = LiquidityBuckets()
    # one highly liquid asset and one medium asset
    positions = {
        "A": {"weight": 0.6, "liquidity_days": 1.0},
        "B": {"weight": 0.4, "liquidity_days": 3.0},
    }

    buckets, worst = lb.aggregate_portfolio(positions)
    assert worst == Bucket.T5  # B present -> T+5 worst

    # apply stress that doubles liquidity days -> B moves from T+5 to T+20
    stressed_buckets, stressed_worst = lb.stress_adjusted_aggregate(
        positions, stress_factor=1.0
    )
    assert stressed_worst == Bucket.T20
