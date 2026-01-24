from octa_core.aum_allocator import (
    AUMAwareAllocator,
    StrategyCapacitySpec,
    inverse_scale_factory,
)


def test_higher_aum_smaller_relative_positions():
    # two strategies identical except scale_fn differences
    low_ref = 1000.0
    spec = {
        "S1": StrategyCapacitySpec(
            base_fraction_of_aum=0.02,
            scale_fn=inverse_scale_factory(reference_aum=low_ref, floor=0.05),
        ),
        "S2": StrategyCapacitySpec(
            base_fraction_of_aum=0.02,
            scale_fn=inverse_scale_factory(reference_aum=low_ref, floor=0.05),
        ),
    }
    allocator = AUMAwareAllocator(capacity_specs=spec, deploy_fraction=0.8)

    expected = {"S1": 1.0, "S2": 1.0}

    # small AUM
    aum_small = 500.0
    alloc_small = allocator.allocate(expected, aum_total=aum_small)
    rel_small = {k: v / aum_small for k, v in alloc_small.items()}

    # large AUM
    aum_large = 1_000_000.0
    alloc_large = allocator.allocate(expected, aum_total=aum_large)
    rel_large = {k: v / aum_large for k, v in alloc_large.items()}

    # relative position should be smaller at larger AUM
    for k in expected:
        assert rel_large[k] <= rel_small[k]


def test_capacity_respected():
    spec = {
        "S1": StrategyCapacitySpec(base_fraction_of_aum=0.01, scale_fn=lambda a: 1.0),
        "S2": StrategyCapacitySpec(base_fraction_of_aum=0.5, scale_fn=lambda a: 1.0),
    }
    allocator = AUMAwareAllocator(capacity_specs=spec, deploy_fraction=1.0)
    expected = {"S1": 10.0, "S2": 1.0}
    aum = 10000.0
    alloc = allocator.allocate(expected, aum_total=aum)

    # caps: S1 <= 0.01*aum = 100, S2 <= 0.5*aum = 5000
    assert alloc["S1"] <= 100 + 1e-8
    assert alloc["S2"] <= 5000 + 1e-8
