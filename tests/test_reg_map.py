from octa_reg.reg_map import discover_components, generate_mapping, validate_component


def test_every_module_mapped():
    comps = discover_components()
    mapping = generate_mapping()
    # ensure every discovered component has an entry and at least one domain
    for c in comps:
        assert c in mapping
        assert len(mapping[c]) >= 1


def test_unmapped_component_rejected():
    mapping = generate_mapping()
    try:
        validate_component("octa_nonexistent_fake", mapping)
        raise AssertionError("expected ValueError for unmapped component")
    except ValueError:
        pass
