from octa_ip.contracts import Contract, ContractRegistry


def test_non_breaking_add_optional_field():
    old = Contract(
        name="svc.foo",
        version="1.0.0",
        input_schema={"a": {"type": "int", "required": True}},
        output_schema={"r": {"type": "float", "required": True}},
    )
    new = Contract(
        name="svc.foo",
        version="1.1.0",
        input_schema={
            "a": {"type": "int", "required": True},
            "b": {"type": "str", "required": False},
        },
        output_schema={"r": {"type": "float", "required": True}},
    )
    reg = ContractRegistry()
    reg.register(old)
    ok, reason, bump = reg.is_compatible(old, new)
    assert ok is True
    assert bump == "minor"


def test_breaking_remove_field():
    old = Contract(
        name="svc.bar",
        version="1.0.0",
        input_schema={
            "x": {"type": "int", "required": True},
            "y": {"type": "int", "required": True},
        },
        output_schema={"o": {"type": "float", "required": True}},
    )
    new = Contract(
        name="svc.bar",
        version="2.0.0",
        input_schema={"x": {"type": "int", "required": True}},  # y removed
        output_schema={"o": {"type": "float", "required": True}},
    )
    reg = ContractRegistry()
    reg.register(old)
    ok, reason, bump = reg.is_compatible(old, new)
    assert ok is False
    assert bump == "major"


def test_type_change_detected():
    old = Contract(
        name="svc.baz",
        version="1.0.0",
        input_schema={"k": {"type": "str", "required": True}},
        output_schema={"v": {"type": "int", "required": True}},
    )
    new = Contract(
        name="svc.baz",
        version="2.0.0",
        input_schema={"k": {"type": "int", "required": True}},
        output_schema={"v": {"type": "int", "required": True}},
    )
    reg = ContractRegistry()
    reg.register(old)
    ok, reason, bump = reg.is_compatible(old, new)
    assert ok is False
    assert "type" in reason
