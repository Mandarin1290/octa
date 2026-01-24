from octa_ip.contracts import Contract, ContractRegistry


def sample_contracts():
    old = Contract(
        name="alpha.signal",
        version="1.0.0",
        input_schema={
            "symbol": {"type": "str", "required": True},
            "window": {"type": "int", "required": True},
        },
        output_schema={
            "signal": {"type": "float", "required": True},
        },
    )

    compatible_new = Contract(
        name="alpha.signal",
        version="1.1.0",
        input_schema={
            "symbol": {"type": "str", "required": True},
            "window": {"type": "int", "required": True},
            "mode": {"type": "str", "required": False},
        },
        output_schema={
            "signal": {"type": "float", "required": True},
            "confidence": {"type": "float", "required": False},
        },
    )

    breaking_new = Contract(
        name="alpha.signal",
        version="2.0.0",
        input_schema={
            "symbol": {"type": "int", "required": True},  # changed type
        },
        output_schema={
            # removed required output
        },
    )

    return old, compatible_new, breaking_new


def run_sample_checks():
    old, comp, br = sample_contracts()
    reg = ContractRegistry()
    reg.register(old)
    ok, reason, bump = reg.is_compatible(old, comp)
    print("compatible check:", ok, reason, bump)
    ok2, reason2, bump2 = reg.is_compatible(old, br)
    print("breaking check:", ok2, reason2, bump2)


if __name__ == "__main__":
    run_sample_checks()
