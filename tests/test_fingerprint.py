from octa_fabric.fingerprint import canonicalize, sha256_hexdigest


def test_fingerprint_stable():
    obj = {"b": 2, "a": 1}
    f1 = sha256_hexdigest(obj)
    f2 = sha256_hexdigest(obj)
    assert f1 == f2
    # canonicalization order must be stable
    assert canonicalize(obj).startswith("{")
