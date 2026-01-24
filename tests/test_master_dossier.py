import json

from octa_reports.master_dossier import MasterDossier


class StubAudit:
    def __init__(self):
        self._logs = []

    def list_logs(self):
        return list(self._logs)

    def verify_logs(self):
        return True

    def append(self, entry):
        self._logs.append(entry)


class StubRegistry:
    def __init__(self):
        self._dg = {"alpha@v1": [], "lib@v1": []}

    def dependency_graph(self):
        return self._dg


class StubLongevity:
    def generate_longevity_cert(self):
        return {"certified": True, "notes": "stub"}


def test_master_dossier_struct_and_hash(tmp_path):
    audit = StubAudit()
    audit.append({"entry": 1})
    reg = StubRegistry()
    lon = StubLongevity()

    md = MasterDossier(
        repo_path=".", subsystems={"audit": audit, "ip_registry": reg, "longevity": lon}
    )
    dossier = md.generate()
    assert "sections" in dossier
    assert "dossier_hash" in dossier
    # write and re-read to ensure serializable
    p = tmp_path / "dossier.json"
    md.export_json(str(p))
    with open(p, "r", encoding="utf-8") as fh:
        loaded = json.load(fh)
    assert loaded["dossier_hash"] == dossier["dossier_hash"]
