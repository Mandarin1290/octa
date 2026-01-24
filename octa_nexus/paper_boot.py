from __future__ import annotations

import argparse
import os

from octa_nexus.paper_runtime import PaperRuntime


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--manifests", default="./manifests")
    p.add_argument("--atlas", default="./atlas_repo")
    p.add_argument("--ledger", default="./ledger")
    p.add_argument("--vertex", default="./vertex_store")
    args = p.parse_args()
    os.makedirs(args.manifests, exist_ok=True)
    os.makedirs(args.atlas, exist_ok=True)
    os.makedirs(args.ledger, exist_ok=True)
    os.makedirs(args.vertex, exist_ok=True)

    rt = PaperRuntime(
        manifests_dir=args.manifests,
        atlas_root=args.atlas,
        ledger_dir=args.ledger,
        vertex_store=args.vertex,
    )
    res = rt.run_once()
    print("Paper run result:", res)


if __name__ == "__main__":
    main()
