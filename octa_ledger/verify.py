from __future__ import annotations

import argparse

from .store import LedgerStore


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="octa_ledger.verify")
    p.add_argument("logdir", help="directory containing ledger.log and ledger.db")
    args = p.parse_args(argv)
    store = LedgerStore(args.logdir)
    ok = store.verify_chain()
    print("chain ok:", ok)
    return 0 if ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
