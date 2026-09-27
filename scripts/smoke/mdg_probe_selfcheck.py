#!/usr/bin/env python3
"""Cross-check mdg_probe.py against the Go-pinned golden vectors (16 keys,
routing_version=1, membership {node-a:1, node-b:1}) from
internal/routing/table_test.go. Exit 0 only when the independent Python
oracle reproduces every pinned primary/standby pair."""
import subprocess
import sys
from pathlib import Path

PROBE = str(Path(__file__).resolve().with_name("mdg_probe.py"))

GOLDEN = """tbank|shares|BBG004730N88-000|1m|trade node-a node-b
tbank|shares|BBG004730N88-001|1m|trade node-a node-b
tbank|shares|BBG004730N88-002|1m|trade node-a node-b
tbank|shares|BBG004730N88-003|1m|trade node-a node-b
tbank|shares|BBG004730N88-004|1m|trade node-b node-a
tbank|shares|BBG004730N88-005|1m|trade node-a node-b
tbank|shares|BBG004730N88-006|1m|trade node-a node-b
tbank|shares|BBG004730N88-007|1m|trade node-b node-a
tbank|shares|BBG004730N88-008|1m|trade node-a node-b
tbank|shares|BBG004730N88-009|1m|trade node-a node-b
tbank|shares|BBG004730N88-010|1m|trade node-a node-b
tbank|shares|BBG004730N88-011|1m|trade node-a node-b
tbank|shares|BBG004730N88-012|1m|trade node-b node-a
tbank|shares|BBG004730N88-013|1m|trade node-a node-b
tbank|shares|BBG004730N88-014|1m|trade node-a node-b
tbank|shares|BBG004730N88-015|1m|trade node-b node-a"""


def main() -> int:
    failures = 0
    for line in GOLDEN.strip().splitlines():
        ident, want_primary, want_standby = line.rsplit(" ", 2)
        out = subprocess.run(
            [sys.executable, PROBE, "assign", "1", ident,
             "--nodes", "node-a=1", "--nodes", "node-b=1"],
            capture_output=True, text=True, check=True,
        ).stdout.strip()
        if out != f"{want_primary} {want_standby}":
            print(f"MISMATCH {ident}: oracle={out!r} golden={want_primary} {want_standby}")
            failures += 1
    if failures:
        print(f"ORACLE DIVERGES on {failures} vectors")
        return 1
    print("ORACLE MATCHES ALL 16 GOLDEN VECTORS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
