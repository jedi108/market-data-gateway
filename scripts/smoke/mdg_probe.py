#!/usr/bin/env python3
"""Independent rendezvous-routing oracle for Market Data Gateway smoke tests.

Re-implements the Go routing core (internal/routing/table.go) in Python and
answers two questions the shell smoke cannot answer on its own:

  assign    — which node is the deterministic primary owner of a series?
  distribute— how are N synthetic series distributed across the membership?

The Go side pins its own behavior with golden vectors in
internal/routing/table_test.go; this oracle is a deliberately independent
derivation so agreement between the two is evidence, not tautology.

Usage:
  mdg_probe.py assign <routing_version> <venue>|<market>|<figi>|<timeframe>|<candle> --nodes id=weight [--nodes id=weight ...]
  mdg_probe.py distribute <routing_version> <count> --nodes id=weight [...]

Exit status is non-zero on any parse/validation failure. Never prints secrets
(it only ever sees routing parameters, which are non-secret by contract).
"""
from __future__ import annotations

import argparse
import hashlib
import math
import struct
import sys


def uniform(identity: str, node_id: str, routing_version: int) -> float:
    h = hashlib.sha256()
    h.update(b"mdg-rdz-v1\n")
    h.update(f"rv={routing_version}\n".encode())
    h.update(f"series={identity}\n".encode())
    h.update(f"node={node_id}\n".encode())
    digest = h.digest()
    v = struct.unpack(">Q", digest[:8])[0] >> (64 - 53)
    return float(v) / float(1 << 53)


def score(identity: str, node_id: str, weight: float, routing_version: int) -> float:
    u = uniform(identity, node_id, routing_version)
    # Mirror the Go guards exactly.
    if u <= 0:
        u = math.ulp(0.0)
    if u >= 1:
        u = 1 - math.ulp(0.0)
    return weight / -math.log(u)


def assignment(identity: str, nodes: dict[str, float], routing_version: int) -> tuple[str, str]:
    primary, standby = "", ""
    primary_score = standby_score = 0.0
    for node_id, weight in nodes.items():
        s = score(identity, node_id, weight, routing_version)
        if primary == "" or s > primary_score:
            standby, standby_score = primary, primary_score
            primary, primary_score = node_id, s
        elif standby == "" or s > standby_score:
            standby, standby_score = node_id, s
    return primary, standby


def parse_series(spec: str) -> str:
    parts = spec.split("|")
    if len(parts) != 5 or not all(p.strip() for p in parts):
        raise SystemExit(f"invalid series spec (want venue|market|figi|timeframe|candle): {spec!r}")
    return spec


def parse_nodes(items: list[str]) -> dict[str, float]:
    nodes: dict[str, float] = {}
    for item in items:
        node_id, _, weight = item.partition("=")
        try:
            w = float(weight)
        except ValueError:
            raise SystemExit(f"invalid node weight: {item!r}")
        if not node_id or w <= 0 or math.isnan(w):
            raise SystemExit(f"invalid node entry: {item!r}")
        nodes[node_id] = w
    if not nodes:
        raise SystemExit("at least one --nodes entry is required")
    return nodes


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    p_assign = sub.add_parser("assign")
    p_assign.add_argument("routing_version", type=int)
    p_assign.add_argument("series")
    p_assign.add_argument("--nodes", action="append", required=True)

    p_dist = sub.add_parser("distribute")
    p_dist.add_argument("routing_version", type=int)
    p_dist.add_argument("count", type=int)
    p_dist.add_argument("--nodes", action="append", required=True)

    args = parser.parse_args(argv)
    nodes = parse_nodes(args.nodes)
    if args.routing_version <= 0:
        raise SystemExit("routing version must be positive")

    if args.command == "assign":
        identity = parse_series(args.series)
        primary, standby = assignment(identity, nodes, args.routing_version)
        print(f"{primary} {standby}")
        return 0

    counts = {node_id: 0 for node_id in nodes}
    for i in range(args.count):
        identity = f"tbank|shares|SMOKE-{i:06d}|1m|trade"
        primary, _ = assignment(identity, nodes, args.routing_version)
        counts[primary] += 1
    for node_id in sorted(counts):
        print(f"{node_id} {counts[node_id]}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
