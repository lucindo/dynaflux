#!/usr/bin/env python3
# Copyright 2026 The Dynaflux Authors
# SPDX-License-Identifier: Apache-2.0

"""Capture golden reference data from the S5 cost model (costs.py) without modifying it.

- Imports costs.py by path (run with `python3 -I`, sys.path insert is explicit).
- Reads the scenario list from costs.py's own `if __name__ == "__main__":` block via ast,
  so the list is never duplicated here.
- Captures every local of scenario() at return through sys.setprofile, so all
  intermediates (rate, inv_per_shard, v1_rows, w_*, r_*, ...) come from the real code.
- Floats are written with repr precision (json does this), so Go can compare bit-exact.

Usage, from this directory: python3 -I capture.py costs.py reference.json
"""
import ast
import inspect
import io
import json
import os
import sys
from contextlib import redirect_stdout


def main(costs_path, out_path):
    spike_dir = os.path.dirname(os.path.abspath(costs_path))
    sys.dont_write_bytecode = True  # never leave __pycache__ next to costs.py
    sys.path.insert(0, spike_dir)
    import costs  # noqa: E402  (by path, through the explicit insert above)

    assert costs.__file__.endswith("/costs.py"), costs.__file__

    calls = scenario_calls(costs_path)
    sig = inspect.signature(costs.scenario)
    constants = {k: v for k, v in vars(costs).items()
                 if k.isupper() and isinstance(v, (int, float))}

    scenarios = []
    for args, kwargs in calls:
        bound = sig.bind(*args, **kwargs)
        bound.apply_defaults()
        captured = {}

        def prof(frame, event, arg, captured=captured):
            if event == "return" and frame.f_code is costs.scenario.__code__:
                captured.update(frame.f_locals)

        sys.setprofile(prof)
        try:
            result = costs.scenario(*args, **kwargs)
        finally:
            sys.setprofile(None)

        buf = io.StringIO()
        with redirect_stdout(buf):
            costs.show(result)

        inputs = dict(bound.arguments)
        locals_ = {k: v for k, v in captured.items()
                   if k not in inputs and k != "per_region" and isinstance(v, (int, float))}
        # archive_regions is rebound inside scenario() (None -> regions); keep both.
        locals_["archive_regions_effective"] = captured["archive_regions"]
        scenarios.append({
            "name": result["name"],
            "inputs": inputs,
            "intermediates": dict(sorted(locals_.items())),
            "per_region": result["per_region"],
            "per_region_sum": sum(result["per_region"].values()),
            "total": result["total"],
            "over_item_limit": result["hot_row_kb"] > 400,
            "show": buf.getvalue(),
        })

    with open(out_path, "w") as f:
        json.dump({
            "source": os.path.basename(costs_path),
            "python": sys.version.split()[0],
            "constants": constants,
            "defaults": {k: p.default for k, p in sig.parameters.items()
                         if p.default is not inspect.Parameter.empty},
            "scenarios": scenarios,
        }, f, indent=2, ensure_ascii=False)
        f.write("\n")
    print_table(scenarios)


def scenario_calls(path):
    """Return [(args, kwargs)] for every scenario(...) call in the __main__ block."""
    tree = ast.parse(open(path).read(), path)
    mains = [n for n in tree.body if isinstance(n, ast.If)
             and isinstance(n.test, ast.Compare)
             and getattr(n.test.left, "id", None) == "__name__"]
    assert len(mains) == 1, "expected one __main__ block"
    out = []
    for node in ast.walk(mains[0]):
        if isinstance(node, ast.Call) and getattr(node.func, "id", None) == "scenario":
            args = [ast.literal_eval(a) for a in node.args]
            kwargs = {k.arg: ast.literal_eval(k.value) for k in node.keywords}
            out.append((args, kwargs))
    assert out, "no scenario() calls found"
    return out


def print_table(scenarios):
    print("| Scenario | Shards/region | Avg batch | Inv/s/region | HWM entries | Hot row KB "
          "| Writes/s/region | Reads/s/region | Per region $ | Total $/month |")
    print("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
    for s in scenarios:
        i = s["intermediates"]
        flag = " (over 400 KB)" if s["over_item_limit"] else ""
        print(f"| {s['name']} | {i['partitions']:,} | {i['batch']:.2f} | {i['invocations']:,.1f} "
              f"| {i['hwm_entries']:,.1f} | {i['hot_row_kb']:.3f}{flag} | {i['wps']:,.1f} "
              f"| {i['rps']:,.1f} | {s['per_region_sum']:,.2f} | {s['total']:,.2f} |")


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
