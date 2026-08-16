"""Phase 12's demo: one command that exercises the whole system end to end and
narrates what it did.

Runs entirely through the real ``pyraft-lab`` CLI entry point - the same one a user
would type - never the Python API directly, so this is honest evidence the shipped
commands actually do what the README says: scaffold a project, run scenarios with
faults and plot them, drive a live cluster interactively, and compare cluster sizes
under packet loss. Everything lands under ``--output-dir`` (default ``demo-output/``,
never committed - see ``.gitignore``) so a repeated run never touches a developer's own
``results/``.

Usage::

    python scripts/demo.py [--output-dir demo-output]

Exits non-zero if any step's exit code says it failed - a demo that silently glosses
over a failing step would be worse than no demo at all.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
CLI = [sys.executable, "-m", "pyraft_lab.cli.main"]


def _run(title: str, args: list[str], *, input_text: str | None = None) -> int:
    print(f"\n=== {title} ===")
    print("$ pyraft-lab " + " ".join(args))
    result = subprocess.run(
        [*CLI, *args],
        input=input_text,
        text=True,
        cwd=REPO_ROOT,
        capture_output=True,
    )
    print(result.stdout, end="")
    if result.returncode != 0:
        print(result.stderr, end="", file=sys.stderr)
    print(f"--- exit code {result.returncode} ---")
    return result.returncode


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", default="demo-output", help="Where the demo writes.")
    args = parser.parse_args()
    out = Path(args.output_dir)
    results_dir = out / "results"
    plot_dir = out / "plots"

    failures = 0

    failures += (
        _run(
            "1. Scaffold a project",
            ["init", "--config", str(out / "cluster.yaml"), "--results-dir", str(results_dir)],
        )
        != 0
    )

    # A quiet baseline, then a scenario with a real fault - each persisted and plotted,
    # so the four graphs README describes (latency/leadership/replication/consistency)
    # exist for both an uneventful run and one with a failover in it.
    for scenario in ("baseline", "leader_crash", "consistency_availability"):
        failures += (
            _run(
                f"2. Run scenario: {scenario}",
                [
                    "run",
                    "--scenario",
                    str(REPO_ROOT / "scenarios" / f"{scenario}.yaml"),
                    "--results-dir",
                    str(results_dir),
                    "--plot",
                    str(plot_dir),
                ],
            )
            != 0
        )

    failures += _run("3. List persisted runs", ["results", "--results-dir", str(results_dir)]) != 0

    # A scripted interactive session over piped stdin - the same seam the CLI's own
    # tests drive, here just to prove the transcript in README's "Cluster controller"
    # and "Live dashboard" sections is real rather than aspirational.
    failures += (
        _run(
            "4. A live 3-node cluster session",
            ["cluster", "start", "--nodes", "3", "--config", str(out / "no-such-config.yaml")],
            input_text=(
                "put color blue\n"
                "get color\n"
                "status\n"
                "topology\n"
                "dashboard --refreshes 1\n"
                "restart n2\n"
                "status\n"
                "exit\n"
            ),
        )
        != 0
    )

    failures += (
        _run(
            "5. Compare cluster sizes under packet loss",
            [
                "benchmark",
                "--nodes",
                "3,5",
                "--loss",
                "0,10",
                "--duration",
                "5s",
                "--results-dir",
                str(results_dir),
                "--output-dir",
                str(out / "benchmark"),
                "--report",
                str(out / "benchmark-report.json"),
            ],
        )
        != 0
    )

    print(f"\n=== demo output written under {out} ===")
    if failures:
        print(f"{failures} step(s) failed", file=sys.stderr)
        return 1
    print("all steps passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
