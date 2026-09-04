#!/usr/bin/env python3
"""Render the sticky PR comment for the quick language-bench comparison.

Reads the `--json` report emitted by `surrealql-test bench run` and prints a
markdown comment to stdout. Also used (without `--results`) to render the
"running" and "failed" states so the same comment is updated in place.
"""

import argparse
import json
import sys


def fmt_secs(s: float) -> str:
    if s < 1e-6:
        return f"{s * 1e9:.0f} ns"
    if s < 1e-3:
        return f"{s * 1e6:.1f} µs"
    if s < 1.0:
        return f"{s * 1e3:.2f} ms"
    return f"{s:.3f} s"


def pretty_name(name: str) -> str:
    # "bench/scans/foo.surql [indexed]" -> "scans/foo [indexed]"
    variant = ""
    if name.endswith("]") and " [" in name:
        name, variant = name[: name.rfind(" [")], name[name.rfind(" ["):]
    name = name.removeprefix("bench/").removesuffix(".surql")
    return name + variant


VERDICT_RANK = {"regressed": 0, "improved": 1, "within-noise": 2, None: 3}
VERDICT_EMOJI = {
    "regressed": "🔴",
    "improved": "🟢",
    "within-noise": "⚪",
    None: "⚫",
}


def render_table(benches: list) -> str:
    rows = []
    for b in benches:
        comp = b.get("comparison")
        verdict = comp["verdict"] if comp else None
        # The baseline (main) median only exists when a baseline was found.
        main_ms = fmt_secs(comp["base_median_secs"]) if comp else "—"
        change = f"{comp['change_pct']:+.1f}%" if comp else "—"
        p = f"{comp['p_value']:.2f}" if comp else "—"
        rows.append(
            (
                VERDICT_RANK.get(verdict, 3),
                # within a rank, biggest absolute change first
                -(abs(comp["change_pct"]) if comp else 0.0),
                f"| {VERDICT_EMOJI.get(verdict, '⚫')} `{pretty_name(b['name'])}` "
                f"| {main_ms} | {fmt_secs(b['median_secs'])} | {change} | {p} |",
            )
        )
    rows.sort(key=lambda r: (r[0], r[1]))
    header = (
        "| | Bench | Median (main) | Median (PR) | Δ | p |\n"
        "|---|---|--:|--:|--:|--:|"
    )
    return header + "\n" + "\n".join(r[2] for r in rows)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--status",
        choices=["instructions", "running", "done", "failed"],
        required=True,
    )
    ap.add_argument("--sha", default="")
    ap.add_argument("--base", default="main")
    ap.add_argument("--phase", default="")
    ap.add_argument("--run-url", default="")
    ap.add_argument("--results", default="")
    args = ap.parse_args()

    # Opt-in instructions, posted on PR open before any benchmark has run.
    if args.status == "instructions":
        print(
            "\n".join(
                [
                    "## 📊 Benchmark",
                    "",
                    f"Add the **`benchmark`** label to this PR to benchmark your "
                    f"changes and compare them against the latest nightly `{args.base}` "
                    f"baseline.",
                    "",
                    f"- `{args.base}` is benchmarked **nightly** (full run, same runner "
                    "pool); this PR is compared against that baseline, so only your "
                    "branch is benchmarked here.",
                    "- Runs on every push while the label is set; remove the label to "
                    "stop.",
                ]
            )
        )
        return 0

    short = args.sha[:9]
    lines = [f"## 📊 Benchmark — `{short}` vs nightly `{args.base}`", ""]

    if args.status == "running":
        phase = args.phase or "Starting…"
        lines.append(f"⏳ **{phase}**")
    elif args.status == "failed":
        lines.append("❌ **Benchmark run failed.**")
        if args.phase:
            lines.append("")
            lines.append(f"Last phase: {args.phase}")
    else:  # done
        with open(args.results) as f:
            report = json.load(f)
        benches = report.get("benches", [])
        compared = [b for b in benches if b.get("comparison")]
        reg = [b for b in compared if b["comparison"]["verdict"] == "regressed"]
        imp = [b for b in compared if b["comparison"]["verdict"] == "improved"]
        flat = [b for b in compared if b["comparison"]["verdict"] == "within-noise"]
        nob = [b for b in benches if not b.get("comparison")]

        lines.append(
            f"🔴 **{len(reg)}** regressed · 🟢 **{len(imp)}** improved · "
            f"⚪ **{len(flat)}** within noise · ⚫ **{len(nob)}** no baseline "
            f"(backend `{report.get('backend', '?')}`, {len(benches)} benches)"
        )
        lines.append("")

        changed = reg + imp
        if changed:
            lines.append("### Significant changes")
            lines.append(render_table(changed))
            lines.append("")
        else:
            lines.append("_No significant changes detected._")
            lines.append("")

        lines.append("<details><summary>Full results</summary>")
        lines.append("")
        lines.append(render_table(benches))
        lines.append("")
        lines.append("</details>")

    lines.append("")
    note = (
        f"> `{args.base}` is benchmarked nightly on the same runner pool; this "
        "compares the PR head against that baseline. Updates on every push while "
        "the `benchmark` label is set."
    )
    lines.append(note)
    if args.run_url:
        lines.append("")
        lines.append(f"[View run]({args.run_url})")

    print("\n".join(lines))
    return 0


if __name__ == "__main__":
    sys.exit(main())
