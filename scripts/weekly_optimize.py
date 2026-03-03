#!/usr/bin/env python3
"""
Weekly optimization orchestration for Google Ads accounts.

Runs the full optimization cycle:
1. Pull fresh dashboard data
2. Run Semrush analysis
3. Audit conversion actions
4. Generate optimization plan (bid adjustments + negatives)
5. Optionally apply changes with guardrails

Usage:
    python scripts/weekly_optimize.py --client embenauto                    # Report only
    python scripts/weekly_optimize.py --client embenauto --apply            # Apply bids + negatives
    python scripts/weekly_optimize.py --client embenauto --apply --max-spend-impact 30
"""

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime

from gads_helpers import ROOT, client_data_dir, log_action


def run_script(script_name, args_list, label):
    """Run a Python script as a subprocess. Returns True on success."""
    cmd = [sys.executable, os.path.join(ROOT, "scripts", script_name)] + args_list
    print(f"\n{'='*60}")
    print(f"  STEP: {label}")
    print(f"  CMD:  {' '.join(cmd)}")
    print(f"{'='*60}\n")

    result = subprocess.run(cmd, cwd=ROOT)
    if result.returncode != 0:
        print(f"\n  WARNING: {script_name} exited with code {result.returncode}")
        return False
    return True


def estimate_spend_impact(client_slug):
    """Estimate daily spend impact of bid adjustments."""
    bid_csv = os.path.join(client_data_dir(client_slug), "recommendations", "bid_adjustments.csv")
    if not os.path.exists(bid_csv):
        return 0

    import csv
    total_impact = 0
    with open(bid_csv, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            current = float(row.get("current_cpc", 0))
            market = float(row.get("market_cpc", 0))
            # Midpoint increase, assume ~10 clicks/day across all keywords
            increase = ((current + market) / 2 - current)
            if increase > 0:
                total_impact += increase * 0.5  # rough daily click estimate per keyword

    return total_impact


def generate_combined_log(client_slug):
    """Combine individual log JSON files into combined.json for the dashboard."""
    import glob

    log_dir = os.path.join(client_data_dir(client_slug), "optimization_log")
    if not os.path.exists(log_dir):
        return

    files = sorted(glob.glob(os.path.join(log_dir, "*.json")))
    files = [f for f in files if not f.endswith("combined.json")]

    entries = []
    for f in files:
        try:
            with open(f) as fh:
                entries.append(json.load(fh))
        except (json.JSONDecodeError, IOError):
            continue

    combined_path = os.path.join(log_dir, "combined.json")
    with open(combined_path, "w") as f:
        json.dump(entries, f, indent=2)
    print(f"\n  Combined {len(entries)} log entries → {combined_path}")


def main():
    parser = argparse.ArgumentParser(description="Weekly Google Ads optimization")
    parser.add_argument("--client", required=True, help="Client slug (e.g. embenauto)")
    parser.add_argument("--months", type=int, default=2, help="Months of data to pull (default: 2)")
    parser.add_argument("--apply", action="store_true",
                        help="Apply bid adjustments and negative keywords (keywords require manual review)")
    parser.add_argument("--max-spend-impact", type=float, default=50.0,
                        help="Max estimated daily spend increase in $ (default: $50)")
    parser.add_argument("--skip-pull", action="store_true", help="Skip data pull (use existing data)")
    parser.add_argument("--skip-semrush", action="store_true", help="Skip Semrush analysis")
    args = parser.parse_args()

    start_time = datetime.utcnow()
    print(f"\n{'#'*60}")
    print(f"  WEEKLY OPTIMIZATION: {args.client}")
    print(f"  Started: {start_time.isoformat()}Z")
    print(f"  Mode: {'APPLY' if args.apply else 'REPORT ONLY'}")
    print(f"{'#'*60}")

    steps_run = []
    steps_failed = []

    # Step 1: Pull fresh data
    if not args.skip_pull:
        ok = run_script("pull_dashboard_data.py", [
            "--client", args.client,
            "--months", str(args.months),
        ], "Pull Google Ads data")
        (steps_run if ok else steps_failed).append("pull_data")
    else:
        print("\n  Skipping data pull (--skip-pull)")

    # Step 2: Semrush analysis
    if not args.skip_semrush:
        ok = run_script("semrush_analyzer.py", [
            "--client", args.client,
        ], "Semrush competitive analysis")
        (steps_run if ok else steps_failed).append("semrush")
    else:
        print("\n  Skipping Semrush analysis (--skip-semrush)")

    # Step 3: Audit conversion actions
    ok = run_script("audit_conversion_actions.py", [
        "--client", args.client,
    ], "Audit conversion actions")
    (steps_run if ok else steps_failed).append("conversion_audit")

    # Step 3.5: Anomaly detection
    ok = run_script("anomaly_detector.py", [
        "--client", args.client,
    ], "Anomaly detection")
    (steps_run if ok else steps_failed).append("anomaly_detection")

    # Step 4: Apply optimizations (if --apply)
    if args.apply:
        # Check spend impact guardrail
        impact = estimate_spend_impact(args.client)
        print(f"\n  Estimated daily spend impact: ${impact:.2f}")
        print(f"  Guardrail: ${args.max_spend_impact:.2f}/day")

        if impact > args.max_spend_impact:
            print(f"\n  GUARDRAIL EXCEEDED: ${impact:.2f} > ${args.max_spend_impact:.2f}")
            print(f"  Skipping bid adjustments. Use --max-spend-impact to override.")
        else:
            # Apply bid adjustments
            ok = run_script("apply_bid_adjustments.py", [
                "--client", args.client,
            ], "Apply bid adjustments")
            (steps_run if ok else steps_failed).append("bid_adjustments")

        # Apply negative keywords (always safe, low risk)
        ok = run_script("add_negative_keywords.py", [
            "--client", args.client,
        ], "Add negative keywords")
        (steps_run if ok else steps_failed).append("negative_keywords")

        # Device modifiers
        ok = run_script("set_device_bid_modifiers.py", [
            "--client", args.client,
        ], "Set device bid modifiers")
        (steps_run if ok else steps_failed).append("device_modifiers")

        print(f"\n  NOTE: Keyword additions skipped (require manual review).")
        print(f"  Run: python scripts/add_keywords.py --client {args.client} --dry-run")
    else:
        print(f"\n  Report-only mode. To apply changes, add --apply flag.")
        print(f"  Individual commands:")
        print(f"    python scripts/apply_bid_adjustments.py --client {args.client} --dry-run")
        print(f"    python scripts/add_keywords.py --client {args.client} --dry-run")
        print(f"    python scripts/add_negative_keywords.py --client {args.client} --dry-run")
        print(f"    python scripts/set_device_bid_modifiers.py --client {args.client} --dry-run")

    # Step 5: Generate combined log for dashboard
    generate_combined_log(args.client)

    # Summary
    elapsed = (datetime.utcnow() - start_time).total_seconds()
    print(f"\n{'#'*60}")
    print(f"  COMPLETE: {args.client}")
    print(f"  Duration: {elapsed:.0f}s")
    print(f"  Steps run: {', '.join(steps_run) if steps_run else 'none'}")
    if steps_failed:
        print(f"  Steps failed: {', '.join(steps_failed)}")
    print(f"{'#'*60}\n")

    # Log the run itself
    log_action(args.client, "weekly_optimize", {
        "mode": "apply" if args.apply else "report",
        "duration_seconds": round(elapsed),
        "steps_run": steps_run,
        "steps_failed": steps_failed,
    })

    # Re-generate combined log to include the weekly_optimize entry
    generate_combined_log(args.client)


if __name__ == "__main__":
    main()
