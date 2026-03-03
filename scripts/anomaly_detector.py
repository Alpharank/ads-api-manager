#!/usr/bin/env python3
"""
Anomaly detection engine for Google Ads campaigns.

Computes rolling z-scores for CPA, CTR, and conversion rate across campaigns
and keywords. Flags anomalies exceeding configurable thresholds.

Outputs:
  - data/{client}/anomalies/campaign_anomalies.json
  - data/{client}/anomalies/keyword_anomalies.json
  - data/{client}/anomalies/summary.json
  - Console report

Usage:
    python scripts/anomaly_detector.py --client embenauto
    python scripts/anomaly_detector.py --client embenauto --window 21 --threshold 2.5
    python scripts/anomaly_detector.py --client embenauto --lookback 90
"""

import argparse
import csv
import json
import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from math import sqrt

from gads_helpers import ROOT, client_data_dir, log_action


# ──────────────────────────────────────────────────────────────
# Data Loading
# ──────────────────────────────────────────────────────────────

def load_daily_campaign_data(client_slug):
    """Load all daily campaign CSVs into a list of dicts, sorted by date."""
    daily_dir = os.path.join(client_data_dir(client_slug), "daily")
    if not os.path.exists(daily_dir):
        print(f"  No daily data directory: {daily_dir}")
        return []

    rows = []
    for fname in sorted(os.listdir(daily_dir)):
        if not fname.endswith(".csv"):
            continue
        path = os.path.join(daily_dir, fname)
        with open(path, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                rows.append({
                    "date": row["date"],
                    "campaign_id": row["campaign_id"],
                    "campaign_name": row["campaign_name"],
                    "impressions": int(row["impressions"]),
                    "clicks": int(row["clicks"]),
                    "cost": float(row["cost"]),
                    "conversions": float(row["conversions"]),
                })
    rows.sort(key=lambda r: (r["date"], r["campaign_id"]))
    return rows


def load_monthly_keyword_data(client_slug, months=3):
    """Load recent monthly keyword CSVs into a list of dicts."""
    kw_dir = os.path.join(client_data_dir(client_slug), "keywords")
    if not os.path.exists(kw_dir):
        print(f"  No keywords directory: {kw_dir}")
        return []

    files = sorted([f for f in os.listdir(kw_dir) if f.endswith(".csv")])
    files = files[-months:]  # most recent N months

    rows = []
    for fname in files:
        month = fname.replace(".csv", "")
        path = os.path.join(kw_dir, fname)
        with open(path, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                rows.append({
                    "month": month,
                    "campaign_id": row["campaign_id"],
                    "campaign_name": row["campaign_name"],
                    "ad_group_id": row["ad_group_id"],
                    "ad_group_name": row["ad_group_name"],
                    "keyword": row["keyword"],
                    "match_type": row["match_type"],
                    "impressions": int(row["impressions"]),
                    "clicks": int(row["clicks"]),
                    "cost": float(row["cost"]),
                    "conversions": float(row["conversions"]),
                })
    return rows


# ──────────────────────────────────────────────────────────────
# Statistical Helpers
# ──────────────────────────────────────────────────────────────

def rolling_stats(values, window):
    """
    Compute rolling mean and stddev for a list of floats.
    Returns list of (mean, stddev) tuples, one per value.
    Uses trailing window (current point is the last in the window).
    """
    results = []
    for i in range(len(values)):
        start = max(0, i - window + 1)
        w = values[start:i + 1]
        n = len(w)
        if n < 3:  # need at least 3 points for meaningful stats
            results.append((None, None))
            continue
        mean = sum(w) / n
        variance = sum((x - mean) ** 2 for x in w) / n
        stddev = sqrt(variance) if variance > 0 else 0
        results.append((mean, stddev))
    return results


def z_score(value, mean, stddev):
    """Compute z-score. Returns None if stddev is 0 or inputs are None."""
    if mean is None or stddev is None or stddev == 0:
        return None
    return (value - mean) / stddev


def compute_derived_metrics(row):
    """Add CTR, CPA, and conversion_rate to a row dict (in-place)."""
    imp = row["impressions"]
    clicks = row["clicks"]
    cost = row["cost"]
    conv = row["conversions"]

    row["ctr"] = (clicks / imp * 100) if imp > 0 else 0
    row["cpa"] = (cost / conv) if conv > 0 else None
    row["conversion_rate"] = (conv / clicks * 100) if clicks > 0 else 0
    row["cost_per_click"] = (cost / clicks) if clicks > 0 else 0
    return row


# ──────────────────────────────────────────────────────────────
# Campaign-Level Anomaly Detection
# ──────────────────────────────────────────────────────────────

def detect_campaign_anomalies(daily_data, window=14, threshold=2.0, lookback=60):
    """
    Detect anomalies in daily campaign metrics using rolling z-scores.

    Returns list of anomaly dicts with:
      date, campaign_id, campaign_name, metric, value, mean, stddev, z_score, severity
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(days=lookback)).strftime("%Y-%m-%d")

    # Group by campaign
    by_campaign = defaultdict(list)
    for row in daily_data:
        by_campaign[row["campaign_id"]].append(row)

    anomalies = []
    campaign_stats = {}

    for cid, rows in by_campaign.items():
        rows.sort(key=lambda r: r["date"])
        campaign_name = rows[0]["campaign_name"]

        # Compute derived metrics
        for r in rows:
            compute_derived_metrics(r)

        # Metrics to monitor
        metrics = {
            "ctr": {"values": [r["ctr"] for r in rows], "direction": "both", "min_data": 0},
            "cpa": {"values": [r["cpa"] for r in rows], "direction": "high", "min_data": 0},
            "conversion_rate": {"values": [r["conversion_rate"] for r in rows], "direction": "both", "min_data": 0},
            "cost_per_click": {"values": [r["cost_per_click"] for r in rows], "direction": "high", "min_data": 0},
            "cost": {"values": [r["cost"] for r in rows], "direction": "high", "min_data": 0},
        }

        # Track per-campaign summary stats
        recent_rows = [r for r in rows if r["date"] >= cutoff]
        if recent_rows:
            total_cost = sum(r["cost"] for r in recent_rows)
            total_conv = sum(r["conversions"] for r in recent_rows)
            total_clicks = sum(r["clicks"] for r in recent_rows)
            total_imp = sum(r["impressions"] for r in recent_rows)
            campaign_stats[cid] = {
                "campaign_name": campaign_name,
                "days": len(recent_rows),
                "total_cost": round(total_cost, 2),
                "total_conversions": total_conv,
                "avg_cpa": round(total_cost / total_conv, 2) if total_conv > 0 else None,
                "avg_ctr": round(total_clicks / total_imp * 100, 2) if total_imp > 0 else 0,
                "avg_daily_cost": round(total_cost / len(recent_rows), 2),
            }

        for metric_name, meta in metrics.items():
            values = meta["values"]
            # Replace None with 0 for rolling calc (CPA can be None)
            clean_values = [v if v is not None else 0 for v in values]
            stats = rolling_stats(clean_values, window)

            for i, (mean, stddev) in enumerate(stats):
                if mean is None:
                    continue
                if rows[i]["date"] < cutoff:
                    continue

                val = clean_values[i]
                zs = z_score(val, mean, stddev)
                if zs is None:
                    continue

                # Check direction
                is_anomaly = False
                if meta["direction"] == "high" and zs > threshold:
                    is_anomaly = True
                elif meta["direction"] == "low" and zs < -threshold:
                    is_anomaly = True
                elif meta["direction"] == "both" and abs(zs) > threshold:
                    is_anomaly = True

                if is_anomaly:
                    severity = "critical" if abs(zs) > threshold + 1 else "warning"
                    anomalies.append({
                        "date": rows[i]["date"],
                        "campaign_id": cid,
                        "campaign_name": campaign_name,
                        "metric": metric_name,
                        "value": round(val, 4),
                        "rolling_mean": round(mean, 4),
                        "rolling_stddev": round(stddev, 4),
                        "z_score": round(zs, 2),
                        "severity": severity,
                        "impressions": rows[i]["impressions"],
                        "clicks": rows[i]["clicks"],
                        "cost": round(rows[i]["cost"], 2),
                        "conversions": rows[i]["conversions"],
                    })

    anomalies.sort(key=lambda a: (a["date"], abs(a["z_score"])), reverse=True)
    return anomalies, campaign_stats


# ──────────────────────────────────────────────────────────────
# Keyword-Level Anomaly Detection
# ──────────────────────────────────────────────────────────────

def detect_keyword_anomalies(keyword_data, threshold=2.0):
    """
    Detect keyword-level anomalies across months.

    Compares each keyword's metrics against its campaign peers in the same month.
    A keyword is anomalous if its CPA or CPC is >threshold stddevs above the
    campaign average, or its CTR/conv_rate is >threshold stddevs below.

    Returns list of anomaly dicts.
    """
    # Group by (month, campaign_id)
    by_group = defaultdict(list)
    for row in keyword_data:
        compute_derived_metrics(row)
        key = (row["month"], row["campaign_id"])
        by_group[key].append(row)

    anomalies = []

    for (month, cid), rows in by_group.items():
        # Filter to keywords with enough activity (at least 5 clicks)
        active = [r for r in rows if r["clicks"] >= 5]
        if len(active) < 3:
            continue

        campaign_name = active[0]["campaign_name"]

        # For each metric, compute group mean/stddev and flag outliers
        for metric_name, direction in [
            ("cpa", "high"),
            ("cost_per_click", "high"),
            ("ctr", "low"),
            ("conversion_rate", "low"),
        ]:
            values = []
            for r in active:
                v = r.get(metric_name)
                if v is not None:
                    values.append(v)

            if len(values) < 3:
                continue

            mean = sum(values) / len(values)
            variance = sum((x - mean) ** 2 for x in values) / len(values)
            stddev = sqrt(variance) if variance > 0 else 0

            if stddev == 0:
                continue

            for r in active:
                val = r.get(metric_name)
                if val is None:
                    continue

                zs = (val - mean) / stddev

                is_anomaly = False
                if direction == "high" and zs > threshold:
                    is_anomaly = True
                elif direction == "low" and zs < -threshold:
                    is_anomaly = True

                if is_anomaly:
                    severity = "critical" if abs(zs) > threshold + 1 else "warning"
                    anomalies.append({
                        "month": month,
                        "campaign_id": cid,
                        "campaign_name": campaign_name,
                        "ad_group_name": r["ad_group_name"],
                        "keyword": r["keyword"],
                        "match_type": r["match_type"],
                        "metric": metric_name,
                        "value": round(val, 4),
                        "group_mean": round(mean, 4),
                        "group_stddev": round(stddev, 4),
                        "z_score": round(zs, 2),
                        "severity": severity,
                        "impressions": r["impressions"],
                        "clicks": r["clicks"],
                        "cost": round(r["cost"], 2),
                        "conversions": r["conversions"],
                    })

    anomalies.sort(key=lambda a: abs(a["z_score"]), reverse=True)
    return anomalies


# ──────────────────────────────────────────────────────────────
# Trend Detection
# ──────────────────────────────────────────────────────────────

def detect_trends(daily_data, lookback=30):
    """
    Detect sustained trends: compare last 7 days vs prior period.

    Returns list of trend dicts with direction and magnitude.
    """
    cutoff_recent = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%d")
    cutoff_prior = (datetime.now(timezone.utc) - timedelta(days=lookback)).strftime("%Y-%m-%d")

    by_campaign = defaultdict(list)
    for row in daily_data:
        by_campaign[row["campaign_id"]].append(row)

    trends = []

    for cid, rows in by_campaign.items():
        campaign_name = rows[0]["campaign_name"]

        recent = [r for r in rows if r["date"] >= cutoff_recent]
        prior = [r for r in rows if cutoff_prior <= r["date"] < cutoff_recent]

        if not recent or not prior:
            continue

        for metric_fn, metric_name, label in [
            (lambda rs: sum(r["cost"] for r in rs) / max(sum(r["conversions"] for r in rs), 0.001),
             "cpa", "CPA"),
            (lambda rs: sum(r["clicks"] for r in rs) / max(sum(r["impressions"] for r in rs), 1) * 100,
             "ctr", "CTR"),
            (lambda rs: sum(r["cost"] for r in rs) / len(rs),
             "daily_cost", "Daily Spend"),
            (lambda rs: sum(r["conversions"] for r in rs) / len(rs),
             "daily_conversions", "Daily Conversions"),
        ]:
            recent_val = metric_fn(recent)
            prior_val = metric_fn(prior)

            if prior_val == 0:
                continue

            pct_change = ((recent_val - prior_val) / prior_val) * 100

            # Only flag significant changes (>20%)
            if abs(pct_change) < 20:
                continue

            direction = "increasing" if pct_change > 0 else "decreasing"
            # CPA increasing is bad, CTR/conversions decreasing is bad
            is_concerning = (
                (metric_name == "cpa" and pct_change > 0) or
                (metric_name == "ctr" and pct_change < 0) or
                (metric_name == "daily_conversions" and pct_change < 0) or
                (metric_name == "daily_cost" and pct_change > 30)
            )

            trends.append({
                "campaign_id": cid,
                "campaign_name": campaign_name,
                "metric": metric_name,
                "label": label,
                "recent_value": round(recent_val, 2),
                "prior_value": round(prior_val, 2),
                "pct_change": round(pct_change, 1),
                "direction": direction,
                "concerning": is_concerning,
                "recent_days": len(recent),
                "prior_days": len(prior),
            })

    trends.sort(key=lambda t: abs(t["pct_change"]), reverse=True)
    return trends


# ──────────────────────────────────────────────────────────────
# Console Reporting
# ──────────────────────────────────────────────────────────────

def print_campaign_anomalies(anomalies, limit=20):
    """Print campaign anomalies to console."""
    if not anomalies:
        print("\n  No campaign anomalies detected.")
        return

    print(f"\n  {'='*80}")
    print(f"  CAMPAIGN ANOMALIES ({len(anomalies)} total, showing top {min(limit, len(anomalies))})")
    print(f"  {'='*80}")
    print(f"  {'Date':<12} {'Campaign':<22} {'Metric':<16} {'Value':>10} {'Mean':>10} {'Z-Score':>8} {'Sev':>8}")
    print(f"  {'-'*12} {'-'*22} {'-'*16} {'-'*10} {'-'*10} {'-'*8} {'-'*8}")

    for a in anomalies[:limit]:
        sev_icon = "!!" if a["severity"] == "critical" else "! "
        print(f"  {a['date']:<12} {a['campaign_name'][:22]:<22} {a['metric']:<16} "
              f"{a['value']:>10.2f} {a['rolling_mean']:>10.2f} {a['z_score']:>+8.1f} {sev_icon:>8}")


def print_keyword_anomalies(anomalies, limit=15):
    """Print keyword anomalies to console."""
    if not anomalies:
        print("\n  No keyword anomalies detected.")
        return

    print(f"\n  {'='*90}")
    print(f"  KEYWORD ANOMALIES ({len(anomalies)} total, showing top {min(limit, len(anomalies))})")
    print(f"  {'='*90}")
    print(f"  {'Month':<9} {'Keyword':<30} {'Metric':<16} {'Value':>10} {'Avg':>10} {'Z':>6} {'Cost':>8}")
    print(f"  {'-'*9} {'-'*30} {'-'*16} {'-'*10} {'-'*10} {'-'*6} {'-'*8}")

    for a in anomalies[:limit]:
        kw = a["keyword"][:30]
        print(f"  {a['month']:<9} {kw:<30} {a['metric']:<16} "
              f"{a['value']:>10.2f} {a['group_mean']:>10.2f} {a['z_score']:>+6.1f} ${a['cost']:>7.2f}")


def print_trends(trends):
    """Print trend analysis to console."""
    if not trends:
        print("\n  No significant trends detected.")
        return

    concerning = [t for t in trends if t["concerning"]]
    positive = [t for t in trends if not t["concerning"]]

    if concerning:
        print(f"\n  {'='*80}")
        print(f"  CONCERNING TRENDS ({len(concerning)})")
        print(f"  {'='*80}")
        for t in concerning:
            arrow = "↑" if t["direction"] == "increasing" else "↓"
            print(f"  {arrow} {t['campaign_name']}: {t['label']} {t['direction']} "
                  f"{abs(t['pct_change']):.0f}% ({t['prior_value']:.2f} → {t['recent_value']:.2f})")

    if positive:
        print(f"\n  {'='*80}")
        print(f"  POSITIVE TRENDS ({len(positive)})")
        print(f"  {'='*80}")
        for t in positive:
            arrow = "↑" if t["direction"] == "increasing" else "↓"
            print(f"  {arrow} {t['campaign_name']}: {t['label']} {t['direction']} "
                  f"{abs(t['pct_change']):.0f}% ({t['prior_value']:.2f} → {t['recent_value']:.2f})")


def print_campaign_health(campaign_stats):
    """Print campaign health summary."""
    if not campaign_stats:
        return

    print(f"\n  {'='*80}")
    print(f"  CAMPAIGN HEALTH SUMMARY (last 60 days)")
    print(f"  {'='*80}")
    print(f"  {'Campaign':<25} {'Cost':>10} {'Conv':>8} {'CPA':>10} {'CTR':>8} {'$/Day':>8}")
    print(f"  {'-'*25} {'-'*10} {'-'*8} {'-'*10} {'-'*8} {'-'*8}")

    for cid, s in campaign_stats.items():
        cpa_str = f"${s['avg_cpa']:.2f}" if s["avg_cpa"] else "N/A"
        print(f"  {s['campaign_name'][:25]:<25} ${s['total_cost']:>9.2f} "
              f"{s['total_conversions']:>8.0f} {cpa_str:>10} "
              f"{s['avg_ctr']:>7.1f}% ${s['avg_daily_cost']:>7.2f}")


# ──────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Google Ads anomaly detection")
    parser.add_argument("--client", required=True, help="Client slug (e.g. embenauto)")
    parser.add_argument("--window", type=int, default=14,
                        help="Rolling window size in days (default: 14)")
    parser.add_argument("--threshold", type=float, default=2.0,
                        help="Z-score threshold for anomalies (default: 2.0)")
    parser.add_argument("--lookback", type=int, default=60,
                        help="Days to look back for anomaly detection (default: 60)")
    parser.add_argument("--keyword-months", type=int, default=3,
                        help="Months of keyword data to analyze (default: 3)")
    args = parser.parse_args()

    print(f"\n{'#'*60}")
    print(f"  ANOMALY DETECTION: {args.client}")
    print(f"  Window: {args.window} days | Threshold: {args.threshold}σ | Lookback: {args.lookback} days")
    print(f"{'#'*60}")

    # Load data
    print("\n  Loading daily campaign data...")
    daily_data = load_daily_campaign_data(args.client)
    print(f"  Loaded {len(daily_data)} daily rows")

    print("  Loading keyword data...")
    keyword_data = load_monthly_keyword_data(args.client, months=args.keyword_months)
    print(f"  Loaded {len(keyword_data)} keyword rows")

    if not daily_data and not keyword_data:
        print("\n  ERROR: No data found. Run pull_dashboard_data.py first.")
        sys.exit(1)

    # Detect anomalies
    campaign_anomalies, campaign_stats = detect_campaign_anomalies(
        daily_data, window=args.window, threshold=args.threshold, lookback=args.lookback
    )
    keyword_anomalies = detect_keyword_anomalies(keyword_data, threshold=args.threshold)
    trends = detect_trends(daily_data, lookback=args.lookback)

    # Console output
    print_campaign_health(campaign_stats)
    print_campaign_anomalies(campaign_anomalies)
    print_keyword_anomalies(keyword_anomalies)
    print_trends(trends)

    # Save results
    out_dir = os.path.join(client_data_dir(args.client), "anomalies")
    os.makedirs(out_dir, exist_ok=True)

    campaign_path = os.path.join(out_dir, "campaign_anomalies.json")
    with open(campaign_path, "w") as f:
        json.dump(campaign_anomalies, f, indent=2)
    print(f"\n  Saved {len(campaign_anomalies)} campaign anomalies → {campaign_path}")

    keyword_path = os.path.join(out_dir, "keyword_anomalies.json")
    with open(keyword_path, "w") as f:
        json.dump(keyword_anomalies, f, indent=2)
    print(f"  Saved {len(keyword_anomalies)} keyword anomalies → {keyword_path}")

    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat() + "Z",
        "config": {
            "window": args.window,
            "threshold": args.threshold,
            "lookback": args.lookback,
        },
        "campaign_health": campaign_stats,
        "totals": {
            "campaign_anomalies": len(campaign_anomalies),
            "keyword_anomalies": len(keyword_anomalies),
            "critical_campaign": len([a for a in campaign_anomalies if a["severity"] == "critical"]),
            "critical_keyword": len([a for a in keyword_anomalies if a["severity"] == "critical"]),
            "concerning_trends": len([t for t in trends if t["concerning"]]),
        },
        "trends": trends,
    }
    summary_path = os.path.join(out_dir, "summary.json")
    with open(summary_path, "w") as f:
        json.dump(summary, f, indent=2)
    print(f"  Saved summary → {summary_path}")

    # Log the run
    log_action(args.client, "anomaly_detection", {
        "campaign_anomalies": len(campaign_anomalies),
        "keyword_anomalies": len(keyword_anomalies),
        "trends": len(trends),
        "config": summary["config"],
    })

    # Summary
    critical = summary["totals"]["critical_campaign"] + summary["totals"]["critical_keyword"]
    warnings = (len(campaign_anomalies) + len(keyword_anomalies)) - critical
    print(f"\n{'#'*60}")
    print(f"  SUMMARY: {critical} critical, {warnings} warnings, "
          f"{summary['totals']['concerning_trends']} concerning trends")
    print(f"{'#'*60}\n")


if __name__ == "__main__":
    main()
