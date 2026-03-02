#!/usr/bin/env python3
"""
Semrush-Powered Google Ads Recommendation Engine

Analyzes current Google Ads keyword performance against Semrush market intelligence
to generate actionable recommendations for campaigns, ad groups, and keywords.

Uses:
  - Semrush API for keyword volume, CPC, competition, and related keyword discovery
  - Semrush competitor paid keyword intelligence
  - Google Ads performance data from data/ directory

Outputs:
  - data/<client_id>/recommendations/latest.json — Structured recommendations
  - data/<client_id>/recommendations/latest.csv  — Flat keyword opportunities
  - data/<client_id>/semrush/keyword_intelligence.json — Raw Semrush data cache

Usage:
    python scripts/semrush_analyzer.py
    python scripts/semrush_analyzer.py --client embenauto
    python scripts/semrush_analyzer.py --competitors montway.com,sherpa.com
"""

import argparse
import csv
import io
import json
import os
import sys
import time
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import datetime

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(ROOT, "data")

SEMRUSH_API_KEY = "89be8b05071ffbec69e45688aea6e87a"
SEMRUSH_BASE = "https://api.semrush.com/"

# Top competitors in car shipping / auto transport
DEFAULT_COMPETITORS = [
    "montway.com",
    "sherpa.com",
    "americanautoshipping.com",
    "a1autotransport.com",
    "shipcar.com",
]


def semrush_request(params, label=""):
    """Make a Semrush API request, return parsed rows."""
    params["key"] = SEMRUSH_API_KEY
    url = SEMRUSH_BASE + "?" + urllib.parse.urlencode(params)
    try:
        resp = urllib.request.urlopen(url, timeout=30)
        text = resp.read().decode("utf-8").strip()
        if text.startswith("ERROR"):
            return []
        reader = csv.DictReader(io.StringIO(text), delimiter=";")
        return list(reader)
    except Exception as e:
        print(f"  Semrush API error ({label}): {e}")
        return []


def get_keyword_data(keyword, database="us"):
    """Get volume, CPC, competition for a single keyword."""
    rows = semrush_request({
        "type": "phrase_this",
        "phrase": keyword,
        "database": database,
    }, f"phrase_this: {keyword}")
    if rows:
        r = rows[0]
        return {
            "keyword": r.get("Keyword", keyword),
            "volume": int(r.get("Search Volume", 0)),
            "cpc": float(r.get("CPC", 0)),
            "competition": float(r.get("Competition", 0)),
        }
    return None


def get_related_keywords(seed, database="us", limit=50):
    """Get related keywords from Semrush."""
    rows = semrush_request({
        "type": "phrase_related",
        "phrase": seed,
        "database": database,
        "display_limit": str(limit),
    }, f"phrase_related: {seed}")
    results = []
    for r in rows:
        relevance = float(r.get("Related Relevance", 0))
        if relevance < 0.1:
            continue
        results.append({
            "keyword": r.get("Keyword", ""),
            "volume": int(r.get("Search Volume", 0)),
            "cpc": float(r.get("CPC", 0)),
            "competition": float(r.get("Competition", 0)),
            "relevance": relevance,
        })
    return results


def get_competitor_paid_keywords(domain, database="us", limit=50):
    """Get a competitor's paid (AdWords) keywords."""
    rows = semrush_request({
        "type": "domain_adwords",
        "domain": domain,
        "database": database,
        "display_limit": str(limit),
    }, f"domain_adwords: {domain}")
    results = []
    for r in rows:
        results.append({
            "keyword": r.get("Keyword", ""),
            "position": int(r.get("Position", 0)),
            "volume": int(r.get("Search Volume", 0)),
            "cpc": float(r.get("CPC", 0)),
            "competition": float(r.get("Competition", 0)),
            "traffic_pct": float(r.get("Traffic (%)", 0)),
        })
    return results


def get_competitor_organic_keywords(domain, database="us", limit=30):
    """Get a competitor's top organic keywords."""
    rows = semrush_request({
        "type": "domain_organic",
        "domain": domain,
        "database": database,
        "display_limit": str(limit),
    }, f"domain_organic: {domain}")
    results = []
    for r in rows:
        results.append({
            "keyword": r.get("Keyword", ""),
            "position": int(r.get("Position", 0)),
            "volume": int(r.get("Search Volume", 0)),
            "cpc": float(r.get("CPC", 0)),
            "traffic_pct": float(r.get("Traffic (%)", 0)),
        })
    return results


def load_google_ads_data(client_id):
    """Load the latest Google Ads data from data/ directory."""
    client_dir = os.path.join(DATA_DIR, client_id)
    data = {"campaigns": [], "keywords": [], "search_terms": []}

    # Load all month files and merge
    for data_type in data:
        type_dir = os.path.join(client_dir, data_type)
        if not os.path.isdir(type_dir):
            continue
        for fn in sorted(os.listdir(type_dir)):
            if not fn.endswith(".csv"):
                continue
            with open(os.path.join(type_dir, fn)) as f:
                reader = csv.DictReader(f)
                for row in reader:
                    data[data_type].append(row)

    return data


def analyze_keyword_performance(ads_data):
    """Analyze Google Ads keyword performance."""
    kw_stats = defaultdict(lambda: {
        "impressions": 0, "clicks": 0, "cost": 0.0, "conversions": 0.0,
        "campaigns": set(), "ad_groups": set(), "match_types": set(),
    })

    for row in ads_data["keywords"]:
        kw = row["keyword"].lower().strip()
        kw_stats[kw]["impressions"] += int(row.get("impressions", 0))
        kw_stats[kw]["clicks"] += int(row.get("clicks", 0))
        kw_stats[kw]["cost"] += float(row.get("cost", 0))
        kw_stats[kw]["conversions"] += float(row.get("conversions", 0))
        kw_stats[kw]["campaigns"].add(row.get("campaign_name", ""))
        kw_stats[kw]["ad_groups"].add(row.get("ad_group_name", ""))
        kw_stats[kw]["match_types"].add(row.get("match_type", ""))

    # Calculate derived metrics
    results = []
    for kw, stats in kw_stats.items():
        ctr = (stats["clicks"] / stats["impressions"] * 100) if stats["impressions"] > 0 else 0
        cpc = (stats["cost"] / stats["clicks"]) if stats["clicks"] > 0 else 0
        conv_rate = (stats["conversions"] / stats["clicks"] * 100) if stats["clicks"] > 0 else 0
        cost_per_conv = (stats["cost"] / stats["conversions"]) if stats["conversions"] > 0 else None

        results.append({
            "keyword": kw,
            "impressions": stats["impressions"],
            "clicks": stats["clicks"],
            "cost": round(stats["cost"], 2),
            "conversions": stats["conversions"],
            "ctr": round(ctr, 2),
            "avg_cpc": round(cpc, 2),
            "conv_rate": round(conv_rate, 2),
            "cost_per_conversion": round(cost_per_conv, 2) if cost_per_conv else None,
            "campaigns": list(stats["campaigns"]),
            "ad_groups": list(stats["ad_groups"]),
            "match_types": list(stats["match_types"]),
        })

    return sorted(results, key=lambda x: x["cost"], reverse=True)


def analyze_search_terms(ads_data):
    """Analyze search term performance to find winners and losers."""
    st_stats = defaultdict(lambda: {
        "impressions": 0, "clicks": 0, "cost": 0.0, "conversions": 0.0,
        "keywords": set(),
    })

    for row in ads_data["search_terms"]:
        term = row["search_term"].lower().strip()
        st_stats[term]["impressions"] += int(row.get("impressions", 0))
        st_stats[term]["clicks"] += int(row.get("clicks", 0))
        st_stats[term]["cost"] += float(row.get("cost", 0))
        st_stats[term]["conversions"] += float(row.get("conversions", 0))
        st_stats[term]["keywords"].add(row.get("keyword", ""))

    results = []
    for term, stats in st_stats.items():
        if stats["clicks"] < 1:
            continue
        ctr = (stats["clicks"] / stats["impressions"] * 100) if stats["impressions"] > 0 else 0
        cpc = (stats["cost"] / stats["clicks"]) if stats["clicks"] > 0 else 0
        conv_rate = (stats["conversions"] / stats["clicks"] * 100) if stats["clicks"] > 0 else 0

        results.append({
            "search_term": term,
            "impressions": stats["impressions"],
            "clicks": stats["clicks"],
            "cost": round(stats["cost"], 2),
            "conversions": stats["conversions"],
            "ctr": round(ctr, 2),
            "avg_cpc": round(cpc, 2),
            "conv_rate": round(conv_rate, 2),
            "matched_keywords": list(stats["keywords"]),
        })

    return sorted(results, key=lambda x: x["clicks"], reverse=True)


def generate_recommendations(ads_keywords, search_terms, semrush_intel, competitor_intel):
    """Generate actionable recommendations based on combined intelligence."""
    recommendations = {
        "generated_at": datetime.now().isoformat(),
        "summary": {},
        "keyword_opportunities": [],
        "negative_keyword_suggestions": [],
        "bid_adjustments": [],
        "campaign_structure": [],
        "competitor_insights": [],
    }

    # Current keyword set
    current_kws = {k["keyword"].lower() for k in ads_keywords}

    # --- 1. Keyword Opportunities (from Semrush related + competitor gaps) ---
    all_opportunities = {}

    for kw_data in semrush_intel.get("related_keywords", []):
        kw = kw_data["keyword"].lower()
        if kw in current_kws:
            continue
        if kw_data["volume"] < 100:
            continue
        if kw_data["cpc"] < 1.0:
            continue  # Filter out irrelevant low-CPC
        if kw not in all_opportunities or kw_data["volume"] > all_opportunities[kw]["volume"]:
            all_opportunities[kw] = {
                "keyword": kw,
                "volume": kw_data["volume"],
                "cpc": kw_data["cpc"],
                "competition": kw_data["competition"],
                "relevance": kw_data.get("relevance", 0),
                "source": "semrush_related",
                "priority": "high" if kw_data["volume"] >= 1000 and kw_data["relevance"] >= 0.3 else "medium",
            }

    # Add competitor keyword gaps
    for comp_domain, comp_kws in competitor_intel.items():
        for ckw in comp_kws.get("paid_keywords", []):
            kw = ckw["keyword"].lower()
            if kw in current_kws:
                continue
            if ckw["volume"] < 100:
                continue
            if kw not in all_opportunities or ckw["volume"] > all_opportunities[kw]["volume"]:
                all_opportunities[kw] = {
                    "keyword": kw,
                    "volume": ckw["volume"],
                    "cpc": ckw["cpc"],
                    "competition": ckw["competition"],
                    "source": f"competitor:{comp_domain}",
                    "relevance": ckw.get("traffic_pct", 0),
                    "priority": "high" if ckw["volume"] >= 1000 else "medium",
                }

    # Filter to transport-relevant keywords
    transport_terms = {"ship", "shipping", "transport", "auto", "car", "vehicle", "haul",
                       "carrier", "delivery", "move", "moving", "freight", "snowbird"}
    filtered_opps = []
    for kw, data in all_opportunities.items():
        words = set(kw.lower().split())
        if words & transport_terms:
            filtered_opps.append(data)

    recommendations["keyword_opportunities"] = sorted(
        filtered_opps, key=lambda x: (x["priority"] == "high", x["volume"]), reverse=True
    )[:50]

    # --- 2. Negative Keyword Suggestions ---
    # Search terms with clicks but no conversions and irrelevant intent
    irrelevant_patterns = {
        "job", "jobs", "hiring", "salary", "career", "careers",
        "near me", "address", "phone", "hours", "review", "reviews",
        "free", "diy", "how to", "tutorial",
        "train", "amtrak", "u-haul", "uhaul", "rental",
        "game", "movie", "song",
    }

    for st in search_terms:
        term_lower = st["search_term"].lower()
        if st["clicks"] >= 2 and st["conversions"] == 0 and st["cost"] > 3:
            # Check if it matches irrelevant patterns
            is_irrelevant = any(p in term_lower for p in irrelevant_patterns)
            # Also flag competitor brand searches that waste spend
            is_competitor_brand = any(c in term_lower for c in
                ["montway", "sherpa", "a1 auto", "a-1 auto", "nexus auto", "carmax"])

            if is_irrelevant or is_competitor_brand:
                recommendations["negative_keyword_suggestions"].append({
                    "term": st["search_term"],
                    "reason": "competitor_brand" if is_competitor_brand else "irrelevant_intent",
                    "clicks": st["clicks"],
                    "cost": st["cost"],
                    "matched_keyword": st["matched_keywords"][0] if st["matched_keywords"] else "",
                })

    # --- 3. Bid Adjustment Recommendations ---
    for kw in ads_keywords:
        semrush_data = semrush_intel.get("keyword_data", {}).get(kw["keyword"].lower())
        if not semrush_data:
            continue

        market_cpc = semrush_data["cpc"]
        actual_cpc = kw["avg_cpc"]
        volume = semrush_data["volume"]

        if kw["clicks"] < 3:
            continue

        rec = {
            "keyword": kw["keyword"],
            "current_cpc": actual_cpc,
            "market_cpc": market_cpc,
            "volume": volume,
            "your_ctr": kw["ctr"],
            "your_conv_rate": kw["conv_rate"],
            "conversions": kw["conversions"],
        }

        # Underbidding on high-converting keywords
        if kw["conv_rate"] > 5 and actual_cpc < market_cpc * 0.8:
            rec["action"] = "increase_bid"
            rec["reason"] = f"High converter ({kw['conv_rate']}% CR) but bidding below market (${actual_cpc:.2f} vs ${market_cpc:.2f})"
            rec["priority"] = "high"
            recommendations["bid_adjustments"].append(rec)

        # Overbidding on non-converting keywords
        elif kw["conversions"] == 0 and kw["clicks"] >= 10 and actual_cpc > market_cpc * 1.2:
            rec["action"] = "decrease_bid"
            rec["reason"] = f"No conversions after {kw['clicks']} clicks, CPC above market (${actual_cpc:.2f} vs ${market_cpc:.2f})"
            rec["priority"] = "medium"
            recommendations["bid_adjustments"].append(rec)

        # Good performer, could scale
        elif kw["conv_rate"] > 3 and kw["impressions"] < volume * 0.3:
            rec["action"] = "increase_bid"
            rec["reason"] = f"Good converter ({kw['conv_rate']}% CR) but low impression share ({kw['impressions']}/{volume} monthly searches)"
            rec["priority"] = "medium"
            recommendations["bid_adjustments"].append(rec)

    # --- 4. Campaign Structure Recommendations ---
    # Analyze current funnel structure
    campaigns = defaultdict(lambda: {"keywords": 0, "cost": 0, "conversions": 0, "clicks": 0})
    for kw in ads_keywords:
        for c in kw["campaigns"]:
            campaigns[c]["keywords"] += 1
            campaigns[c]["cost"] += kw["cost"]
            campaigns[c]["conversions"] += kw["conversions"]
            campaigns[c]["clicks"] += kw["clicks"]

    for camp_name, stats in campaigns.items():
        camp_cpa = (stats["cost"] / stats["conversions"]) if stats["conversions"] > 0 else None

        # Suggest structure improvements
        if "TOF" in camp_name and stats["conversions"] > 0:
            recommendations["campaign_structure"].append({
                "campaign": camp_name,
                "action": "expand_keywords",
                "reason": f"TOF campaign converting ({stats['conversions']} conversions). Add more broad/related keywords to capture more top-of-funnel traffic.",
                "priority": "high",
            })

        if "MOF" in camp_name and stats["keywords"] < 5:
            recommendations["campaign_structure"].append({
                "campaign": camp_name,
                "action": "add_keywords",
                "reason": f"MOF campaign has only {stats['keywords']} keywords. Add branded modifier keywords and comparison terms.",
                "priority": "medium",
            })

    # --- 5. Competitor Insights ---
    for comp_domain, comp_data in competitor_intel.items():
        paid_kws = comp_data.get("paid_keywords", [])
        organic_kws = comp_data.get("organic_keywords", [])

        if not paid_kws and not organic_kws:
            continue

        # Find keywords competitors bid on that we don't
        comp_only = [k for k in paid_kws if k["keyword"].lower() not in current_kws]
        overlap = [k for k in paid_kws if k["keyword"].lower() in current_kws]

        insight = {
            "competitor": comp_domain,
            "total_paid_keywords": len(paid_kws),
            "overlap_with_you": len(overlap),
            "unique_to_competitor": len(comp_only),
            "top_unique_keywords": sorted(comp_only, key=lambda x: x["volume"], reverse=True)[:10],
            "top_organic_keywords": sorted(organic_kws, key=lambda x: x["volume"], reverse=True)[:10],
        }
        recommendations["competitor_insights"].append(insight)

    # --- Summary ---
    total_spend = sum(k["cost"] for k in ads_keywords)
    total_conversions = sum(k["conversions"] for k in ads_keywords)
    avg_cpa = (total_spend / total_conversions) if total_conversions > 0 else None

    recommendations["summary"] = {
        "total_keywords_analyzed": len(ads_keywords),
        "total_search_terms_analyzed": len(search_terms),
        "total_spend": round(total_spend, 2),
        "total_conversions": total_conversions,
        "avg_cpa": round(avg_cpa, 2) if avg_cpa else None,
        "new_keyword_opportunities": len(recommendations["keyword_opportunities"]),
        "negative_keyword_suggestions": len(recommendations["negative_keyword_suggestions"]),
        "bid_adjustment_suggestions": len(recommendations["bid_adjustments"]),
        "campaign_structure_suggestions": len(recommendations["campaign_structure"]),
        "competitors_analyzed": len(recommendations["competitor_insights"]),
    }

    return recommendations


def save_results(client_id, recommendations, semrush_data):
    """Save recommendations and Semrush data."""
    client_dir = os.path.join(DATA_DIR, client_id)

    # Save Semrush intelligence cache
    semrush_dir = os.path.join(client_dir, "semrush")
    os.makedirs(semrush_dir, exist_ok=True)
    with open(os.path.join(semrush_dir, "keyword_intelligence.json"), "w") as f:
        json.dump(semrush_data, f, indent=2, default=str)
    print(f"  Saved: semrush/keyword_intelligence.json")

    # Save recommendations JSON
    rec_dir = os.path.join(client_dir, "recommendations")
    os.makedirs(rec_dir, exist_ok=True)
    with open(os.path.join(rec_dir, "latest.json"), "w") as f:
        json.dump(recommendations, f, indent=2, default=str)
    print(f"  Saved: recommendations/latest.json")

    # Save keyword opportunities CSV
    opps = recommendations.get("keyword_opportunities", [])
    if opps:
        with open(os.path.join(rec_dir, "keyword_opportunities.csv"), "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["keyword", "volume", "cpc", "competition", "relevance", "source", "priority"])
            writer.writeheader()
            writer.writerows(opps)
        print(f"  Saved: recommendations/keyword_opportunities.csv ({len(opps)} opportunities)")

    # Save negative keyword suggestions CSV
    negs = recommendations.get("negative_keyword_suggestions", [])
    if negs:
        with open(os.path.join(rec_dir, "negative_keywords.csv"), "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["term", "reason", "clicks", "cost", "matched_keyword"])
            writer.writeheader()
            writer.writerows(negs)
        print(f"  Saved: recommendations/negative_keywords.csv ({len(negs)} suggestions)")

    # Save bid adjustments CSV
    bids = recommendations.get("bid_adjustments", [])
    if bids:
        with open(os.path.join(rec_dir, "bid_adjustments.csv"), "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["keyword", "action", "reason", "priority", "current_cpc", "market_cpc", "volume", "your_ctr", "your_conv_rate", "conversions"])
            writer.writeheader()
            writer.writerows(bids)
        print(f"  Saved: recommendations/bid_adjustments.csv ({len(bids)} suggestions)")


def print_report(recommendations):
    """Print a human-readable report."""
    s = recommendations["summary"]
    print("\n" + "=" * 70)
    print("  SEMRUSH-POWERED GOOGLE ADS RECOMMENDATIONS")
    print("=" * 70)
    print(f"  Generated: {recommendations['generated_at']}")
    print(f"  Keywords analyzed: {s['total_keywords_analyzed']}")
    print(f"  Search terms analyzed: {s['total_search_terms_analyzed']}")
    print(f"  Total spend (period): ${s['total_spend']:,.2f}")
    print(f"  Total conversions: {s['total_conversions']}")
    if s["avg_cpa"]:
        print(f"  Average CPA: ${s['avg_cpa']:,.2f}")
    print()

    # Top keyword opportunities
    opps = recommendations["keyword_opportunities"][:15]
    if opps:
        print("  TOP KEYWORD OPPORTUNITIES (not currently bidding)")
        print("  " + "-" * 66)
        print(f"  {'Keyword':<35} {'Vol':>7} {'CPC':>7} {'Comp':>6} {'Src':<15}")
        print("  " + "-" * 66)
        for o in opps:
            src = o["source"].replace("competitor:", "comp:")[:15]
            star = "*" if o["priority"] == "high" else " "
            print(f" {star}{o['keyword']:<35} {o['volume']:>7,} ${o['cpc']:>5.2f} {o['competition']:>5.2f} {src:<15}")
        print()

    # Negative keyword suggestions
    negs = recommendations["negative_keyword_suggestions"][:10]
    if negs:
        print("  NEGATIVE KEYWORD SUGGESTIONS (wasting spend)")
        print("  " + "-" * 66)
        for n in negs:
            print(f"  - \"{n['term']}\" ({n['reason']}, {n['clicks']} clicks, ${n['cost']:.2f})")
        print()

    # Bid adjustments
    bids = recommendations["bid_adjustments"][:10]
    if bids:
        print("  BID ADJUSTMENT RECOMMENDATIONS")
        print("  " + "-" * 66)
        for b in bids:
            arrow = "^" if b["action"] == "increase_bid" else "v"
            print(f"  {arrow} \"{b['keyword']}\" — {b['reason']}")
        print()

    # Campaign structure
    camp = recommendations["campaign_structure"]
    if camp:
        print("  CAMPAIGN STRUCTURE SUGGESTIONS")
        print("  " + "-" * 66)
        for c in camp:
            print(f"  [{c['priority'].upper()}] {c['campaign']}: {c['reason']}")
        print()

    # Competitor insights
    comps = recommendations["competitor_insights"]
    if comps:
        print("  COMPETITOR INTELLIGENCE")
        print("  " + "-" * 66)
        for c in comps:
            print(f"  {c['competitor']}: {c['total_paid_keywords']} paid KWs, "
                  f"{c['overlap_with_you']} overlap, {c['unique_to_competitor']} unique")
            for kw in c["top_unique_keywords"][:5]:
                print(f"    -> \"{kw['keyword']}\" (vol: {kw['volume']:,}, CPC: ${kw['cpc']:.2f})")
        print()

    print("=" * 70)


def main():
    parser = argparse.ArgumentParser(description="Semrush-Powered Google Ads Analyzer")
    parser.add_argument("--client", default="embenauto", help="Client ID in data/")
    parser.add_argument("--competitors", default=None, help="Comma-separated competitor domains")
    parser.add_argument("--database", default="us", help="Semrush database (default: us)")
    args = parser.parse_args()

    competitors = args.competitors.split(",") if args.competitors else DEFAULT_COMPETITORS

    print(f"\nSemrush Analyzer for: {args.client}")
    print(f"Competitors: {', '.join(competitors)}")
    print(f"Database: {args.database}\n")

    # 1. Load Google Ads data
    print("Loading Google Ads data...")
    ads_data = load_google_ads_data(args.client)
    ads_keywords = analyze_keyword_performance(ads_data)
    search_terms = analyze_search_terms(ads_data)
    print(f"  {len(ads_keywords)} keywords, {len(search_terms)} search terms with clicks\n")

    # 2. Pull Semrush keyword intelligence
    print("Pulling Semrush keyword intelligence...")
    semrush_data = {"keyword_data": {}, "related_keywords": []}

    # Get data for each active keyword
    unique_kws = list({k["keyword"].lower() for k in ads_keywords})
    print(f"  Enriching {len(unique_kws)} keywords...")
    for i, kw in enumerate(unique_kws):
        data = get_keyword_data(kw, args.database)
        if data:
            semrush_data["keyword_data"][kw] = data
        if (i + 1) % 10 == 0:
            print(f"    {i + 1}/{len(unique_kws)} done")
            time.sleep(0.5)

    # Get related keywords for top seed terms
    seed_keywords = ["car shipping", "auto transport", "vehicle transport",
                     "car shipping canada", "snowbird auto transport",
                     "car transport service", "ship car across country"]
    print(f"  Finding related keywords from {len(seed_keywords)} seeds...")
    seen = set()
    for seed in seed_keywords:
        related = get_related_keywords(seed, args.database, limit=30)
        for r in related:
            if r["keyword"].lower() not in seen:
                semrush_data["related_keywords"].append(r)
                seen.add(r["keyword"].lower())
        time.sleep(0.3)
    print(f"  Found {len(semrush_data['related_keywords'])} unique related keywords\n")

    # 3. Pull competitor intelligence
    print("Pulling competitor intelligence...")
    competitor_intel = {}
    for domain in competitors:
        print(f"  Analyzing {domain}...")
        paid = get_competitor_paid_keywords(domain, args.database, limit=40)
        organic = get_competitor_organic_keywords(domain, args.database, limit=20)
        competitor_intel[domain] = {
            "paid_keywords": paid,
            "organic_keywords": organic,
        }
        time.sleep(0.3)
    print()

    # 4. Generate recommendations
    print("Generating recommendations...")
    recommendations = generate_recommendations(
        ads_keywords, search_terms, semrush_data, competitor_intel
    )

    # 5. Save everything
    print("\nSaving results...")
    save_results(args.client, recommendations, semrush_data)

    # 6. Print report
    print_report(recommendations)


if __name__ == "__main__":
    main()
